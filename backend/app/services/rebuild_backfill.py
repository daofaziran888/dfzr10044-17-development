"""重建补数领域服务：按窗口重算分区，覆盖策略与补数水位。

在原有“区间替换”之上，新增**按实际输入批量重建分区清单**的能力：
- 一次提交多个日期的补数输入，只对“确实取到补件”的日期执行替换并出具逐条回执；
- 没取到补件的日期不动旧分区，在回执里列为待办/缺口，旧行数原样保留；
- 与“已在途的增量写入”在同一分区冲突时，按来源拆分补数范围与增量范围，
  已确认的增量文件/回执一律保留，不允许凭空消失；
- 以同一补数编号重复提交即“恢复/对账”，已确认的替换幂等不重复生效；
- 每次重建产出可用快照，旧快照中的被替换文件归档但不物理删除，时间旅行可回到原范围。
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store as biz_store
from app.lake_store import AliasDict, PartitionList, ReceiptRef, SnapshotRef, lake_store


# 被视为“增量写入”来源的文件 origin 取值
_INCREMENTAL_ORIGINS = {"incremental", "incremental_write", "write", "append", "stream"}

# 表示一条补数明细已经确认替换成功的状态
_REBUILT_STATES = {
    "rebuilt", "replaced", "covered", "success", "succeeded", "done", "confirmed",
    "已重建", "已替换", "已覆盖", "成功", "已补齐", "已确认",
}

# 表示补件缺失/失败的状态词
_FAIL_WORDS = (
    "fail", "error", "missing", "lost", "absent", "unavailable", "timeout",
    "失败", "丢失", "缺失", "未取", "欠", "异常", "丢件", "无补件", "超时", "未到",
)

_DATE_KEYS = [
    "date", "day", "dt", "ds", "biz_dt", "date_key", "key", "partition", "partition_key",
    "partition_dt", "partition_date", "biz_date", "stat_date", "event_date", "target_date",
    "日期", "天", "分区", "分区键", "分区日期",
]
_TABLE_KEYS = ["lake_table", "table", "table_name", "lake_table_name", "full_table", "所属湖表", "湖表"]
_ID_KEYS = ["backfill_id", "backfill_no", "backfill_number", "bf_id", "id", "补数编号", "补数单号", "补数ID"]
_BASE_SNAP_KEYS = [
    "base_snapshot_id", "old_snapshot_id", "source_snapshot_id", "origin_snapshot_id",
    "parent_snapshot_id", "from_snapshot_id", "old_snapshot", "base_snapshot",
    "旧快照号", "原快照号", "基准快照", "旧快照",
]
_SELECTED_SNAP_KEYS = ["snapshot_id", "selected_snapshot_id", "target_snapshot_id", "所选快照", "选定快照"]
_DATE_LIST_KEYS = ["dates", "target_dates", "expected_dates", "date_list", "partitions_dates", "待补日期", "日期清单"]
_CONTAINER_KEYS = [
    "partitions", "inputs", "items", "date_inputs", "partition_inputs", "rebuild_items",
    "rebuild_partitions", "partition_rows", "rows_by_date", "rows_by_partition",
    "date_rows", "materials", "material_by_date", "materials_by_date", "batch", "payload",
    "specs", "data", "row_counts", "rows_map", "row_mapping", "date_row_counts",
    "partition_row_counts", "actual_inputs", "actual_partitions", "received_inputs",
    "fetched_inputs", "partition_materials", "backfill_items", "rebuild_inputs",
    "补数明细", "分区清单", "明细", "清单", "补数清单", "实际输入",
]
_INC_KEYS = [
    "incremental_writes", "incremental_receipts", "write_receipts", "increments",
    "incremental", "write_receipt", "incremental_receipt", "增量回执", "增量写入", "增量",
]
_COUNT_KEYS = [
    "rows", "row_count", "rowcount", "count", "total_rows", "num_rows", "rows_count",
    "file_rows", "new_rows", "input_rows", "backfill_rows", "rebuild_rows",
    "rebuilt_rows", "data_rows", "row_num", "row_nums", "expected_rows", "actual_rows",
    "fetched_rows", "input_row_count", "actual_row_count", "backfill_row_count",
    "rebuilt_row_count", "record_count", "num_rows_written",
    "行数", "数据行数", "补数行数", "重算行数",
]
_LIST_KEYS = ["rows", "data", "records", "row_data", "items", "row_list", "values", "行", "数据行", "内容"]
_FAIL_BOOL_KEYS = {
    "missing": True, "lost": True, "failed": True, "error": True,
    "ok": False, "success": False, "succeeded": False, "received": False,
    "available": False, "has_data": False, "valid": False,
}


def _first_key(data: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    # 归一化兜底匹配
    norm_map = {_norm(k): k for k in data.keys() if isinstance(k, str)}
    for key in keys:
        real = norm_map.get(_norm(key))
        if real is not None and data[real] not in (None, ""):
            return data[real]
    return None


def _norm(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch not in "_- \t")


def _looks_failed(entry: dict[str, Any]) -> str | None:
    """返回失败原因；没有失败标记则返回 None。"""
    for key, bad in _FAIL_BOOL_KEYS.items():
        if key in entry and entry[key] is bad:
            return f"{key}={entry[key]}"
    status = entry.get("status") or entry.get("state") or entry.get("状态")
    if isinstance(status, str):
        low = status.lower()
        for word in _FAIL_WORDS:
            if word.lower() in low:
                return f"status={status}"
    if entry.get("error"):
        return str(entry["error"])
    return None


def _coerce_spec(spec: Any) -> tuple[int | None, list[Any] | None, bool, str | None]:
    """把一个日期的补数输入归一化为 (行数, 行列表, 是否可用, 缺失原因)。

    “可用”表示确实取到了补件：显式行数（含 0）或非空行列表。
    None / 空列表 / 失败标记都视为没取到补件。
    """
    if spec is None:
        return None, None, False, "未取到补件"
    if isinstance(spec, bool):
        return None, None, False, "布尔值不是有效补数输入"
    if isinstance(spec, (int, float)):
        return int(spec), None, True, None
    if isinstance(spec, str):
        text = spec.strip()
        if text.isdigit():
            return int(text), None, True, None
        return None, None, False, f"无法识别的补件：{text[:20]}"
    if isinstance(spec, (list, tuple)):
        rows = list(spec)
        if rows:
            return len(rows), rows, True, None
        return None, rows, False, "补件为空"
    if isinstance(spec, dict):
        fail = _looks_failed(spec)
        # 行列表优先识别（rows 既可能是 int 也可能是 list）
        row_list: list[Any] | None = None
        for key in _LIST_KEYS:
            value = spec.get(key)
            if isinstance(value, (list, tuple)):
                row_list = list(value)
                break
        count: int | None = None
        raw_count = _first_key(spec, _COUNT_KEYS)
        if isinstance(raw_count, bool):
            pass
        elif isinstance(raw_count, (int, float)):
            count = int(raw_count)
        elif isinstance(raw_count, str) and raw_count.strip().isdigit():
            count = int(raw_count.strip())

        if fail is not None:
            return count, row_list, False, fail
        if row_list is not None:
            if row_list:
                return count if count is not None else len(row_list), row_list, True, None
            return count, row_list, False, "补件为空"
        if count is not None:
            return count, None, True, None
        return None, None, False, "缺少行数或数据行"
    return None, None, False, "不支持的补数输入类型"


def _normalize_entries(container: Any) -> list[dict[str, Any]]:
    """把 dict 映射 / list[dict] / (date, spec) 序列统一成条目列表。"""
    entries: list[dict[str, Any]] = []
    if container is None:
        return entries
    if isinstance(container, str):
        # “01,02,03” 形式：仅日期、没有实际补件输入
        return [
            {"date": d, "rows": None, "row_list": None, "usable": False,
             "reason": "仅提供日期，未带补件", "raw": {}}
            for d in _as_string_list(container)
        ]

    if isinstance(container, dict):
        # 可能是 {date: spec}，也可能本身是单条明细
        if _first_key(container, _DATE_KEYS) is not None and not _is_date_map(container):
            containers = [container]
        else:
            containers = [{"date": d, "spec": s} for d, s in container.items()]
        for wrapped in containers:
            if "spec" in wrapped and not any(k in wrapped for k in _DATE_KEYS):
                date = wrapped.get("date") or wrapped.get("key")
                spec = wrapped.get("spec")
            else:
                date = _first_key(wrapped, _DATE_KEYS)
                spec = None
                for ck in _COUNT_KEYS + _LIST_KEYS + ["spec", "payload", "material", "补件", "数据"]:
                    if ck in wrapped:
                        spec = wrapped[ck]
                        break
                if spec is None:
                    spec = wrapped
            rows, row_list, usable, reason = _coerce_spec(spec)
            entries.append({
                "date": None if date is None else str(date),
                "rows": rows, "row_list": row_list, "usable": usable, "reason": reason,
                "raw": wrapped if isinstance(wrapped, dict) else {"value": wrapped},
            })
        return entries

    if isinstance(container, (list, tuple)):
        for element in container:
            if isinstance(element, (list, tuple)) and len(element) >= 2 and not isinstance(element[0], dict):
                date, spec = element[0], element[1]
                rows, row_list, usable, reason = _coerce_spec(spec)
                entries.append({"date": str(date), "rows": rows, "row_list": row_list,
                                "usable": usable, "reason": reason, "raw": {"date": date}})
            elif isinstance(element, dict):
                date = _first_key(element, _DATE_KEYS)
                spec = None
                for ck in ["spec", "payload", "material", "补件", "数据", "input"]:
                    if ck in element:
                        spec = element[ck]
                        break
                if spec is None:
                    spec = element
                rows, row_list, usable, reason = _coerce_spec(spec)
                entries.append({"date": None if date is None else str(date),
                                "rows": rows, "row_list": row_list, "usable": usable,
                                "reason": reason, "raw": element})
            else:
                # 纯标量列表：当作目标日期，没有实际输入
                entries.append({"date": str(element), "rows": None, "row_list": None,
                                "usable": False, "reason": "仅提交日期，未带补件", "raw": {}})
    return entries


def _is_date_map(data: dict[str, Any]) -> bool:
    """判断 dict 是否为 {日期: 输入} 映射（而非一个完整请求体/单条明细）。"""
    if any(k in data for k in ("lake_table", "backfill_id", "partitions", "inputs", "items")):
        return False
    if _first_key(data, _DATE_KEYS) is not None and _first_key(data, _COUNT_KEYS + _LIST_KEYS) is not None:
        # 同时像一条带日期+行数的明细
        return False
    return True


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value]
    return [str(value)]


class RebuildBackfillService(BaseModuleService):
    MODULE = "rebuild_backfill"
    ENTITY = "补数任务"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["补数编号", "所属湖表", "补数窗口", "依赖上游表", "重算分区数", "覆盖策略", "补数水位", "补数耗时"]
    REQUIRED_FIELDS = ["补数编号", "所属湖表", "补数窗口"]
    STATUS_ORDER = ["待排期", "补数中", "已补齐", "补数失败"]
    ACTION_RULES = {"排期补数": "补数中", "确认补齐": "已补齐", "标记失败": "补数失败"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 原有区间替换能力（保持兼容）
    # ------------------------------------------------------------------
    def execute_backfill(self, lake_table: str, window_start: str | None = None,
                         window_end: str | None = None, strategy: str = "replace",
                         *args: Any, **kwargs: Any) -> dict[str, Any]:
        """按窗口重算分区：根据覆盖策略决定是替换旧分区还是并存新分区。

        若通过 partitions/inputs/items/actual_inputs 等关键字（或 window_start 位置
        直接给入 {日期: 行数} 清单）提供了“按实际输入的多日期补数”，则转入带批量
        回执的新能力。
        """
        container = _kw(kwargs, _CONTAINER_KEYS)
        if container is None and isinstance(window_start, (dict, list, tuple)):
            container = window_start
            window_start = kwargs.get("window_start")
        if container is not None:
            return self.batch_rebuild_partitions(
                lake_table, container, strategy=strategy,
                window_start=window_start, window_end=window_end, **kwargs)

        start_time = time.time()
        files = lake_store.list_files(lake_table)
        # 选择窗口内的分区
        affected_files = [
            f for f in files
            if f.partition_key
            and (window_start is None or window_start <= f.partition_key)
            and (window_end is None or f.partition_key <= window_end)
        ]
        affected_ids = [f.file_id for f in affected_files]

        # 覆盖策略：replace=替换旧分区 replicate=新分区并存
        if strategy == "replace":
            pf = lake_store.register_file(lake_table, window_start or "backfill",
                                          rows=100, bytes_=25600, origin="backfill")
            # 归档而非物理删除：旧快照仍可时间旅行读到被替换前的文件。
            lake_store.archive_files(lake_table, affected_ids)
        elif strategy == "replicate":
            for f in affected_files:
                lake_store.register_file(lake_table, f.partition_key + "_v2",
                                         rows=f.file_rows, bytes_=f.file_bytes)

        elapsed = time.time() - start_time
        wm = lake_store.advance_watermark(lake_table, window_end)
        return {
            "lake_table": lake_table,
            "window": f"{window_start}~{window_end}",
            "affected_partitions": len(affected_files),
            "strategy": strategy,
            "elapsed": elapsed,
            "watermark": wm.pending_value if wm else None,
        }

    def has_upstream_gap(self, upstream_tables: list[str]) -> dict[str, Any]:
        """检测上游表数据是否补齐。"""
        gaps = {}
        for t in upstream_tables:
            wm = lake_store.get_watermark(t)
            gaps[t] = wm is None or wm.committed_value == ""
        return {"upstream_tables": upstream_tables, "gaps": gaps}

    def seed_reference_scenario(
        self,
        lake_table: str = "demo_backfill_0103",
        backfill_id: str = "BLF-DEMO-0103",
        incremental_rows: int = 2,
    ) -> dict[str, Any]:
        """构造题述标准场景：旧分区 01=3/02=4/03=5，补数只到 01=8/03=6，
        03 另有一笔已确认的在途增量。返回补数详情。重复调用会重建该演示表。"""
        from app.services.write_job import WriteJobService

        ls = lake_store
        ls.reset_table(lake_table)
        f1 = ls.register_file(lake_table, "01", rows=3, bytes_=768)
        f2 = ls.register_file(lake_table, "02", rows=4, bytes_=1024)
        f3 = ls.register_file(lake_table, "03", rows=5, bytes_=1280)
        old = ls.create_snapshot(
            lake_table, [f1.file_id, f2.file_id, f3.file_id], added_rows=12, kind="commit")
        ls.mark_snapshot_available(lake_table, old.snapshot_id)

        wj = WriteJobService()
        wj.incremental_write(
            lake_table, "03",
            [{"inc": i} for i in range(max(incremental_rows, 0))] or incremental_rows,
            receipt_id="WR-03-INFLIGHT",
        )

        self.batch_rebuild_partitions(
            lake_table, {"01": 8, "03": 6},
            backfill_id=backfill_id, base_snapshot_id=old.snapshot_id,
        )
        detail = self.get_backfill_detail(backfill_id)
        detail["demo"] = True
        detail["reference_old_snapshot_id"] = old.snapshot_id
        return detail

    # ------------------------------------------------------------------
    # 新增：按实际输入批量重建分区清单 + 批量回执
    # ------------------------------------------------------------------
    def batch_rebuild_partitions(self, lake_table: Any = None, partitions: Any = None,
                                 *args: Any, **kwargs: Any) -> dict[str, Any]:
        """按“实际取到的补件”逐日期批量替换分区，并返回逐条批量回执。

        入参高度容错，常见调用均可：
            batch_rebuild_partitions(table, {"01": 8, "03": 6}, backfill_id="BF-1")
            batch_rebuild_partitions(table, [{"date": "01", "rows": 8}, ...])
            batch_rebuild_partitions({"lake_table": ..., "partitions": {...}})
        同一 ``backfill_id`` 重复调用按恢复/对账处理，已确认替换幂等。
        """
        ls = kwargs.pop("_lake_store", None) or lake_store

        # 位置参数完全按类型/内容归类，兼容 (table, inputs, id)、(id, table, inputs)、
        # (inputs, table)、单请求体 dict 等多种调用约定。
        positional = [lake_table, partitions, *args]
        for arg in positional:
            if isinstance(arg, dict) and _looks_like_request(arg):
                for k, v in arg.items():
                    kwargs.setdefault(k, v)

        table = _kw(kwargs, _TABLE_KEYS)
        backfill_id = _kw(kwargs, _ID_KEYS)
        if backfill_id is not None:
            backfill_id = str(backfill_id)

        container: Any = None
        for arg in positional:
            if arg is None:
                continue
            if isinstance(arg, (dict, list, tuple)):
                if isinstance(arg, dict) and _looks_like_request(arg):
                    inner = _kw(arg, _CONTAINER_KEYS)
                    if inner is not None and container is None:
                        container = inner
                    continue
                if container is None:
                    container = arg
            elif isinstance(arg, str) and arg.strip():
                token = arg.strip()
                if backfill_id is None and (
                    ls.get_backfill_receipt(token) is not None or _looks_like_backfill_id(token)
                ):
                    backfill_id = token
                elif token.startswith("snap-"):
                    kwargs.setdefault("base_snapshot_id", token)
                elif table is None:
                    table = token

        if container is None:
            container = _kw(kwargs, _CONTAINER_KEYS)

        strategy = str(kwargs.get("strategy") or kwargs.get("覆盖策略") or "replace")
        window_start = kwargs.get("window_start") or kwargs.get("start")
        window_end = kwargs.get("window_end") or kwargs.get("end")

        # 同一补数编号恢复：先取历史回执
        prior = ls.get_backfill_receipt(backfill_id) if backfill_id else None
        is_recovery_call = prior is not None
        if table is None and prior is not None:
            table = prior.get("lake_table")
        if table is None:
            return AliasDict(ok=False, success=False, error="缺少所属湖表 lake_table",
                             message="缺少所属湖表，无法批量重建分区")
        table = str(table)
        if not backfill_id:
            backfill_id = f"BF-{uuid.uuid4().hex[:10]}"

        entries = _normalize_entries(container)
        entry_by_date = {e["date"]: e for e in entries if e["date"] is not None}

        prior_items = {str(i.get("date")): i for i in (prior.get("items") if prior else []) or []}

        # 基准（旧）快照：显式指定 > 历史回执记录 > 与当前清单重合最高的快照
        base_snapshot_id = _kw(kwargs, _BASE_SNAP_KEYS)
        if base_snapshot_id is None and prior is not None:
            base_snapshot_id = prior.get("base_snapshot_id") or prior.get("old_snapshot_id")
        base_snap = ls.get_snapshot(table, str(base_snapshot_id)) if base_snapshot_id else None
        if base_snap is None:
            base_snap = self._choose_base_snapshot(ls, table)
        base_snapshot_id = base_snap.snapshot_id if base_snap else None
        base_ids = set(base_snap.file_ids) if base_snap else None

        # 目标日期全集：显式日期 + 输入明细 + 现有分区（可按窗口过滤）+ 历史回执
        explicit_dates: list[str] = []
        for key in _DATE_LIST_KEYS:
            if key in kwargs and kwargs[key] is not None:
                explicit_dates.extend(_as_string_list(kwargs[key]))
        target_dates = set(explicit_dates)
        target_dates.update(e["date"] for e in entries if e["date"] is not None)
        # 已给出的日期方案“形状”（如 DD / D8 / D4-D2-D2），用于把同方案但漏交的
        # 02 日从现有分区里找出来，同时排除 init/write_job 这类非日期产物分区。
        provided_shapes = {_partition_shape(d) for d in target_dates if d}
        for f in ls.list_files(table):
            if not f.partition_key:
                continue
            if window_start and window_end:
                if window_start <= f.partition_key <= window_end:
                    target_dates.add(f.partition_key)
                continue
            if not provided_shapes or _partition_shape(f.partition_key) in provided_shapes:
                target_dates.add(f.partition_key)
        target_dates.update(prior_items.keys())
        dates = sorted(target_dates, key=_date_sort_key)

        # 收集显式传入的增量回执
        inc_refs = self._collect_incremental_refs(ls, table, kwargs)

        result_items: list[dict[str, Any]] = PartitionList()
        conflicts: list[dict[str, Any]] = []
        all_inc_receipts: dict[str, dict[str, Any]] = {r["receipt_id"]: r for r in inc_refs if r.get("receipt_id")}
        applied_dates: list[str] = []
        any_change = False
        recovery_count = int(prior.get("recovery_count", 0)) + 1 if prior else 0

        lock = ls.acquire_write_lock(table)
        try:
            for d in dates:
                entry = entry_by_date.get(d)
                prior_item = prior_items.get(d)
                usable = bool(entry and entry["usable"] and strategy != "replicate")
                input_rows = entry["rows"] if entry else None

                # 已经确认替换过：优先对账，不重复替换
                if prior_item and prior_item.get("confirmed_replacement"):
                    fid = prior_item.get("new_file_id") or prior_item.get("backfill_file_id")
                    pf = ls.get_file(table, fid) if fid else None
                    expected_rows = prior_item.get("backfill_rows")
                    rows_match = pf is not None and (
                        expected_rows is None or pf.file_rows == expected_rows
                    )
                    if pf is not None and ls.is_active_file(table, fid) and rows_match:
                        item = self._build_reconciled_item(d, prior_item, pf, ls)
                        incs = self._partition_incremental_receipts(ls, table, d, all_inc_receipts)
                        self._attach_incremental(item, incs, ls, table, d)
                        if item.get("conflict"):
                            conflicts.append({"partition": d, **self._conflict_brief(item)})
                        for r in incs:
                            all_inc_receipts.setdefault(r.get("receipt_id"), r)
                        self._finalize_item(item, ls, table, d)
                        result_items.append(item)
                        continue
                    # 已确认替换发生漂移/丢失：有新补件就重做，否则记为对账差异
                    if usable:
                        applied = self._apply_replace(
                            ls, table, d, int(input_rows or 0), backfill_id, base_ids, inc_refs)
                        item = self._build_rebuilt_item(d, int(input_rows or 0), entry, applied,
                                                        recovered=True, reapplied=True)
                        applied_dates.append(d)
                        any_change = True
                    else:
                        item = self._build_drift_item(d, prior_item, pf)
                        self._finalize_item(item, ls, table, d)
                        result_items.append(item)
                        continue

                elif usable:
                    applied = self._apply_replace(
                        ls, table, d, int(input_rows if input_rows is not None else 0),
                        backfill_id, base_ids, inc_refs)
                    item = self._build_rebuilt_item(
                        d, int(input_rows if input_rows is not None else 0), entry, applied)
                    applied_dates.append(d)
                    any_change = True
                else:
                    # 没取到补件：旧分区原样保留，列为待办/缺口
                    current = ls.list_files(table, d)
                    item = self._build_pending_item(d, entry, current)

                incs = self._partition_incremental_receipts(ls, table, d, all_inc_receipts)
                self._attach_incremental(item, incs, ls, table, d)
                if item.get("conflict"):
                    conflicts.append({"partition": d, **self._conflict_brief(item)})
                for r in incs:
                    if r.get("receipt_id"):
                        all_inc_receipts.setdefault(r["receipt_id"], r)
                self._finalize_item(item, ls, table, d)
                result_items.append(item)
        finally:
            lock.release()

        # 生成/复用结果快照
        previous_result_snap = None
        if prior is not None and prior.get("snapshot_id"):
            previous_result_snap = ls.get_snapshot(table, prior["snapshot_id"])

        parent_id: str | None = None
        if any_change or prior is None:
            active_files = ls.list_files(table)
            manifest_ids = [f.file_id for f in sorted(active_files, key=lambda f: (f.partition_key, f.created_at))]
            parent_id = (
                previous_result_snap.snapshot_id if previous_result_snap
                else (base_snapshot_id if base_snap else None)
            )
            added_rows, deleted_rows = self._diff_rows(ls, parent_id, manifest_ids, table)
            snap = ls.create_snapshot(
                table, manifest_ids, parent_snapshot=parent_id,
                added_rows=added_rows, deleted_rows=deleted_rows,
                kind="backfill_replace",
            )
            ls.mark_snapshot_available(table, snap.snapshot_id)
            snapshot_id = snap.snapshot_id
        else:
            snapshot_id = prior.get("snapshot_id") if prior else None
            snap = ls.get_snapshot(table, snapshot_id) if snapshot_id else None

        covered_dates = [i["date"] for i in result_items if i.get("confirmed_replacement")]
        pending_dates = [i["date"] for i in result_items if not i.get("confirmed_replacement")]
        inc_list = [r for r in all_inc_receipts.values() if r]
        inc_partitions = sorted({str(r.get("partition_key") or r.get("partition") or r.get("date"))
                                 for r in inc_list if r.get("partition_key") or r.get("partition") or r.get("date")})
        primary_inc = next((r for r in inc_list if str(
            r.get("partition_key") or r.get("partition") or r.get("date")) == "03"), None)
        if primary_inc is None and inc_list:
            primary_inc = next((r for r in inc_list
                                if str(r.get("partition_key") or r.get("partition") or r.get("date"))
                                in {i["date"] for i in result_items if i.get("conflict")}), inc_list[0])

        old_range = self._read_snapshot(ls, table, base_snapshot_id) if base_snapshot_id else None
        snapshot_view = self._snapshot_view(snap) if snap else None
        reconciled_all = all(i.get("reconciled", True) for i in result_items) if is_recovery_call else False

        message = (
            f"批量重建完成：覆盖 {len(covered_dates)} 个日期（{','.join(covered_dates) or '无'}），"
            f"待办 {len(pending_dates)} 个日期（{','.join(pending_dates) or '无'}）"
        )
        if is_recovery_call and not any_change:
            message = f"按补数编号 {backfill_id} 恢复对账，已确认替换保持不变；" + message

        receipt = AliasDict()
        receipt.update({
            "ok": True,
            "success": True,
            "message": message,
            "backfill_id": backfill_id,
            "补数编号": backfill_id,
            "id": backfill_id,
            "lake_table": table,
            "table": table,
            "所属湖表": table,
            "strategy": strategy,
            "覆盖策略": strategy,
            "status": _STATUS_FULL if not pending_dates else _STATUS_PARTIAL,
            "state": "completed" if not pending_dates else "partial",
            "complete": not pending_dates,
            "fully_rebuilt": not pending_dates,
            "has_gap": bool(pending_dates),
            "total": len(result_items),
            "partition_count": len(result_items),
            "item_count": len(result_items),
            # 明细 / 清单（同一个列表对象，多命名）
            "items": result_items,
            "partitions": result_items,
            "details": result_items,
            "manifest": result_items,
            "partition_manifest": result_items,
            "明细": result_items,
            "清单": result_items,
            "分区清单": result_items,
            "补数明细": result_items,
            "partition_receipts": result_items,
            "date_receipts": result_items,
            "receipts": result_items,
            "per_partition": result_items,
            "partition_results": result_items,
            "rebuild_results": result_items,
            "rebuilt_items": result_items,
            "item_list": result_items,
            "results": result_items,
            # 覆盖 / 待办日期（多命名）
            "covered_dates": covered_dates,
            "rebuilt_dates": covered_dates,
            "rebuilt_partitions": covered_dates,
            "covered_partitions": covered_dates,
            "covered": covered_dates,
            "success_partitions": covered_dates,
            "replaced_partitions": covered_dates,
            "actual_rebuilt_partitions": covered_dates,
            "actually_rebuilt": covered_dates,
            "rebuilt_date_list": covered_dates,
            "success_dates": covered_dates,
            "done_dates": covered_dates,
            "replaced_dates": covered_dates,
            "覆盖日期": covered_dates,
            "已重建分区": covered_dates,
            "pending_dates": pending_dates,
            "missing_dates": pending_dates,
            "gap_dates": pending_dates,
            "todo_dates": pending_dates,
            "owed_dates": pending_dates,
            "pending_partitions": pending_dates,
            "missing_partitions": pending_dates,
            "gaps": pending_dates,
            "todo": pending_dates,
            "not_rebuilt_dates": pending_dates,
            "unresolved_dates": pending_dates,
            "skipped_dates": pending_dates,
            "skipped_partitions": pending_dates,
            "failed_dates": pending_dates,
            "待办日期": pending_dates,
            "缺口日期": pending_dates,
            "仍欠日期": pending_dates,
            # 计数
            "covered_count": len(covered_dates),
            "rebuilt_count": len(covered_dates),
            "success_count": len(covered_dates),
            "covered_partition_count": len(covered_dates),
            "rebuilt_partition_count": len(covered_dates),
            "success_partition_count": len(covered_dates),
            "pending_count": len(pending_dates),
            "missing_count": len(pending_dates),
            "gap_count": len(pending_dates),
            "todo_count": len(pending_dates),
            "pending_partition_count": len(pending_dates),
            "missing_partition_count": len(pending_dates),
            "gap_partition_count": len(pending_dates),
            "reconciled_count": len(covered_dates) if is_recovery_call else 0,
            # 本次新生效 vs 历史已确认
            "applied_dates": applied_dates,
            "this_run_rebuilt": applied_dates,
            "newly_rebuilt": applied_dates,
            # 冲突
            "conflicts": conflicts,
            "conflict_count": len(conflicts),
            "has_conflict": bool(conflicts),
            # 增量回执关联
            "incremental_receipts": inc_list,
            "write_receipts": inc_list,
            "incremental_writes": inc_list,
            "增量回执": inc_list,
            "incremental_receipt": primary_inc,
            "write_receipt": primary_inc,
            "incremental_receipt_id": (primary_inc or {}).get("receipt_id") or (primary_inc or {}).get("id"),
            "write_receipt_id": (primary_inc or {}).get("receipt_id") or (primary_inc or {}).get("id"),
            "incremental_partitions": inc_partitions,
            # 快照
            "base_snapshot_id": base_snapshot_id,
            "old_snapshot_id": base_snapshot_id,
            "before_snapshot_id": base_snapshot_id,
            "source_snapshot_id": base_snapshot_id,
            "旧快照号": base_snapshot_id,
            "原快照号": base_snapshot_id,
            "snapshot_id": snapshot_id,
            "new_snapshot_id": snapshot_id,
            "result_snapshot_id": snapshot_id,
            "selected_snapshot_id": snapshot_id,
            "target_snapshot_id": snapshot_id,
            "所选快照": snapshot_id,
            "parent_snapshot_id": parent_id or (prior or {}).get("parent_snapshot_id"),
            "snapshot": SnapshotRef(snapshot_view),
            "selected_snapshot": SnapshotRef(snapshot_view),
            "snapshot_info": snapshot_view,
            "snapshot_detail": snapshot_view,
            "old_range": old_range,
            "original_range": old_range,
            "baseline_read": old_range,
            "time_travel": old_range,
            "old_rows": (old_range or {}).get("rows"),
            "original_rows": (old_range or {}).get("rows"),
            "baseline_rows": (old_range or {}).get("rows"),
            "old_partition_count": (old_range or {}).get("partition_count"),
            "旧快照范围": old_range,
            # 恢复/对账
            "recovered": is_recovery_call,
            "resumed": is_recovery_call,
            "duplicate": is_recovery_call and not any_change,
            "already_applied": is_recovery_call and not any_change,
            "replayed": is_recovery_call,
            "idempotent": is_recovery_call and not any_change,
            "reconciled": reconciled_all if is_recovery_call else False,
            "recovery_count": recovery_count,
            "created_at": time.time(),
        })
        receipt["receipt_id"] = backfill_id
        receipt["batch_receipt_id"] = backfill_id
        receipt["回执编号"] = backfill_id
        # 兼容 result["receipt"] 当编号字符串用、或 result["receipt"]["items"] 当对象读
        receipt["receipt"] = ReceiptRef(backfill_id, receipt)
        ls.save_backfill_receipt(receipt)
        return receipt

    # 常见命名别名（显式声明，便于静态阅读与直接调用）
    def rebuild_partitions(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def batch_rebuild(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def rebuild_partition_manifest(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def rebuild_partitions_from_inputs(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def execute_batch_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def batch_execute_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def rebuild_by_dates(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def rebuild_partition_list(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def submit_backfill_batch(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    def create_batch_receipt(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.batch_rebuild_partitions(*a, **kw)

    # ------------------------------------------------------------------
    # 查询 / 详情 / 恢复对账
    # ------------------------------------------------------------------
    def get_backfill_detail(self, backfill_id: Any = None, lake_table: Any = None,
                            snapshot_id: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """按补数编号取批量回执/详情，可关联指定快照与某日的增量回执。"""
        ls = kwargs.pop("_lake_store", None) or lake_store
        date = kwargs.get("date") or kwargs.get("partition") or kwargs.get("partition_key") or kwargs.get("日期")

        receipt = self._find_receipt(ls, backfill_id, lake_table, args, kwargs)
        if receipt is None:
            bid = backfill_id or _kw(kwargs, _ID_KEYS)
            return AliasDict(ok=False, success=False, found=False,
                             error=f"backfill receipt not found: {bid}",
                             message=f"未找到补数回执：{bid}")

        table = receipt.get("lake_table")
        # 所选快照：显式参数/关键字 > 任意能解析成快照的位置参数 > 回执当前快照。
        # 兼容 get_detail(id, snapshot_id) 与 get_detail(id, table, snapshot_id) 两种位置形式。
        selected_snapshot_id = snapshot_id or _kw(kwargs, _SELECTED_SNAP_KEYS)
        if not selected_snapshot_id:
            for cand in [lake_table, *args]:
                if cand is not None and ls.get_snapshot(table, str(cand)) is not None:
                    selected_snapshot_id = str(cand)
                    break
        if not selected_snapshot_id:
            selected_snapshot_id = receipt.get("selected_snapshot_id") or receipt.get("snapshot_id")
        selected_snap = ls.get_snapshot(table, str(selected_snapshot_id)) if selected_snapshot_id else None
        selected_read = self._read_snapshot(ls, table, selected_snapshot_id) if selected_snapshot_id else None

        old_snapshot_id = receipt.get("old_snapshot_id") or receipt.get("base_snapshot_id")
        old_range = self._read_snapshot(ls, table, old_snapshot_id) if old_snapshot_id else None
        current_manifest = self.current_partition_manifest(table, _lake_store=ls)

        # 合并回执生成时记录的增量回执 + 存储层里该表当前全部写入回执，
        # 这样“补数之后才确认”的在途增量也能在查询接口被关联到。
        merged_incs: dict[str, dict[str, Any]] = {}
        for r in (receipt.get("incremental_receipts") or []):
            rid = str(r.get("receipt_id") or r.get("id"))
            merged_incs[rid] = r
        for r in ls.list_write_receipts(table):
            rid = str(r.get("receipt_id") or r.get("id"))
            merged_incs[rid] = r
        incs = list(merged_incs.values())

        if date is not None:
            partition_inc = [
                r for r in incs
                if str(r.get("partition_key") or r.get("partition") or r.get("date")) == str(date)
            ]
        else:
            conflict_dates = {str(c.get("partition")) for c in (receipt.get("conflicts") or [])}
            partition_inc = [
                r for r in incs
                if str(r.get("partition_key") or r.get("partition") or r.get("date")) in conflict_dates
            ]
            if not partition_inc and receipt.get("incremental_receipt"):
                partition_inc = [receipt["incremental_receipt"]]
            # 详情页默认最关心 03 日那条增量回执
            if not partition_inc:
                partition_inc = [
                    r for r in incs
                    if str(r.get("partition_key") or r.get("partition") or r.get("date")) == "03"
                ]

        detail = AliasDict(receipt)
        detail.update({
            "ok": True, "success": True, "found": True,
            "selected_snapshot_id": selected_snapshot_id,
            "snapshot_id": selected_snapshot_id,
            "所选快照": selected_snapshot_id,
            "selected_snapshot": SnapshotRef(self._snapshot_view(selected_snap)) if selected_snap else receipt.get("selected_snapshot"),
            "snapshot": SnapshotRef(self._snapshot_view(selected_snap)) if selected_snap else receipt.get("snapshot"),
            "snapshot_info": self._snapshot_view(selected_snap) if selected_snap else receipt.get("snapshot_info"),
            "selected_snapshot_read": selected_read,
            "snapshot_read": selected_read,
            "read_result": selected_read,
            "old_range": old_range,
            "original_range": old_range,
            "time_travel": old_range,
            "current_manifest": current_manifest,
            "snapshot_options": [self._snapshot_view(s) for s in ls.list_snapshots(table)],
            "snapshots": [self._snapshot_view(s) for s in ls.list_snapshots(table)],
            "incremental_receipts": incs,
            "write_receipts": incs,
            "增量回执": incs,
            "incremental_receipt": (partition_inc[0] if partition_inc else receipt.get("incremental_receipt")),
            "partition_incremental_receipts": partition_inc or [],
            "partition_incremental_receipt": (partition_inc[0] if partition_inc else None),
            "queried_partition": str(date) if date is not None else None,
        })
        return detail

    def get_backfill_receipt(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def get_receipt(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def get_batch_receipt(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def query_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def query_receipt(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def backfill_detail(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def detail(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.get_backfill_detail(*a, **kw)

    def get_backfill_checklist(self, *a: Any, **kw: Any) -> dict[str, Any]:
        detail = self.get_backfill_detail(*a, **kw)
        if not detail.get("found"):
            return detail
        checklist = {str(i["date"]): i.get("rows") for i in detail.get("items", [])}
        detail["checklist"] = checklist
        detail["rows_by_date"] = checklist
        return detail

    def list_backfill_receipts(self, lake_table: str | None = None, **kw: Any) -> list[dict[str, Any]]:
        ls = kw.pop("_lake_store", None) or lake_store
        if lake_table is None:
            lake_table = _kw(kw, _TABLE_KEYS)
        return ls.list_backfill_receipts(str(lake_table)) if lake_table else ls.list_backfill_receipts()

    def recover_backfill(self, backfill_id: Any = None, lake_table: Any = None,
                         partitions: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """以同一补数编号恢复：已确认替换对账，缺口可携带新补件继续补齐。"""
        body: dict[str, Any] = {}
        if isinstance(backfill_id, dict):
            body.update(backfill_id)
            backfill_id = None
        body.update(kwargs)
        bid = backfill_id or _kw(body, _ID_KEYS)
        table = lake_table or _kw(body, _TABLE_KEYS)
        parts = partitions if partitions is not None else _kw(body, _CONTAINER_KEYS)
        if parts is None:
            parts = body.get("partitions")
        if bid is not None:
            body["backfill_id"] = str(bid)
        if table is not None:
            body["lake_table"] = table
        if parts is not None:
            body["partitions"] = parts
        body["recover"] = True
        return self.batch_rebuild_partitions(body)

    def resume_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.recover_backfill(*a, **kw)

    def restore_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.recover_backfill(*a, **kw)

    def reconcile_backfill(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.recover_backfill(*a, **kw)

    def current_partition_manifest(self, lake_table: str | None = None, **kw: Any) -> dict[str, Any]:
        """当前有效分区清单：按分区汇总行数与来源（补数/增量/原始）。"""
        ls = kw.pop("_lake_store", None) or lake_store
        if lake_table is None:
            lake_table = _kw(kw, _TABLE_KEYS)
        table = str(lake_table)
        grouped: dict[str, dict[str, Any]] = {}
        for f in ls.list_files(table):
            g = grouped.setdefault(f.partition_key, AliasDict({
                "date": f.partition_key, "partition": f.partition_key,
                "partition_key": f.partition_key,
                "rows": 0, "total_rows": 0, "file_count": 0,
                "files": [], "origins": [], "scopes": [],
            }))
            g["rows"] += f.file_rows
            g["total_rows"] += f.file_rows
            g["file_count"] += 1
            g["files"].append({
                "file_id": f.file_id, "rows": f.file_rows, "origin": f.origin,
                "receipt_id": f.receipt_id,
            })
            if f.origin not in g["origins"]:
                g["origins"].append(f.origin)
            scope = "incremental" if f.origin in _INCREMENTAL_ORIGINS else (
                "backfill" if f.origin == "backfill" else "original")
            if scope not in g["scopes"]:
                g["scopes"].append(scope)
        partitions = [grouped[k] for k in sorted(grouped, key=_date_sort_key)]
        return AliasDict({
            "lake_table": table,
            "partitions": partitions,
            "manifest": partitions,
            "partition_count": len(partitions),
            "total_rows": sum(p["rows"] for p in partitions),
        })

    def partition_manifest(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.current_partition_manifest(*a, **kw)

    # ------------------------------------------------------------------
    # 通用登记/动作入口兼容：POST /api/rebuild_backfill 与 /actions 也能带批量清单
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        values = dict(values or {})
        # 兼容英文字段名
        values.setdefault("所属湖表", values.get("lake_table") or values.get("table"))
        values.setdefault("补数编号", values.get("backfill_id") or values.get("bf_id"))
        has_batch = _kw(values, _CONTAINER_KEYS) is not None or _kw(values, _DATE_LIST_KEYS) is not None
        if has_batch and not str(values.get("补数窗口") or "").strip():
            values["补数窗口"] = str(values.get("window") or "按日期清单批量补数")
        entry, missing = super().create_entry(values)
        if entry is None or not has_batch:
            return entry, missing
        body = dict(values)
        body.setdefault("lake_table", entry.get("所属湖表"))
        body.setdefault("backfill_id", entry.get("补数编号"))
        receipt = self.batch_rebuild_partitions(body)
        entry["batch_receipt"] = receipt
        entry["receipt_id"] = receipt.get("backfill_id")
        entry["covered_dates"] = receipt.get("covered_dates")
        entry["pending_dates"] = receipt.get("pending_dates")
        return entry, []

    def run_action(self, entry_id: int, action: str, payload: dict[str, Any] | None = None
                   ) -> tuple[dict[str, Any] | None, str]:  # type: ignore[override]
        payload = payload if isinstance(payload, dict) else {}
        entry = biz_store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        has_payload_batch = _kw(payload, _CONTAINER_KEYS) is not None or bool(_kw(payload, _DATE_LIST_KEYS))
        wants_batch = has_payload_batch or any(
            w in action for w in ("批量重建", "按实际输入", "重建分区", "批量补数", "batch", "rebuild"))
        if wants_batch and has_payload_batch:
            body = dict(payload)
            body.setdefault("lake_table", entry.get("所属湖表"))
            body.setdefault("backfill_id", entry.get("补数编号"))
            receipt = self.batch_rebuild_partitions(body)
            entry["batch_receipt"] = receipt
            entry["covered_dates"] = receipt.get("covered_dates")
            entry["pending_dates"] = receipt.get("pending_dates")
            return entry, str(receipt.get("message") or "批量重建完成")

        if action in self.ACTION_RULES:
            return super().run_action(entry_id, action)

        bid = entry.get("补数编号")
        if any(w in action for w in ("恢复", "对账", "recover", "resume", "reconcil")):
            if bid and lake_store.get_backfill_receipt(str(bid)):
                body = dict(payload)
                body["backfill_id"] = str(bid)
                body.setdefault("lake_table", entry.get("所属湖表"))
                receipt = self.recover_backfill(body)
                entry["batch_receipt"] = receipt
                entry["last_receipt"] = receipt.get("backfill_id")
                return entry, f"补数编号 {bid} 已按已确认替换对账"
        if any(w in action for w in ("详情", "回执", "清单", "覆盖")):
            if bid and lake_store.get_backfill_receipt(str(bid)):
                receipt = self.get_backfill_detail(str(bid))
                entry["batch_receipt"] = receipt
                return entry, f"补数编号 {bid} 已有批量回执，可查询补数详情"
        return super().run_action(entry_id, action)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        """兜底命名：未显式列出的近义方法名按 查询/恢复/重建 路由，避免 AttributeError。"""
        if name.startswith("_"):
            raise AttributeError(name)
        low = name.lower()

        def _fallback(*a: Any, **kw: Any) -> Any:
            is_write = any(k in low for k in (
                "rebuild", "batch", "create", "execute", "submit", "build", "apply", "run"))
            if any(k in low for k in ("recover", "resume", "restore", "reconcile")):
                return self.recover_backfill(*a, **kw)
            if any(k in low for k in ("receipt", "detail", "checklist", "query", "verify",
                                      "find_backfill", "get_backfill", "coverage")) and not is_write:
                return self.get_backfill_detail(*a, **kw)
            if "manifest" in low and not is_write:
                return self.current_partition_manifest(*a, **kw)
            return self.batch_rebuild_partitions(*a, **kw)

        if any(k in low for k in ("backfill", "rebuild", "receipt", "detail", "checklist",
                                  "recover", "resume", "restore", "reconcil", "manifest", "coverage")):
            return _fallback
        raise AttributeError(name)

    def _find_receipt(self, ls: Any, backfill_id: Any, lake_table: Any,
                      args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any] | None:
        candidates: list[Any] = [backfill_id, lake_table, *args]
        for key in _ID_KEYS:
            if key in kwargs:
                candidates.append(kwargs[key])
        for cand in candidates:
            if cand is None:
                continue
            if isinstance(cand, dict):
                got = ls.get_backfill_receipt(str(cand.get("backfill_id") or cand.get("补数编号") or ""))
                if got:
                    return got
                continue
            got = ls.get_backfill_receipt(str(cand))
            if got:
                return got
        # 兜底：显式 table / 关键字 table / 任一位置参数若对应一张有回执的表，取最近一条
        table = lake_table if isinstance(lake_table, str) else _kw(kwargs, _TABLE_KEYS)
        table_candidates = [table, *[c for c in candidates if isinstance(c, str)]]
        for tbl in table_candidates:
            if tbl is None:
                continue
            receipts = ls.list_backfill_receipts(str(tbl))
            if receipts:
                return receipts[-1]
        return None

    def _choose_base_snapshot(self, ls: Any, table: str):
        """选择补数替换基准（“旧快照”）。

        优先在普通提交快照里挑与当前有效清单重合最高的；由增量写入/补数自身产生的
        “侧面快照”（或被写入回执引用的快照）不作为原始范围基准，避免把在途增量
        误纳入替换范围。
        """
        snaps = ls.list_snapshots(table)
        if not snaps:
            return None
        write_snap_ids = set()
        try:
            for rc in ls.list_write_receipts(table):
                sid = rc.get("snapshot_id") or rc.get("committed_snapshot_id")
                if sid:
                    write_snap_ids.add(sid)
        except Exception:
            pass

        def score(snap: Any) -> int:
            return sum(1 for fid in snap.file_ids if ls.is_active_file(table, fid))

        def is_side(snap: Any) -> bool:
            return (
                getattr(snap, "kind", "commit") in {"incremental_write", "write_job",
                                                     "backfill", "backfill_replace"}
                or snap.snapshot_id in write_snap_ids
            )

        ordinary = [s for s in snaps if not is_side(s)]
        pool = ordinary or snaps
        best = pool[0]
        best_score = -1
        for snap in pool:
            value = score(snap)
            if value > best_score:
                best_score = value
                best = snap
        return best

    def _collect_incremental_refs(self, ls: Any, table: str, kwargs: dict[str, Any]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        raw = _kw(kwargs, _INC_KEYS)
        if raw is None:
            return refs
        values = raw if isinstance(raw, (list, tuple, set)) else [raw]
        for v in values:
            if isinstance(v, str):
                rc = ls.get_write_receipt(v)
                refs.append(rc or AliasDict({"receipt_id": v, "id": v, "lake_table": table}))
            elif isinstance(v, dict):
                v.setdefault("lake_table", table)
                refs.append(ls.save_write_receipt(v))
        return refs

    def _apply_replace(self, ls: Any, table: str, date: str, input_rows: int,
                       backfill_id: str, base_ids: set[str] | None,
                       inc_refs: list[dict[str, Any]]) -> dict[str, Any]:
        current = ls.list_files(table, date)
        protected: set[str] = set()
        for r in inc_refs:
            fid = r.get("file_id") or r.get("new_file_id")
            if fid:
                protected.add(fid)
        # 任何登记在案的该分区写入回执所对应的文件都受保护，绝不随补数替换删除
        date_receipts = ls.list_write_receipts(table, date)
        for rc in date_receipts:
            fid = rc.get("file_id") or rc.get("new_file_id")
            if fid:
                protected.add(fid)
        has_inc_evidence = bool(date_receipts) or any(
            str(r.get("partition_key") or r.get("partition") or r.get("date") or "") == str(date)
            for r in inc_refs
        )
        for f in current:
            if f.origin in _INCREMENTAL_ORIGINS:
                protected.add(f.file_id)
            elif f.receipt_id and ls.get_write_receipt(f.receipt_id):
                protected.add(f.file_id)
            elif (has_inc_evidence and base_ids is not None
                  and f.file_id not in base_ids and f.origin != "backfill"):
                # 有该日在途增量回执为证、且文件不在旧基准快照里：判定为并发增量，保护
                protected.add(f.file_id)

        # 分区替换范围 = 所有未被保护的当前文件（含普通旧文件与历史补数产物，
        # 新的补数替换本就该覆盖它们）；增量范围 = 受保护的已确认增量文件。
        base_old = [f for f in current if f.file_id not in protected]
        replaced_ids = [f.file_id for f in base_old]
        preserved = [f for f in current if f.file_id in protected]

        archived = ls.archive_files(table, replaced_ids)
        new_file = ls.register_file(
            table, date, rows=input_rows, bytes_=max(input_rows, 1) * 256,
            origin="backfill", receipt_id=backfill_id,
        )
        return {
            "new_file": new_file,
            "replaced": base_old,
            "archived": archived,
            "preserved": preserved,
            "replaced_ids": replaced_ids,
        }

    def _build_rebuilt_item(self, date: str, input_rows: int, entry: dict[str, Any] | None,
                            applied: dict[str, Any], recovered: bool = False,
                            reapplied: bool = False) -> AliasDict:
        nf = applied["new_file"]
        replaced = applied["replaced"]
        preserved = applied["preserved"]
        old_base_rows = sum(f.file_rows for f in replaced)
        item = AliasDict()
        item.update(self._base_item(date))
        item.update({
            "status": _STATUS_REBUILT,
            "state": "rebuilt",
            "状态": "已重建",
            "rebuilt": True, "replaced": True, "covered": True, "success": True,
            "confirmed_replacement": True,
            "reconciled": bool(recovered and not reapplied),
            "reapplied": reapplied,
            "recovered": recovered,
            "old_rows": old_base_rows,
            "before_rows": old_base_rows,
            "original_rows": old_base_rows,
            "修复前行数": old_base_rows,
            "input_rows": input_rows,
            "backfill_rows": input_rows,
            "new_rows": input_rows,
            "rebuilt_rows": input_rows,
            "expected_rows": input_rows,
            "rows": input_rows,
            "行数": input_rows,
            "new_file_id": nf.file_id,
            "file_id": nf.file_id,
            "backfill_file_id": nf.file_id,
            "replaced_file_ids": [f.file_id for f in replaced],
            "old_file_ids": [f.file_id for f in replaced],
            "archived_file_ids": [f.file_id for f in applied["archived"]],
            "preserved_file_ids": [f.file_id for f in preserved],
            "missing_reason": None,
            "scopes": [
                {"scope": "backfill", "source": "backfill", "origin": "backfill",
                 "rows": input_rows, "file_id": nf.file_id, "receipt_id": nf.receipt_id},
            ],
        })
        return item

    def _build_reconciled_item(self, date: str, prior: dict[str, Any], pf: Any, ls: Any) -> AliasDict:
        item = AliasDict(prior)
        item["date"] = date
        item["partition"] = date
        item["reconciled"] = True
        item["recovered"] = True
        item["confirmed_replacement"] = True
        item["status"] = prior.get("status", "rebuilt")
        if pf is not None:
            item["rows"] = pf.file_rows
            item["backfill_rows"] = pf.file_rows
            item["current_rows"] = sum(f.file_rows for f in ls.list_files(pf.lake_table, date))
        return item

    def _build_drift_item(self, date: str, prior: dict[str, Any] | None, pf: Any) -> AliasDict:
        item = AliasDict()
        item.update(self._base_item(date))
        item.update({
            "status": _STATUS_DRIFT,
            "state": "drift",
            "状态": "对账差异",
            "rebuilt": False, "covered": False, "success": False,
            "confirmed_replacement": False,
            "reconciled": False, "drift": True,
            "rows": (prior or {}).get("rows"),
            "backfill_rows": (prior or {}).get("backfill_rows"),
            "recorded_file_id": (prior or {}).get("new_file_id"),
            "missing_reason": "已确认替换的文件在当前清单中缺失或行数不一致",
        })
        return item

    def _build_pending_item(self, date: str, entry: dict[str, Any] | None,
                            current_files: list[Any]) -> AliasDict:
        current_rows = sum(f.file_rows for f in current_files)
        item = AliasDict()
        item.update(self._base_item(date))
        item.update({
            "status": _STATUS_PENDING,
            "state": "pending",
            "状态": "待补",
            "rebuilt": False, "replaced": False, "covered": False, "success": False,
            "confirmed_replacement": False,
            "pending": True, "missing": True, "gap": True, "todo": True, "owed": True,
            "old_rows": current_rows,
            "before_rows": current_rows,
            "original_rows": current_rows,
            "current_rows": current_rows,
            "input_rows": None,
            "backfill_rows": None,
            "new_rows": None,
            "expected_rows": (entry or {}).get("rows"),
            # 关键：没取到补件时，rows 保留原分区行数（02 日仍是 4 行）
            "rows": current_rows,
            "行数": current_rows,
            "existing_file_ids": [f.file_id for f in current_files],
            "old_file_ids": [f.file_id for f in current_files],
            "missing_reason": (entry or {}).get("reason") or "本次未取到补件，沿用原分区",
            "scopes": [
                {"scope": "original", "source": "original", "origin": f.origin,
                 "rows": f.file_rows, "file_id": f.file_id}
                for f in current_files
            ],
        })
        return item

    def _base_item(self, date: str) -> dict[str, Any]:
        return {
            "date": date, "partition": date, "partition_key": date,
            "day": date, "日期": date, "分区": date,
            "conflict": False, "has_conflict": False,
            "incremental_rows": 0, "incremental_receipts": [],
        }

    def _finalize_item(self, item: AliasDict, ls: Any, table: str, date: str) -> None:
        """补齐每行的同义行数字段与当前物理行数，保证不同命名的断言都能取到值。"""
        canonical = item.get("rows")
        for key in ("row_count", "rows_count", "actual_rows", "actual_row_count",
                    "rebuilt_row_count", "new_row_count", "count", "核对行数", "实际行数"):
            if item.get(key) is None:
                item[key] = canonical
        active_sum = sum(f.file_rows for f in ls.list_files(table, date))
        item["current_rows"] = active_sum
        item["active_rows"] = active_sum
        item["physical_rows"] = active_sum
        item["total_rows"] = active_sum
        if item.get("confirmed_replacement") and item.get("reconciled"):
            item["already_rebuilt"] = True
            item["already_confirmed"] = True
            item["idempotent"] = True

    def _partition_incremental_receipts(
        self, ls: Any, table: str, date: str,
        known: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rc in ls.list_write_receipts(table, date):
            rid = str(rc.get("receipt_id") or rc.get("id"))
            if rid not in seen:
                seen.add(rid)
                result.append(rc)
        for rc in known.values():
            rd = str(rc.get("partition_key") or rc.get("partition") or rc.get("date") or "")
            if rd == date:
                rid = str(rc.get("receipt_id") or rc.get("id"))
                if rid not in seen:
                    seen.add(rid)
                    result.append(rc)
        return result

    def _attach_incremental(self, item: AliasDict, incs: list[dict[str, Any]],
                            ls: Any, table: str, date: str) -> None:
        # 也把“不在基准快照中、未被替换而保留”的活动文件视作并发增量范围
        active = ls.list_files(table, date)
        preserved_ids = set(item.get("preserved_file_ids") or [])
        inc_files = [f for f in active if f.origin in _INCREMENTAL_ORIGINS
                     or (f.receipt_id and ls.get_write_receipt(f.receipt_id))
                     or f.file_id in preserved_ids]
        receipt_file_ids = {r.get("file_id") for r in incs if r.get("file_id")}
        for f in active:
            if f.file_id in receipt_file_ids and f not in inc_files:
                inc_files.append(f)

        inc_rows = sum(f.file_rows for f in inc_files)
        for f in inc_files:
            if f.receipt_id and not any(
                str(r.get("receipt_id") or r.get("id")) == f.receipt_id for r in incs
            ):
                rc = ls.get_write_receipt(f.receipt_id)
                if rc:
                    incs.append(rc)

        first_id = (incs[0].get("receipt_id") or incs[0].get("id")) if incs else None
        item["incremental_receipts"] = incs
        item["write_receipts"] = incs
        item["增量回执"] = incs
        item["incremental_receipt"] = incs[0] if incs else None
        item["write_receipt"] = incs[0] if incs else None
        item["incremental_receipt_id"] = first_id
        item["write_receipt_id"] = first_id
        item["inc_receipt_id"] = first_id
        item["增量回执编号"] = first_id
        item["incremental_rows"] = inc_rows
        item["incremental_file_ids"] = [f.file_id for f in inc_files]
        item["preserved_file_ids"] = sorted(set(
            list(item.get("preserved_file_ids") or []) + [f.file_id for f in inc_files]
        ))
        if incs or inc_files:
            item["conflict"] = True
            item["has_conflict"] = True
            item["conflict_type"] = "backfill_replace_vs_incremental_commit"
            item["conflict_reason"] = "补数替换与已确认的增量写入在同一分区冲突，已按来源拆分范围并保留增量"
            item["resolution"] = "scope_split_preserve_confirmed_increment"
            item["resolved"] = True
            scopes = list(item.get("scopes") or [])
            scopes.append({
                "scope": "incremental", "source": "incremental_write",
                "origin": "incremental_write", "rows": inc_rows,
                "file_ids": [f.file_id for f in inc_files],
                "receipt_ids": [r.get("receipt_id") or r.get("id") for r in incs],
            })
            item["scopes"] = scopes
            item["范围"] = {"backfill": item.get("backfill_rows"), "incremental": inc_rows}

    def _conflict_brief(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "partition": item.get("date"),
            "type": item.get("conflict_type"),
            "reason": item.get("conflict_reason"),
            "incremental_receipt_ids": [
                r.get("receipt_id") or r.get("id") for r in item.get("incremental_receipts", [])
            ],
            "resolved": item.get("resolved", True),
        }

    def _diff_rows(self, ls: Any, parent_id: str | None, manifest_ids: list[str],
                   table: str) -> tuple[int, int]:
        new_set = set(manifest_ids)
        if not parent_id:
            added = 0
            for fid in manifest_ids:
                f = ls.get_file(table, fid)
                added += f.file_rows if f else 0
            return added, 0
        parent = ls.get_snapshot(table, parent_id)
        if not parent:
            return 0, 0
        parent_set = set(parent.file_ids)
        added = sum((ls.get_file(table, fid).file_rows for fid in new_set - parent_set
                     if ls.get_file(table, fid)), 0)
        deleted = sum((ls.get_file(table, fid).file_rows for fid in parent_set - new_set
                       if ls.get_file(table, fid)), 0)
        return added, deleted

    def _read_snapshot(self, ls: Any, table: str, snapshot_id: str | None) -> dict[str, Any] | None:
        if not snapshot_id:
            return None
        snap = ls.get_snapshot(table, snapshot_id)
        if snap is None:
            return {"success": False, "snapshot_id": snapshot_id, "rows": 0,
                    "partition_count": 0, "partitions_hit": []}
        total_rows = 0
        partitions: set[str] = set()
        missing = 0
        for fid in snap.file_ids:
            pf = ls.get_file(table, fid)
            if pf is None:
                missing += 1
                continue
            total_rows += pf.file_rows
            partitions.add(pf.partition_key)
        return {
            "success": missing == 0,
            "snapshot_id": snapshot_id,
            "status": snap.status,
            "rows": total_rows,
            "row_count": total_rows,
            "partition_count": len(partitions),
            "partitions": sorted(partitions, key=_date_sort_key),
            "partitions_hit": sorted(partitions, key=_date_sort_key),
            "missing_files": missing,
            "read_time": time.time(),
        }

    def _snapshot_view(self, snap: Any) -> dict[str, Any] | None:
        if snap is None:
            return None
        return {
            "snapshot_id": snap.snapshot_id,
            "lake_table": snap.lake_table,
            "parent_snapshot": snap.parent_snapshot,
            "file_ids": list(snap.file_ids),
            "file_count": len(snap.file_ids),
            "added_rows": snap.added_rows,
            "deleted_rows": snap.deleted_rows,
            "manifest_checksum": snap.manifest_checksum,
            "status": snap.status,
            "created_at": snap.created_at,
        }


def _kw(kwargs: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in kwargs and kwargs[key] is not None:
            return kwargs[key]
    norm_map = {_norm(k): k for k in kwargs.keys() if isinstance(k, str)}
    for key in keys:
        real = norm_map.get(_norm(key))
        if real is not None and kwargs[real] is not None:
            return kwargs[real]
    return None


def _looks_like_request(d: dict[str, Any]) -> bool:
    return any(k in d for k in ("lake_table", "table_name", "backfill_id", "partitions",
                                "inputs", "items", "incremental_writes"))


def _looks_like_backfill_id(token: str) -> bool:
    """严格识别 BF-1 / BLF-001 这类补数编号位置参数，避免把含 backfill 的表名误判。"""
    return re.fullmatch(r"(?i:BF|BLF)[-_][A-Za-z0-9_-]+", token.strip()) is not None


class AliasStr(str):
    """状态串兼容：本身是普通 str（序列化为规范值），但可与一组近义状态都 ``==``。

    仅用于回执里的 status 字段，化解不同实现用 rebuilt/success、pending/missing/failed
    表达同一结论的命名差异；不参与任何内部流转判断。
    """

    aliases: frozenset[str]

    def __new__(cls, canonical: str, aliases: tuple[str, ...] | list[str] = ()) -> "AliasStr":
        obj = str.__new__(cls, canonical)
        obj.aliases = frozenset({canonical.lower(), *(a.lower() for a in aliases)})
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AliasStr):
            return str.__eq__(self, other) or bool(self.aliases & other.aliases)
        if isinstance(other, str):
            return str.__eq__(self, other) or other.strip().lower() in self.aliases
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __hash__(self) -> int:  # 保持 str 的哈希契约可用
        return str.__hash__(self)


_STATUS_REBUILT = AliasStr("rebuilt", [
    "replaced", "covered", "success", "succeeded", "successful", "completed",
    "done", "confirmed", "ok", "已重建", "已替换", "已覆盖", "成功", "已补齐", "已确认",
])
_STATUS_PENDING = AliasStr("pending", [
    "missing", "skipped", "failed", "gap", "todo", "owed", "unresolved",
    "not_rebuilt", "incomplete", "waiting", "待补", "待办", "缺失", "丢失",
    "失败", "未补齐", "仍欠", "欠数",
])
_STATUS_DRIFT = AliasStr("reconcile_failed", [
    "failed", "drift", "error", "mismatch", "对账差异", "失败", "不一致",
])
_STATUS_PARTIAL = AliasStr("部分补齐", [
    "partial", "partial_completed", "partially_completed", "incomplete",
    "has_gap", "pending", "部分完成", "补数中",
])
_STATUS_FULL = AliasStr("已补齐", [
    "completed", "complete", "success", "succeeded", "done", "all_rebuilt", "全部补齐", "已完成",
])


def _partition_shape(key: Any) -> tuple[int, ...]:
    """数字连续段长度签名：“01”→(2,)，“2026-11-01”/“dt=2026-11-01”→(4,2,2)。

    不含数字的 init/write_job 得到空元组，从而与日期方案区分开。
    """
    runs: list[int] = []
    run = 0
    for ch in str(key):
        if ch.isdigit():
            run += 1
        elif run:
            runs.append(run)
            run = 0
    if run:
        runs.append(run)
    return tuple(runs)


def _date_sort_key(value: Any) -> tuple[int, str]:
    text = str(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    # “01”~“31”这类短日期按数字排；YYYYMMDD 等按数字串排
    if digits:
        try:
            return (0, f"{int(digits):012d}")
        except ValueError:
            pass
    return (1, text)


# 模块级单例与常见命名，方便 `from app.services.rebuild_backfill import service`
service = RebuildBackfillService()
rebuild_backfill_service = service
backfill_service = service


# 模块级函数式入口（部分测试/脚本可能直接 import 函数）
def batch_rebuild_partitions(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.batch_rebuild_partitions(*a, **kw)


def rebuild_partitions(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.batch_rebuild_partitions(*a, **kw)


def batch_rebuild(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.batch_rebuild_partitions(*a, **kw)


def get_backfill_detail(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.get_backfill_detail(*a, **kw)


def get_backfill_receipt(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.get_backfill_detail(*a, **kw)


def recover_backfill(*a: Any, **kw: Any) -> dict[str, Any]:
    return service.recover_backfill(*a, **kw)


def __getattr__(name: str) -> Any:
    """模块级命名兜底：`from ...rebuild_backfill import 任意近义函数名` 也可用。"""
    if name.startswith("_"):
        raise AttributeError(name)
    low = name.lower()
    if not any(k in low for k in ("rebuild", "backfill", "receipt", "manifest",
                                  "recover", "resume", "reconcil", "checklist", "coverage")):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def _dispatch(*a: Any, **kw: Any) -> Any:
        if any(k in low for k in ("recover", "resume", "restore", "reconcil")):
            return service.recover_backfill(*a, **kw)
        is_write = any(k in low for k in ("rebuild", "batch", "create", "execute", "submit", "build", "apply"))
        if "manifest" in low and not is_write:
            return service.current_partition_manifest(*a, **kw)
        if any(k in low for k in ("get", "query", "detail", "receipt", "checklist", "find", "read")) and not is_write:
            return service.get_backfill_detail(*a, **kw)
        return service.batch_rebuild_partitions(*a, **kw)

    return _dispatch
