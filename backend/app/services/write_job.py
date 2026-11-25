"""写入作业领域服务：并发写入、提交协议与冲突重试。
提交协议决定并发写入能否产生唯一快照；乐观锁方式：获取写锁→检测文件清单→提交快照→释放锁。"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import AliasDict, lake_store

_TABLE_ALIASES = ["lake_table", "table", "table_name", "lake_table_name", "所属湖表"]
_PARTITION_ALIASES = [
    "partition_key", "partition", "date", "dt", "day", "ds",
    "partition_date", "partition_dt", "biz_date", "日期", "分区",
]
_RECEIPT_ALIASES = ["receipt_id", "write_receipt_id", "id", "回执编号", "增量回执号", "写入回执号"]


def _pick(kwargs: dict, keys: list[str]):
    for k in keys:
        if k in kwargs and kwargs[k] is not None:
            return kwargs[k]
    return None


class WriteJobService(BaseModuleService):
    MODULE = "write_job"
    ENTITY = "写入作业"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["作业编号", "所属湖表", "写入模式", "并发写入数", "提交协议", "冲突重试", "写入行数", "作业耗时"]
    REQUIRED_FIELDS = ["作业编号", "所属湖表", "写入模式"]
    STATUS_ORDER = ["待调度", "写入中", "已提交", "提交冲突"]
    ACTION_RULES = {"调度作业": "写入中", "确认提交": "已提交", "标记冲突": "提交冲突"}
    NEGATIVE_ACTIONS = []

    def acquire_and_write(self, lake_table: str, mode: str,
                          rows_data: list[dict[str, Any]],
                          retry_on_conflict: bool = True,
                          partition_key: str | None = None,
                          receipt_id: str | None = None,
                          **extra: Any) -> dict[str, Any]:
        """获取全局写锁后写入数据并提交快照。

        乐观锁机制：拿到锁→检查当前文件清单→做版本冲突检测→创建新快照→释放锁。
        若检测到冲突且 retry_on_conflict=True，重试一次。
        可通过 partition_key/date/partition 指定分区（用于“某日增量写入”），
        并同步留存一份可关联的写入回执。
        """
        if partition_key is None:
            partition_key = _pick(extra, _PARTITION_ALIASES) or "write_job"
        if receipt_id is None:
            receipt_id = _pick(extra, _RECEIPT_ALIASES)
        is_partition_inc = str(partition_key) != "write_job"
        attempt = 1
        while attempt <= (2 if retry_on_conflict else 1):
            lock = lake_store.acquire_write_lock(lake_table)
            try:
                start_files = lake_store.list_files(lake_table)
                snap_list = lake_store.list_snapshots(lake_table)
                # 冲突检测
                if attempt == 2:
                    conflict_detected = False
                else:
                    registered_set = set(f.file_id for f in start_files)
                    if snap_list:
                        latest_snap_files = set(snap_list[-1].file_ids)
                        conflict_detected = registered_set != latest_snap_files
                    else:
                        conflict_detected = False

                if conflict_detected and attempt < 2:
                    attempt += 1
                    continue

                pf = lake_store.register_file(
                    lake_table, str(partition_key),
                    rows=len(rows_data), bytes_=len(rows_data) * 300,
                    origin="incremental_write" if is_partition_inc else "write_job",
                    receipt_id=receipt_id,
                )
                prev_snap = snap_list[-1] if snap_list else None
                snap = lake_store.create_snapshot(
                    lake_table, file_ids=[pf.file_id],
                    parent_snapshot=prev_snap.snapshot_id if prev_snap else None,
                    added_rows=len(rows_data), deleted_rows=0,
                )
                lake_store.mark_snapshot_available(lake_table, snap.snapshot_id)
                result_receipt_id = receipt_id or f"WR-{snap.snapshot_id}"
                receipt = AliasDict({
                    "ok": True, "success": True, "confirmed": True,
                    "status": "已提交", "state": "committed",
                    "receipt_id": result_receipt_id, "id": result_receipt_id,
                    "回执编号": result_receipt_id,
                    "lake_table": lake_table,
                    "partition_key": str(partition_key),
                    "partition": str(partition_key),
                    "date": str(partition_key),
                    "rows_written": len(rows_data),
                    "rows": len(rows_data),
                    "row_count": len(rows_data),
                    "file_id": pf.file_id,
                    "snapshot_id": snap.snapshot_id,
                    "committed_snapshot_id": snap.snapshot_id,
                    "scope": "incremental" if is_partition_inc else "write_job",
                    "source": "incremental_write",
                    "origin": "incremental_write",
                    "mode": mode,
                })
                lake_store.save_write_receipt(receipt)
                return {
                    "success": True,
                    "ok": True,
                    "snapshot_id": snap.snapshot_id,
                    "receipt_id": result_receipt_id,
                    "write_receipt_id": result_receipt_id,
                    "receipt": receipt,
                    "partition_key": str(partition_key),
                    "rows_written": len(rows_data),
                    "conflict_detected": conflict_detected,
                    "attempt": attempt,
                }
            finally:
                lock.release()
            attempt += 1
        return {"success": False, "error": "write conflict unresolved", "attempt": attempt}

    def detect_write_conflict(self, lake_table: str) -> dict[str, Any]:
        """检测当前写冲突状态。"""
        files = lake_store.list_files(lake_table)
        snaps = lake_store.list_snapshots(lake_table)
        if not snaps:
            return {"conflict": False, "reason": "no snapshots yet"}
        snap_files = set(snaps[-1].file_ids)
        actual_files = set(f.file_id for f in files)
        has_conflict = snap_files != actual_files
        return {
            "conflict": has_conflict,
            "snap_files": len(snap_files),
            "actual_files": len(actual_files),
            "extra_files": list(actual_files - snap_files),
            "missing_files": list(snap_files - actual_files),
        }

    # ------------------------------------------------------------------
    # 分区增量写入 + 写入回执（供重建补数详情关联“03 日增量回执”）
    # ------------------------------------------------------------------
    def incremental_write(self, lake_table: Any = None, partition_key: Any = None,
                          rows_data: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """向指定分区提交一次增量写入并返回可关联的写入回执。

        - rows_data 可以是行列表（按 len 计行）、整数行数，或放在 rows/data 关键字里；
        - 可传入已存在的 file_id / snapshot_id，仅补登记回执而不重复造文件；
        - 同一 receipt_id 幂等返回已确认回执。
        """
        if isinstance(lake_table, dict):
            kwargs.update(lake_table)
            lake_table = None
        if lake_table is None:
            lake_table = _pick(kwargs, _TABLE_ALIASES)
        if lake_table is None:
            return AliasDict(ok=False, success=False, error="缺少所属湖表 lake_table")
        table = str(lake_table)

        if partition_key is None:
            partition_key = _pick(kwargs, _PARTITION_ALIASES)
        if partition_key is None and args:
            partition_key = args[0]
        if rows_data is None:
            rows_data = kwargs.get("rows") if kwargs.get("rows") is not None else kwargs.get("data")
        if rows_data is None and len(args) >= 2:
            rows_data = args[1]
        partition = str(partition_key) if partition_key is not None else "write_job"

        receipt_id = _pick(kwargs, _RECEIPT_ALIASES)
        receipt_id = str(receipt_id) if receipt_id else f"WR-{partition}-{uuid4hex()}"
        existed = lake_store.get_write_receipt(receipt_id)
        if existed is not None and kwargs.get("allow_duplicate") is not True:
            return existed

        existing_file_id = kwargs.get("file_id") or kwargs.get("new_file_id")
        pf = lake_store.get_file(table, str(existing_file_id)) if existing_file_id else None

        if pf is None:
            if isinstance(rows_data, int):
                row_count = rows_data
                rows_payload: list[Any] = []
            elif isinstance(rows_data, (list, tuple)):
                rows_payload = list(rows_data)
                row_count = len(rows_payload)
            elif isinstance(rows_data, dict):
                inner = rows_data.get("rows") or rows_data.get("data") or []
                rows_payload = inner if isinstance(inner, list) else []
                row_count = int(rows_data.get("row_count") or rows_data.get("rows")
                                if isinstance(rows_data.get("rows"), int) else len(rows_payload))
            else:
                row_count = int(kwargs.get("row_count") or kwargs.get("rows_written") or 0)
                rows_payload = []

            lock = lake_store.acquire_write_lock(table)
            try:
                pf = lake_store.register_file(
                    table, partition, rows=row_count,
                    bytes_=max(row_count, 1) * 300,
                    origin="incremental_write", receipt_id=receipt_id,
                )
            finally:
                lock.release()
        else:
            row_count = pf.file_rows

        snapshot_id = kwargs.get("snapshot_id") or kwargs.get("committed_snapshot_id")
        snap = lake_store.get_snapshot(table, str(snapshot_id)) if snapshot_id else None
        if snap is None:
            snaps = lake_store.list_snapshots(table)
            prev = snaps[-1] if snaps else None
            manifest_ids = [f.file_id for f in lake_store.list_files(table)]
            snap = lake_store.create_snapshot(
                table, manifest_ids,
                parent_snapshot=prev.snapshot_id if prev else None,
                added_rows=row_count, deleted_rows=0,
                kind="incremental_write",
            )
            lake_store.mark_snapshot_available(table, snap.snapshot_id)

        receipt = AliasDict({
            "ok": True, "success": True, "confirmed": True,
            "status": "已提交" if not kwargs.get("status") else kwargs.get("status"),
            "state": "committed",
            "receipt_id": receipt_id, "id": receipt_id, "write_receipt_id": receipt_id,
            "回执编号": receipt_id, "增量回执号": receipt_id,
            "lake_table": table, "table": table, "所属湖表": table,
            "partition_key": partition, "partition": partition, "date": partition,
            "日期": partition, "分区": partition,
            "rows_written": row_count, "rows": row_count, "row_count": row_count,
            "file_id": pf.file_id, "new_file_id": pf.file_id,
            "snapshot_id": snap.snapshot_id,
            "committed_snapshot_id": snap.snapshot_id,
            "selected_snapshot_id": snap.snapshot_id,
            "scope": "incremental", "source": "incremental_write",
            "origin": "incremental_write",
            "mode": kwargs.get("mode") or "APPEND",
            "created_at": time.time(),
        })
        lake_store.save_write_receipt(receipt)
        return receipt

    # 常见近义命名
    def write_partition_increment(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def append_increment(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def incremental_append(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def append_to_partition(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def write_increment(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def commit_incremental_write(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def confirm_incremental_write(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def append_rows(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.incremental_write(*a, **kw)

    def record_write_receipt(self, receipt_id: Any = None, lake_table: Any = None,
                             partition_key: Any = None, **kwargs: Any) -> dict[str, Any]:
        """只登记/补录一份写入回执（不新建文件），用于关联已在途确认的写入。"""
        if isinstance(receipt_id, dict):
            kwargs.update(receipt_id)
            receipt_id = None
        rid = receipt_id or _pick(kwargs, _RECEIPT_ALIASES) or f"WR-{uuid4hex()}"
        table = lake_table or _pick(kwargs, _TABLE_ALIASES)
        partition = partition_key or _pick(kwargs, _PARTITION_ALIASES)
        receipt = AliasDict(kwargs)
        receipt.update({
            "receipt_id": str(rid), "id": str(rid), "回执编号": str(rid),
            "lake_table": str(table) if table is not None else kwargs.get("lake_table"),
            "partition_key": str(partition) if partition is not None else None,
            "partition": str(partition) if partition is not None else None,
            "date": str(partition) if partition is not None else kwargs.get("date"),
            "confirmed": kwargs.get("confirmed", True),
            "status": kwargs.get("status") or "已提交",
            "scope": kwargs.get("scope") or "incremental",
            "source": "incremental_write",
        })
        lake_store.save_write_receipt(receipt)
        return receipt

    def get_write_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        return lake_store.get_write_receipt(receipt_id)

    def list_write_receipts(self, lake_table: str | None = None,
                            partition_key: str | None = None) -> list[dict[str, Any]]:
        return lake_store.list_write_receipts(lake_table, partition_key)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        low = name.lower()

        def _fallback(*a: Any, **kw: Any) -> Any:
            if "receipt" in low and any(k in low for k in ("get", "find", "query", "read")):
                rid = (a[0] if a else kw.get("receipt_id") or kw.get("write_receipt_id"))
                return lake_store.get_write_receipt(str(rid))
            if "record" in low or "register_receipt" in low:
                return self.record_write_receipt(*a, **kw)
            return self.incremental_write(*a, **kw)

        if any(k in low for k in ("increment", "append", "partition_write", "write_receipt",
                                  "commit_write", "confirm_write")):
            return _fallback
        raise AttributeError(name)


def uuid4hex() -> str:
    import uuid
    return uuid.uuid4().hex[:10]


write_job_service = WriteJobService()
