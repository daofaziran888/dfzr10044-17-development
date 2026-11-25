"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def _normalize_lookup_key(key: Any) -> str:
    """把 camelCase / snake_case / 空格 / 连字符归一化成同一查询键。"""
    return "".join(ch for ch in str(key).lower() if ch not in "_- \t")


class AliasDict(dict):
    """带容错键查找的回执字典：`receipt.get("coveredDates")` 也能命中 `covered_dates`。

    显式写入的别名字段优先；未命中时再做大小写/分隔符归一化匹配，
    便于不同调用方按各自命名习惯读取同一份批量回执。
    """

    def __missing__(self, key: Any) -> Any:
        if not isinstance(key, str):
            raise KeyError(key)
        wanted = _normalize_lookup_key(key)
        for existing in self.keys():
            if isinstance(existing, str) and _normalize_lookup_key(existing) == wanted:
                return dict.__getitem__(self, existing)
        raise KeyError(key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


class SnapshotRef(str):
    """既是快照号字符串、又能当快照对象读取的兼容引用。

    - ``read_at_snapshot(table, receipt["snapshot"])`` 可直接当快照号用；
    - ``receipt["snapshot"]["status"]`` / ``.get("file_count")`` 也能取到视图字段。
    """

    view: dict[str, Any]

    def __new__(cls, view: dict[str, Any] | None):
        view = view or {}
        obj = str.__new__(cls, str(view.get("snapshot_id") or ""))
        obj.view = dict(view)
        return obj

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key == "snapshot_id":
                return str(self)
            if key in self.view:
                return self.view[key]
            raise KeyError(key)
        return str.__getitem__(self, key)

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        if key == "snapshot_id":
            return str(self)
        return self.view.get(key, default)

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        return key == "snapshot_id" or key in self.view


class ReceiptRef(str):
    """既是补数编号字符串、又能当回执对象读取的兼容引用（receipt["items"]）。"""

    record: dict[str, Any]

    def __new__(cls, identifier: str, record: dict[str, Any]):
        obj = str.__new__(cls, str(identifier))
        obj.record = record
        return obj

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key in ("receipt_id", "backfill_id", "id"):
                return str(self)
            if key in self.record:
                return self.record[key]
            raise KeyError(key)
        return str.__getitem__(self, key)

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        if key in ("receipt_id", "backfill_id", "id"):
            return str(self)
        return self.record.get(key, default)

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        return key in ("receipt_id", "backfill_id", "id") or key in self.record


class PartitionList(list):
    """既是逐条清单数组（可迭代/len/切片），又支持按日期字符串取单条：

    ``receipt["partitions"]["03"]`` 与 ``receipt["partitions"][2]`` 都成立。
    JSON 序列化时仍是普通数组。
    """

    _DATE_FIELDS = ("date", "partition", "partition_key", "day", "dt", "ds", "日期", "分区")

    def _match(self, key: str) -> dict[str, Any] | None:
        for item in self:
            if not isinstance(item, dict):
                continue
            for field in self._DATE_FIELDS:
                if str(item.get(field, "")) == key:
                    return item
        # 宽松兜底：末尾匹配（2026-11-03 对 “03”）或数字日相等
        key_digits = "".join(ch for ch in key if ch.isdigit())
        for item in self:
            if not isinstance(item, dict):
                continue
            value = str(item.get("date") or item.get("partition_key") or item.get("partition") or "")
            if key and value.endswith(str(key)):
                return item
            if key_digits:
                value_digits = "".join(ch for ch in value if ch.isdigit())
                if value_digits and int(value_digits) == int(key_digits) and len(key_digits) <= 2:
                    return item
        return None

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            item = self._match(key)
            if item is None:
                raise KeyError(key)
            return item
        return list.__getitem__(self, key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return [self._key_of(item) for item in self if isinstance(item, dict)]

    def values(self):
        return list(self)

    def items(self):
        return [(self._key_of(item), item) for item in self if isinstance(item, dict)]

    @staticmethod
    def _key_of(item: dict[str, Any]) -> str:
        for field in PartitionList._DATE_FIELDS:
            if item.get(field) is not None:
                return str(item[field])
        return ""


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    # 来源标记：registered=普通注册，backfill=补数替换产物，incremental_write=在途增量写入。
    # 补数替换与增量提交在同一分区冲突时，靠它分清各自处理的范围。
    origin: str = "registered"
    receipt_id: str | None = None


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float
    kind: str = "commit"  # commit | incremental_write | backfill_replace ...


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}（仅当前有效清单）
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 被替换/淘汰但仍被旧快照引用的文件：lake_table -> {file_id -> PartitionFile}
        # 不物理删除，保证按旧快照号做时间旅行时仍能读到原始范围。
        self._archived_files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写回执（含分区增量写入回执）：receipt_id -> dict
        self._write_receipts: dict[str, dict[str, Any]] = {}

        # 重建补数批量回执：backfill_id -> dict
        self._backfill_receipts: dict[str, dict[str, Any]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int,
        manifest: str | None = None, origin: str = "registered",
        receipt_id: str | None = None,
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            origin=origin,
            receipt_id=receipt_id,
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        """先查当前有效清单，再查归档清单，保证旧快照仍可解析被替换的文件。"""
        active = self._files.get(lake_table, {}).get(file_id)
        if active is not None:
            return active
        return self._archived_files.get(lake_table, {}).get(file_id)

    def is_active_file(self, lake_table: str, file_id: str) -> bool:
        return file_id in self._files.get(lake_table, {})

    def archive_files(self, lake_table: str, file_ids: list[str]) -> list[PartitionFile]:
        """把文件从当前清单移入归档（不物理删除），旧快照仍可通过 get_file 读到。"""
        table_files = self._files.get(lake_table, {})
        archive = self._archived_files.setdefault(lake_table, {})
        archived: list[PartitionFile] = []
        for fid in file_ids:
            pf = table_files.pop(fid, None)
            if pf is not None:
                archive[fid] = pf
                archived.append(pf)
        return archived

    def reset_table(self, lake_table: str) -> None:
        """清空某张表演示/测试用的全部内存态（文件、归档、快照、回执）。"""
        self._files.pop(lake_table, None)
        self._archived_files.pop(lake_table, None)
        self._snapshots.pop(lake_table, None)
        self._watermarks.pop(lake_table, None)
        for rid, rc in list(self._write_receipts.items()):
            if rc.get("lake_table") == lake_table:
                self._write_receipts.pop(rid, None)
        for bid, rc in list(self._backfill_receipts.items()):
            if rc.get("lake_table") == lake_table:
                self._backfill_receipts.pop(bid, None)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
        kind: str = "commit",
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
            kind=kind,
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 写入回执（含分区增量写入） ----

    def save_write_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        rid = str(receipt.get("receipt_id") or receipt.get("id") or "")
        if not rid:
            rid = f"WR-{uuid.uuid4().hex[:10]}"
            receipt["receipt_id"] = rid
        self._write_receipts[rid] = receipt
        return receipt

    def get_write_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        if receipt_id in self._write_receipts:
            return self._write_receipts[receipt_id]
        # 兼容按“回执编号”等业务键查找
        for rc in self._write_receipts.values():
            if receipt_id in {rc.get("回执编号"), rc.get("write_receipt_id")}:
                return rc
        return None

    def list_write_receipts(
        self, lake_table: str | None = None, partition_key: str | None = None
    ) -> list[dict[str, Any]]:
        result = list(self._write_receipts.values())
        if lake_table is not None:
            result = [r for r in result if r.get("lake_table") == lake_table]
        if partition_key is not None:
            result = [
                r for r in result
                if str(r.get("partition_key") or r.get("partition") or r.get("date") or "")
                == str(partition_key)
            ]
        return result

    # ---- 重建补数批量回执 ----

    def save_backfill_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        bid = str(receipt.get("backfill_id") or receipt.get("补数编号") or receipt.get("id") or "")
        if not bid:
            bid = f"BF-{uuid.uuid4().hex[:10]}"
            receipt["backfill_id"] = bid
        self._backfill_receipts[bid] = receipt
        return receipt

    def get_backfill_receipt(self, backfill_id: str) -> dict[str, Any] | None:
        if backfill_id in self._backfill_receipts:
            return self._backfill_receipts[backfill_id]
        for rc in self._backfill_receipts.values():
            if backfill_id in {rc.get("补数编号"), rc.get("backfill_no"), rc.get("id")}:
                return rc
        return None

    def list_backfill_receipts(self, lake_table: str | None = None) -> list[dict[str, Any]]:
        result = list(self._backfill_receipts.values())
        if lake_table is not None:
            result = [r for r in result if r.get("lake_table") == lake_table]
        return result

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()