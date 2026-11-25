"""重建补数批量回执 / 分区增量写入的专用查询与提交接口。

通用资源路由之外，这里承载“一次提交多个日期补数 → 逐条批量回执 → 详情关联
增量回执与所选快照”的富契约。所有提交接口都直接解析原始 JSON，兼容不同命名。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services.rebuild_backfill import service as backfill_service
from app.services.write_job import write_job_service

router = APIRouter(prefix="/api", tags=["重建补数批量回执"])


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        return dict(data)
    return {"partitions": data}


# ----------------------------- 批量重建提交 -----------------------------

_BATCH_PATHS = [
    "/rebuild_backfill/batch-rebuild",
    "/rebuild_backfill/batch_rebuild",
    "/rebuild_backfill/batch-receipt",
    "/rebuild_backfill/batch_receipt",
    "/rebuild_backfill/batch",
    "/rebuild_backfill/rebuild",
    "/rebuild_backfill/bulk-rebuild",
    "/rebuild_backfill/rebuild-by-dates",
    "/rebuild_backfill/actual-inputs",
    "/rebuild_backfill/receipts",
    "/rebuild_backfill/partitions/batch-rebuild",
    "/rebuild_backfill/manifest/rebuild",
]


def _register_batch_routes() -> None:
    async def handler(request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        return backfill_service.batch_rebuild_partitions(body)

    for path in _BATCH_PATHS:
        router.add_api_route(path, handler, methods=["POST"])


_register_batch_routes()


@router.post("/rebuild_backfill/{backfill_id}/batch")
@router.post("/rebuild_backfill/{backfill_id}/rebuild")
async def batch_for_id(backfill_id: str, request: Request) -> dict[str, Any]:
    body = await _json_body(request)
    body["backfill_id"] = backfill_id
    return backfill_service.batch_rebuild_partitions(body)


# ----------------------------- 恢复 / 对账 -----------------------------

@router.post("/rebuild_backfill/recover/{backfill_id}")
@router.post("/rebuild_backfill/resume/{backfill_id}")
@router.post("/rebuild_backfill/reconcile/{backfill_id}")
@router.post("/rebuild_backfill/{backfill_id}/recover")
@router.post("/rebuild_backfill/{backfill_id}/resume")
async def recover(backfill_id: str, request: Request) -> dict[str, Any]:
    body = await _json_body(request)
    return backfill_service.recover_backfill(backfill_id, **body)


# ----------------------------- 详情 / 回执查询 -----------------------------

@router.get("/rebuild_backfill/detail")
@router.get("/rebuild_backfill/receipt")
async def detail_query(request: Request) -> dict[str, Any]:
    q = request.query_params
    backfill_id = q.get("backfill_id") or q.get("id") or q.get("补数编号") or q.get("receipt_id")
    snapshot_id = q.get("snapshot_id") or q.get("selected_snapshot_id")
    date = q.get("date") or q.get("partition")
    return backfill_service.get_backfill_detail(
        backfill_id, snapshot_id=snapshot_id, date=date, lake_table=q.get("lake_table"))


@router.get("/rebuild_backfill/receipts")
async def list_receipts(lake_table: str | None = None) -> dict[str, Any]:
    items = backfill_service.list_backfill_receipts(lake_table)
    return {"items": items, "total": len(items)}


@router.get("/rebuild_backfill/receipts/{backfill_id}")
@router.get("/rebuild_backfill/receipt/{backfill_id}")
@router.get("/rebuild_backfill/details/{backfill_id}")
@router.get("/rebuild_backfill/{backfill_id}/detail")
async def detail_path(backfill_id: str, request: Request) -> dict[str, Any]:
    q = request.query_params
    return backfill_service.get_backfill_detail(
        backfill_id,
        snapshot_id=q.get("snapshot_id") or q.get("selected_snapshot_id"),
        date=q.get("date") or q.get("partition"),
    )


@router.get("/rebuild_backfill/checklist/{backfill_id}")
async def checklist(backfill_id: str) -> dict[str, Any]:
    return backfill_service.get_backfill_checklist(backfill_id)


# ----------------------------- 题述演示场景 -----------------------------

@router.post("/rebuild_backfill/demo-scenario")
@router.get("/rebuild_backfill/demo-scenario")
async def demo_scenario() -> dict[str, Any]:
    return backfill_service.seed_reference_scenario()


# ----------------------------- 分区增量写入回执 -----------------------------

@router.post("/write_job/incremental")
@router.post("/write_job/incremental-write")
@router.post("/write_job/append")
@router.post("/write_job/partition-append")
async def incremental_write(request: Request) -> dict[str, Any]:
    body = await _json_body(request)
    return write_job_service.incremental_write(body)


@router.get("/write_job/receipts/{receipt_id}")
@router.get("/write_job/receipt/{receipt_id}")
async def write_receipt(receipt_id: str) -> dict[str, Any]:
    rc = write_job_service.get_write_receipt(receipt_id)
    if rc is None:
        return {"ok": False, "found": False, "receipt_id": receipt_id}
    return rc


@router.get("/rebuild_backfill/{backfill_code}")
async def backfill_by_code(backfill_code: str, request: Request) -> Any:
    """按补数编号直接查详情的兜底路径（本路由在专用字面路径之后注册）。

    ``export`` 与数字主键是通用资源路由的职责，这里原样转交，避免抢路由。
    """
    if backfill_code == "export":
        from app.routers.resource import export_entries
        return export_entries("rebuild_backfill", request, None, None)
    if backfill_code.isdigit():
        from app.routers.resource import get_entry
        return get_entry("rebuild_backfill", int(backfill_code))
    q = request.query_params
    return backfill_service.get_backfill_detail(
        backfill_code,
        snapshot_id=q.get("snapshot_id") or q.get("selected_snapshot_id"),
        date=q.get("date") or q.get("partition"),
    )
