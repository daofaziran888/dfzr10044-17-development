"""业务路由汇总：补数批量回执专用路由优先注册，其后是所有模块共用的资源路由。"""
from __future__ import annotations

from app.routers import backfill_api, resource

ROUTERS = [backfill_api, resource]
