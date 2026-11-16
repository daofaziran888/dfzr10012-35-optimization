"""日志归档游标迁移接口。

承载两个执行入口：
- 迁移入口：/api/log/archive/{id}/migrate、/resume（行号 -> 事件时间偏移）
- 消费入口：/api/log/archive/{id}/consume（共用同一份异常归档数据索引）

以及索引查询、旧游标映射核对、导入/导出/上传/下载核对。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.log_archive import log_archive_service

router = APIRouter(prefix="/api/log/archive", tags=["日志归档游标迁移"])


class MigratePayload(BaseModel):
    stop_after_row: int | None = Field(default=None, description="模拟中断：迁移到该旧行号（含）后停止")
    simulate_interrupt: bool = Field(default=False, description="模拟部署中断")


class ImportPayload(BaseModel):
    payload: dict[str, Any] = Field(description="待导入的异常归档数据索引文件内容")


class ConsumePayload(BaseModel):
    max_items: int | None = Field(default=None, description="本次消费最多交付条数")


@router.get("")
def list_archives() -> dict[str, Any]:
    """列出全部归档索引及其迁移终态、续做位置。"""
    return {"items": log_archive_service.list_indexes(), "total": len(log_archive_service.list_indexes())}


@router.get("/{archive_id}")
def get_archive(archive_id: str) -> dict[str, Any]:
    """读取单份异常归档数据索引（含事件偏移、已处理游标、续做位置）。"""
    index = log_archive_service.get_index(archive_id)
    if index is None:
        raise HTTPException(status_code=404, detail=f"归档 {archive_id} 的异常归档数据索引不存在")
    return index


@router.post("/{archive_id}/migrate")
def migrate(archive_id: str, body: MigratePayload | None = None) -> dict[str, Any]:
    """执行入口一：行号游标迁移到事件时间偏移；并发调用只生效一次。"""
    body = body or MigratePayload()
    index, message, executed = log_archive_service.run_migration(
        archive_id,
        stop_after_row=body.stop_after_row,
        simulate_interrupt=body.simulate_interrupt,
    )
    return {"ok": True, "message": message, "executed": executed, "index": index}


@router.post("/{archive_id}/resume")
def resume(archive_id: str) -> dict[str, Any]:
    """部署中断后续做：从索引保留的续做位置继续，不重读已归档区间。"""
    index, message = log_archive_service.resume_migration(archive_id)
    if index is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "index": index}


@router.get("/{archive_id}/resolve/{row_cursor:path}")
def resolve_cursor(archive_id: str, row_cursor: str) -> dict[str, Any]:
    """旧行号游标必须映射到唯一事件时间偏移后才能继续读取。"""
    result, message = log_archive_service.resolve_legacy_cursor(archive_id, row_cursor)
    if result is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "resolution": result}


@router.post("/{archive_id}/consume")
def consume(archive_id: str, body: ConsumePayload | None = None) -> dict[str, Any]:
    """执行入口二（消费侧）：使用同一份索引继续后续处理。"""
    body = body or ConsumePayload()
    index, message, delivered = log_archive_service.consume(archive_id, max_items=body.max_items)
    if index is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "delivered": delivered, "index": index}


@router.get("/{archive_id}/export")
def export_archive(archive_id: str) -> dict[str, Any]:
    """导出索引并登记上传记录（含 checksum）。"""
    payload, message = log_archive_service.export_index(archive_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "export": payload}


@router.get("/{archive_id}/download")
def download_archive(archive_id: str) -> dict[str, Any]:
    """下载索引并登记下载结果（含 checksum）。"""
    payload, message = log_archive_service.download_index(archive_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "download": payload}


@router.post("/{archive_id}/import")
def import_archive(archive_id: str, body: ImportPayload) -> dict[str, Any]:
    """导入索引文件：逐项核对内容、映射唯一性与 checksum 后才生效。"""
    index, message = log_archive_service.import_index(archive_id, body.payload)
    return {"ok": index is not None, "message": message, "index": index}


@router.post("/reset")
def reset_archives(archive_id: str | None = Query(default=None)) -> dict[str, Any]:
    """运维/验收辅助：清空指定归档或全部内存索引。"""
    log_archive_service.reset(archive_id)
    return {"ok": True, "message": "归档索引已重置"}
