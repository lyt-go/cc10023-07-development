"""分片交付接口：预计分区、分片回执、分片清单、版本发布/归档/重新打开与 CSV 下载。

这些接口是分片交付的完整入口，通用资源路由仍负责 18 个业务模块的列表/登记/动作。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/predict_delivery", tags=["分片交付"])


def _predict_service():
    service = service_for("predict_batch")
    if service is None:  # pragma: no cover - 注册表固定存在
        raise HTTPException(status_code=500, detail="预测任务服务未注册")
    return service


def _export_service():
    service = service_for("data_export")
    if service is None:  # pragma: no cover - 注册表固定存在
        raise HTTPException(status_code=500, detail="数据导出服务未注册")
    return service


class PartitionPayload(BaseModel):
    partitions: list[Any] | int | str


class ShardReceiptPayload(BaseModel):
    shard_id: str
    rows: list[dict[str, Any]] = Field(default_factory=list)
    model_version: str | None = None
    partition_index: int | None = None
    status: str = "已完成"


class PublishPayload(BaseModel):
    model_version: str | None = None
    require_full: bool = False


class ManifestExportPayload(BaseModel):
    task_id: int
    model_version: str | None = None
    scope: str = "spot"
    version_id: int | None = None


@router.post("/batches/{task_id}/partitions")
def register_partitions(task_id: int, payload: PartitionPayload) -> dict[str, Any]:
    """登记原预测批次的预计分区（分片到齐的核对基准）。"""
    return _predict_service().register_batch_partitions(task_id, payload.partitions)


@router.get("/batches/{task_id}/partitions")
def list_partitions(task_id: int) -> dict[str, Any]:
    return _predict_service().list_batch_partitions(task_id)


@router.post("/batches/{task_id}/shards")
def register_shard(task_id: int, payload: ShardReceiptPayload) -> dict[str, Any]:
    """登记分片完成回执；空结果行也落账。"""
    return _predict_service().register_shard_receipt(
        task_id,
        payload.shard_id,
        payload.rows,
        model_version=payload.model_version,
        partition_index=payload.partition_index,
        status=payload.status,
    )


@router.get("/batches/{task_id}/shards")
def collect_shards(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    return _predict_service().collect_completed_shards(task_id, model_version=model_version)


@router.get("/batches/{task_id}/manifest")
def shard_manifest(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    """带覆盖范围的预测分片文件清单。"""
    return _predict_service().build_shard_manifest(task_id, model_version=model_version)


@router.get("/batches/{task_id}/rows")
def merged_rows(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    return _predict_service().merge_shard_results(task_id, model_version=model_version)


@router.post("/batches/{task_id}/versions")
def publish_version(task_id: int, payload: PublishPayload) -> dict[str, Any]:
    """发布抽查版；require_full=true 时要求分片到齐才允许全量版。"""
    return _predict_service().publish_delivery_version(
        task_id, payload.model_version, require_full=payload.require_full)


@router.get("/batches/{task_id}/versions")
def list_versions(task_id: int) -> dict[str, Any]:
    versions = _predict_service().list_delivery_versions(task_id)
    return {"ok": True, "task_id": task_id, "versions": versions, "total": len(versions)}


@router.get("/versions/{version_id}")
def get_version(version_id: int) -> dict[str, Any]:
    from app.services.prediction_delivery import prediction_delivery

    version = prediction_delivery.get_version(version_id)
    if version is None:
        raise HTTPException(status_code=404, detail=f"预测版本 {version_id} 不存在")
    return version


@router.post("/versions/{version_id}/archive")
def archive_version(version_id: int) -> dict[str, Any]:
    return _predict_service().archive_delivery_version(version_id)


@router.post("/versions/{version_id}/reopen")
def reopen_version(version_id: int) -> dict[str, Any]:
    return _predict_service().reopen_delivery_version(version_id)


@router.get("/versions/{version_id}/export.csv")
def download_version_csv(version_id: int, scope: str = "spot") -> StreamingResponse:
    """从已发布/已归档版本下载 CSV；scope=full 时要求版本带全量标记。"""
    from app.services.prediction_delivery import prediction_delivery

    result = prediction_delivery.render_version_csv(version_id, scope=scope)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message"))
    return _csv_response(result)


@router.post("/exports")
def create_manifest_export(payload: ManifestExportPayload) -> dict[str, Any]:
    """数据导出管理按分片清单创建导出任务。"""
    return _export_service().create_export_from_manifest(
        payload.task_id,
        model_version=payload.model_version,
        scope=payload.scope,
        version_id=payload.version_id,
    )


@router.get("/exports/{export_id}/export.csv")
def download_export_csv(export_id: int, scope: str = "spot") -> StreamingResponse:
    """按导出任务绑定的分片版本下载 CSV。"""
    result = _export_service().export_manifest_to_csv(export_id, scope=scope)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message"))
    return _csv_response(result)


def _csv_response(result: dict[str, Any]) -> StreamingResponse:
    return StreamingResponse(
        iter([result["content"]]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"',
            "X-Version-Label": str(result["version_label"]),
            "X-Coverage-Range": str(result["coverage_range"]),
            "X-Full-Coverage": "true" if result["full_coverage"] else "false",
            "X-Shard-Ids": ",".join(result["shard_ids"]),
        },
    )
