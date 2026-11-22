"""分片交付的兼容入口：让批量预测/数据导出模块也能按模块路径直接操作。

分片交付的主入口在 /api/predict_delivery 下；这里额外提供
/api/predict_batch/{id}/... 与 /api/data_export/{id}/... 的模块级路径，
方便工作表页面和既有调用方按业务模块定位。所有路径最终都落到同一套
领域服务，行为完全一致。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.services import service_for
from app.services.prediction_delivery import prediction_delivery

router = APIRouter(prefix="/api", tags=["分片交付-模块入口"])


def _predict():
    service = service_for("predict_batch")
    if service is None:  # pragma: no cover
        raise HTTPException(status_code=500, detail="预测任务服务未注册")
    return service


def _export():
    service = service_for("data_export")
    if service is None:  # pragma: no cover
        raise HTTPException(status_code=500, detail="数据导出服务未注册")
    return service


def _pick(payload: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in payload and payload[name] is not None:
            return payload[name]
    return default


def _parse_rows(raw: Any) -> list[dict[str, Any]]:
    """接受列表、JSON 字符串或 CSV 文本三种结果行写法。"""
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("["):
            import json
            try:
                return [row for row in json.loads(text) if isinstance(row, dict)]
            except ValueError:
                return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]
    return []


# ── 批量预测模块入口 ────────────────────────────────────────
@router.post("/predict_batch/{task_id}/partitions")
def register_partitions(task_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    partitions = _pick(payload, "partitions", "分区", "预计分区")
    if partitions is None:
        raise HTTPException(status_code=422, detail="缺少 partitions")
    return _predict().register_batch_partitions(task_id, partitions)


@router.get("/predict_batch/{task_id}/partitions")
def list_partitions(task_id: int) -> dict[str, Any]:
    return _predict().list_batch_partitions(task_id)


@router.post("/predict_batch/{task_id}/shards")
def register_shard(task_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    shard_id = _pick(payload, "shard_id", "分片编号", "shard", "分区")
    if not shard_id:
        raise HTTPException(status_code=422, detail="缺少 shard_id")
    rows = _parse_rows(_pick(payload, "rows", "结果行", "predictions", "预测行"))
    return _predict().register_shard_receipt(
        task_id,
        str(shard_id),
        rows,
        model_version=_pick(payload, "model_version", "模型版本"),
        partition_index=_pick(payload, "partition_index", "分区序号"),
        status=str(_pick(payload, "status", "状态", default="已完成")),
    )


@router.get("/predict_batch/{task_id}/shards")
def collect_shards(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    return _predict().collect_completed_shards(task_id, model_version=model_version)


@router.get("/predict_batch/{task_id}/manifest")
def shard_manifest(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    return _predict().build_shard_manifest(task_id, model_version=model_version)


@router.get("/predict_batch/{task_id}/rows")
def merged_rows(task_id: int, model_version: str | None = None) -> dict[str, Any]:
    return _predict().merge_shard_results(task_id, model_version=model_version)


@router.post("/predict_batch/{task_id}/versions")
def publish_version(task_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    return _predict().publish_delivery_version(
        task_id,
        _pick(payload, "model_version", "模型版本"),
        require_full=bool(_pick(payload, "require_full", "全量", default=False)),
    )


@router.get("/predict_batch/{task_id}/versions")
def list_versions(task_id: int) -> dict[str, Any]:
    versions = _predict().list_delivery_versions(task_id)
    return {"ok": True, "task_id": task_id, "versions": versions, "total": len(versions)}


@router.get("/predict_batch/versions/{version_id}")
def get_version(version_id: int) -> dict[str, Any]:
    version = prediction_delivery.get_version(version_id)
    if version is None:
        raise HTTPException(status_code=404, detail=f"预测版本 {version_id} 不存在")
    return version


@router.post("/predict_batch/versions/{version_id}/archive")
def archive_version(version_id: int) -> dict[str, Any]:
    return _predict().archive_delivery_version(version_id)


@router.post("/predict_batch/versions/{version_id}/reopen")
def reopen_version(version_id: int) -> dict[str, Any]:
    return _predict().reopen_delivery_version(version_id)


@router.get("/predict_batch/versions/{version_id}/export.csv")
def download_version_csv(version_id: int, scope: str = "spot") -> StreamingResponse:
    return _download(prediction_delivery.render_version_csv(version_id, scope=scope))


# ── 数据导出模块入口 ────────────────────────────────────────
@router.post("/data_export/from_manifest")
def create_export_from_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    task_id = _pick(payload, "task_id", "预测任务", "batch_id")
    if task_id is None:
        raise HTTPException(status_code=422, detail="缺少 task_id")
    return _export().create_export_from_manifest(
        int(task_id),
        model_version=_pick(payload, "model_version", "模型版本"),
        scope=str(_pick(payload, "scope", "导出范围", default="spot")),
        version_id=(
            int(_pick(payload, "version_id", "版本id"))
            if _pick(payload, "version_id", "版本id") is not None else None
        ),
    )


@router.get("/data_export/{export_id}/export.csv")
def download_export_csv(export_id: int, scope: str = "spot") -> StreamingResponse:
    return _download(_export().export_manifest_to_csv(export_id, scope=scope))


def _download(result: dict[str, Any]) -> StreamingResponse:
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message"))
    return StreamingResponse(
        iter([result["content"]]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"',
            "X-Version-Label": str(result["version_label"]),
            "X-Model-Version": str(result["model_version"]),
            "X-Coverage-Range": str(result["coverage_range"]),
            "X-Full-Coverage": "true" if result["full_coverage"] else "false",
            "X-Shard-Ids": ",".join(result["shard_ids"]),
        },
    )
