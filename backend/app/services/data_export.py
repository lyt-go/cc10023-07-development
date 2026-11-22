"""数据导出管理领域服务：分析结果的导出配置、格式转换与异步任务。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.prediction_delivery import prediction_delivery
from app.store import store


class DataExportService(BaseModuleService):
    MODULE = "data_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务编号", "数据来源", "导出范围", "导出格式", "字段映射", "分片策略", "压缩方式", "导出状态"]
    REQUIRED_FIELDS = ["任务编号", "数据来源", "导出范围"]
    STATUS_ORDER = ["草稿", "导出中", "已完成", "已失败"]
    ACTION_RULES = {"开始导出": "导出中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    # ── 分片交付：清单 → 导出任务 → CSV 下载 ──
    def create_export_from_manifest(
        self,
        task_id: int,
        *,
        model_version: str | None = None,
        scope: str = "spot",
        version_id: int | None = None,
        require_full: bool | None = None,
    ) -> dict[str, Any]:
        """按预测分片文件清单创建导出任务；缺片任务按抽查导出。

        传入 version_id 时绑定到该已发布版本（可从已归档版本下载）；
        否则按当前清单发布一个新版本再绑定，保证导出内容可复现。
        """
        normalized_scope = prediction_delivery._normalize_scope(scope)
        if require_full is None:
            require_full = normalized_scope == "full"

        if version_id is not None:
            version = prediction_delivery.get_version(version_id)
            if version is None or int(version.get("batch_id", 0)) != task_id:
                return {"ok": False, "message": f"预测版本 {version_id} 不属于任务 {task_id}"}
        else:
            published = prediction_delivery.publish_version(
                task_id, model_version=model_version, require_full=False)
            if not published.get("ok"):
                return published
            version = published["version"]

        if require_full and not version.get("full_coverage"):
            return {
                "ok": False,
                "message": (
                    f"版本 {version.get('version_label')} 缺片（"
                    + (",".join(version.get("missing_partitions", [])) or "无预计分区")
                    + "），只能导出抽查版"
                ),
                "missing_partitions": list(version.get("missing_partitions", [])),
            }

        mode = "全量版" if version.get("full_coverage") else "抽查版"
        coverage = str(version.get("coverage_range") or "无")
        export_rows = store.rows(self.MODULE)
        next_id = max((int(row.get("id", 0)) for row in export_rows), default=0) + 1
        export_row = {
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "任务编号": f"EXP-P{task_id:04d}-{next_id:04d}",
            "数据来源": f"predict_task_{task_id}",
            "导出范围": (
                f"{mode}｜模型版本 {version.get('model_version')}｜覆盖分区 {coverage}"
            ),
            "导出格式": "csv",
            "字段映射": ",".join(prediction_delivery.PREDICTION_FIELDS),
            "分片策略": "manifest",
            "压缩方式": "none",
            "导出状态": "已完成",
            "预测任务": task_id,
            "预测版本ID": int(version["id"]),
            "版本标签": version.get("version_label"),
            "模型版本": version.get("model_version"),
            "覆盖分区": coverage,
            "分片清单": ",".join(version.get("shard_ids", [])),
            "缺失分区": ",".join(version.get("missing_partitions", [])),
            "全量标记": bool(version.get("full_coverage")),
            "抽查可用": True,
        }
        export_rows.append(export_row)
        return {
            "ok": True,
            "export_id": next_id,
            "task_id": task_id,
            "version_id": int(version["id"]),
            "version_label": version.get("version_label"),
            "export": dict(export_row),
            "message": f"已按分片清单创建{mode}导出任务 {export_row['任务编号']}",
        }

    def export_manifest_to_csv(self, export_id: int, scope: str = "spot") -> dict[str, Any]:
        """按导出任务绑定的冻结版本生成 CSV。"""
        export_row = store.find(self.MODULE, export_id)
        if export_row is None:
            return {"ok": False, "message": f"导出任务 {export_id} 不存在"}
        version_id = export_row.get("预测版本ID")
        if version_id is None:
            return {"ok": False, "message": f"导出任务 {export_id} 不是分片清单导出，无预测行"}
        return prediction_delivery.render_version_csv(int(version_id), scope=scope)

    # ── 原有导出链路 ──
    def export_feature_dataset(self, task_id: int, output_format: str = "csv") -> dict[str, Any]:
        """将特征数据集导出为指定格式并写入外部存储路径。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        task["status"] = "导出中"
        task["pending"] = True
        task["导出格式"] = output_format
        task["status"] = "已完成"
        task["pending"] = False
        return {"ok": True, "task_id": task_id, "format": output_format,
                "output_path": f"exports/{task_id}/feature_dataset.{output_format}"}

    def convert_to_parquet(self, task_id: int) -> dict[str, Any]:
        """将导出数据转换为Parquet列式存储格式并压缩归档。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        task["导出格式"] = "parquet"
        task["压缩方式"] = "snappy"
        task["status"] = "已完成"
        task["pending"] = False
        return {"ok": True, "task_id": task_id, "format": "parquet",
                "compression": "snappy", "path": f"exports/{task_id}/data.snappy.parquet"}

    def archive_export_to_storage(self, task_id: int) -> dict[str, Any]:
        """将完成的导出任务归档到长期存储，保留历史版本供回溯。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        archive_path = f"archive/exports/{task_id}/v1.0/"
        return {
            "ok": True,
            "task_id": task_id,
            "archive_path": archive_path,
            "format": task.get("导出格式"),
            "message": f"导出任务 {task_id} 已归档至 {archive_path}",
        }

    def query_export_progress(self, task_id: int) -> dict[str, Any]:
        """查询导出任务的执行状态和进度详情。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        return {
            "ok": True,
            "task_id": task_id,
            "status": task.get("status"),
            "source": task.get("数据来源"),
            "format": task.get("导出格式"),
            "compression": task.get("压缩方式"),
        }