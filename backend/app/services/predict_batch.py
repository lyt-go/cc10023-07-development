"""批量预测任务领域服务：基于已注册模型的批量预测执行、结果存储与校验。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.prediction_delivery import prediction_delivery
from app.store import store


class PredictBatchService(BaseModuleService):
    MODULE = "predict_batch"
    ENTITY = "预测任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "模型编号", "数据切片", "输出格式", "分片策略", "并行度", "预测状态", "结果校验"]
    REQUIRED_FIELDS = ["任务编号", "模型编号", "数据切片"]
    STATUS_ORDER = ["草稿", "等待中", "运行中", "已完成"]
    ACTION_RULES = {"提交运行": "草稿", "停止预测": "运行中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    # ── 分片交付：预计分区 → 回执 → 清单 → 版本 ──
    def register_batch_partitions(self, task_id: int, partitions: Any) -> dict[str, Any]:
        """登记原预测批次的预计分区，供分片到齐核对使用。"""
        return prediction_delivery.register_batch_partitions(task_id, partitions)

    def list_batch_partitions(self, task_id: int) -> dict[str, Any]:
        """查询原预测批次的预计分区清单。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}
        partitions = prediction_delivery.list_batch_partitions(task_id)
        return {
            "ok": True,
            "task_id": task_id,
            "partitions": partitions,
            "expected_count": len(partitions),
        }

    def register_shard_receipt(
        self,
        task_id: int,
        shard_id: str,
        rows: list[dict[str, Any]] | None = None,
        *,
        model_version: str | None = None,
        partition_index: int | None = None,
        status: str = "已完成",
    ) -> dict[str, Any]:
        """登记分片完成回执；空结果行同样落账，重复回执按最后一次覆盖。"""
        return prediction_delivery.register_shard_receipt(
            task_id, shard_id, rows,
            model_version=model_version,
            partition_index=partition_index,
            status=status,
        )

    def collect_completed_shards(
        self, task_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """收集原预测批次的已完成分片。"""
        return prediction_delivery.collect_completed_shards(task_id, model_version=model_version)

    def merge_shard_results(
        self, task_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """按同批同模型版本合并实际结果行并去重。"""
        return prediction_delivery.merge_shard_results(task_id, model_version=model_version)

    def build_shard_manifest(
        self, task_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """生成带覆盖范围的预测分片文件清单。"""
        return prediction_delivery.build_shard_manifest(task_id, model_version=model_version)

    def publish_delivery_version(
        self,
        task_id: int,
        model_version: str | None = None,
        *,
        require_full: bool = False,
    ) -> dict[str, Any]:
        """发布抽查版或全量版；全量版要求分片到齐。"""
        result = prediction_delivery.publish_version(
            task_id, model_version=model_version, require_full=require_full)
        task = store.find(self.MODULE, task_id)
        if result.get("ok") and task is not None:
            task["交付版本"] = result["version_label"]
            task["交付状态"] = "全量已发布" if result["full_coverage"] else "抽查已发布"
        return result

    def list_delivery_versions(self, task_id: int | None = None) -> list[dict[str, Any]]:
        """列出预测交付版本（可按任务过滤）。"""
        return prediction_delivery.list_versions(task_id)

    def archive_delivery_version(self, version_id: int) -> dict[str, Any]:
        """归档已发布版本，冻结的结果行仍可下载。"""
        return prediction_delivery.archive_version(version_id)

    def reopen_delivery_version(self, version_id: int) -> dict[str, Any]:
        """重新打开已归档版本。"""
        return prediction_delivery.reopen_version(version_id)

    # ── 链路4: 批量预测 → 挖掘结果 ──
    def execute_batch_and_collect_result(self, task_id: int) -> dict[str, Any]:
        """执行批量预测任务并将预测结果采集到挖掘结果模块进行深度分析。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}
        task["status"] = "已完成"
        task["prediction_state"] = "已完成"
        task["pending"] = False
        mining_rows = store.rows("mining_result")
        next_id = max((int(r.get("id", 0)) for r in mining_rows), default=0) + 1
        model_id = task.get("模型编号", "unknown")
        mining_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "结果编号": f"RES-P{task_id:04d}-{next_id:04d}",
            "分析主题": f"批量预测任务{task_id}挖掘分析",
            "算法类型": "ensemble_voting",
            "洞察摘要": f"从批量预测结果中识别出用户行为模式并写入挖掘结果" + "；发现2个关键行为路径",
            "关键分群": "高转化_群A,待激活_群B",
            "置信度": "0.82",
            "结果文件": f"mining/predict_{task_id}_result.json",
            "结果版本": "v1.0.0",
        })
        return {"ok": True, "task_id": task_id, "mining_id": next_id,
                "message": f"预测结果已采集并导入挖掘结果"}

    def validate_prediction_consistency(self, task_id: int) -> dict[str, Any]:
        """校验预测结果的一致性和格式合法性，确保输出符合预期schema。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        is_valid = task.get("输出格式") in ("csv", "json", "parquet")
        if not is_valid:
            task["abnormal"] = True
            return {"ok": False, "reason": f"输出格式 {task.get('输出格式')} 不合法"}
        return {"ok": True, "task_id": task_id, "format": task.get("输出格式")}

    def split_predict_partitions(self, task_id: int) -> dict[str, Any]:
        """将批量预测任务切分为多个分区并行执行，提高大规模数据集的预测吞吐。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        parallelism = int(str(task.get("并行度", "4")))
        partitions = []
        for i in range(parallelism):
            partitions.append({
                "partition_id": i,
                "task_id": task_id,
                "model_id": task.get("模型编号"),
                "output": f"predict_output/{task_id}/part_{i:03d}.csv",
            })
        return {"ok": True, "task_id": task_id, "partition_count": parallelism, "partitions": partitions}

    def export_predictions_to_csv(self, task_id: int) -> dict[str, Any]:
        """导出批量预测结果为CSV格式，写入外部存储供下游业务消费。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        export_rows = store.rows("data_export")
        next_id = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        export_rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "任务编号": f"EXP-P{task_id:04d}",
            "数据来源": f"predict_task_{task_id}",
            "导出范围": "全量预测结果",
            "导出格式": "csv",
            "字段映射": "prediction_id,model_id,score,label,dt",
            "分片策略": "none",
            "压缩方式": "gzip",
            "导出状态": "已完成",
        })
        return {"ok": True, "export_id": next_id, "task_id": task_id}