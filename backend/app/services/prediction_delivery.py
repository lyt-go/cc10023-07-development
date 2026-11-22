"""批量预测分片交付：回执登记、分片清单合并、版本发布与归档。

批量预测任务本身只负责登记执行参数；真实结果行由各分片异步回执。
本模块把「已完成分片」收集成带覆盖范围的预测分片文件清单（manifest），
再按清单发布可下载的交付版本。版本一旦发布就冻结结果行与覆盖分区，
后续迟到的分片只会进入新版本，不会改写已发布/已归档版本。

关键约束：
- 同批同模型版本合并；不同模型版本各自成清单，不混行。
- 同一 prediction_id 在一个文件里只保留一次：分片内重复、重复回执、
  跨片重叠都按「分区顺序 + 片内行顺序」取第一条。
- 分片是否到齐用预计分区与实际完成分区核对；空结果行的分片只要回执
  状态为已完成，同样计入覆盖。
- 只有全量标记（预计分区全部到齐）的版本才允许全量复核导出；
  缺片版本仍可导出抽查。
"""
from __future__ import annotations

import copy
import csv
import io
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

# CSV 列：prediction_id、模型编号、分数、标签、预测日期
PREDICTION_FIELDS = ["prediction_id", "模型编号", "分数", "标签", "预测日期"]

RECEIPT_TABLE = "predict_shard_receipts"
RECEIPT_HISTORY_TABLE = "predict_shard_receipt_history"
PARTITION_TABLE = "predict_batch_partitions"
VERSION_TABLE = "prediction_delivery_versions"

COMPLETED_STATUSES = {"已完成", "完成", "completed", "complete", "success", "succeeded", "ok"}
FULL_SCOPES = {"full", "full_review", "full-recheck", "全量", "全量复核"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_index(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shard_sort_key(receipt: dict[str, Any]) -> tuple[int, str, int]:
    index = receipt.get("partition_index")
    return (
        int(index) if isinstance(index, int) else 1_000_000,
        str(receipt.get("shard_id", "")),
        int(receipt.get("id", 0)),
    )


class PredictionDeliveryService:
    """分片交付账本；由批量预测服务与数据导出服务共用。"""

    PREDICTION_FIELDS = PREDICTION_FIELDS

    # ── 预计分区 ────────────────────────────────────────────────
    def register_batch_partitions(
        self, batch_id: int, partitions: Any
    ) -> dict[str, Any]:
        """登记原预测批次的预计分区，作为「分片是否到齐」的核对基准。"""
        task = store.find("predict_batch", batch_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {batch_id} 不存在"}

        entries = self._normalize_partitions(partitions)
        if not entries:
            return {"ok": False, "message": "预计分区不能为空"}

        table = store.aux_rows(PARTITION_TABLE)
        replaced = [row for row in table if int(row.get("batch_id", 0)) == batch_id]
        table[:] = [row for row in table if int(row.get("batch_id", 0)) != batch_id]
        next_id = max((int(row.get("id", 0)) for row in table), default=0)
        created: list[dict[str, Any]] = []
        for position, entry in enumerate(entries):
            next_id += 1
            record = {
                "id": next_id,
                "batch_id": batch_id,
                "partition_index": entry["partition_index"],
                "shard_id": entry["shard_id"],
                "order": position,
                "expected": True,
            }
            table.append(record)
            created.append(record)

        task["分片总数"] = len(created)
        task["预计分区"] = ",".join(row["shard_id"] for row in created)
        return {
            "ok": True,
            "batch_id": batch_id,
            "partitions": created,
            "expected_count": len(created),
            "replaced_count": len(replaced),
            "message": f"预测任务 {batch_id} 已登记 {len(created)} 个预计分区",
        }

    def list_batch_partitions(self, batch_id: int) -> list[dict[str, Any]]:
        rows = [
            row for row in store.aux_rows(PARTITION_TABLE)
            if int(row.get("batch_id", 0)) == batch_id
        ]
        return sorted(
            rows,
            key=lambda row: (
                row.get("order", 0),
                row.get("partition_index") if isinstance(row.get("partition_index"), int) else 1_000_000,
                str(row.get("shard_id", "")),
            ),
        )

    @staticmethod
    def _normalize_partitions(partitions: Any) -> list[dict[str, Any]]:
        if isinstance(partitions, int):
            raw: list[Any] = list(range(partitions))
        elif isinstance(partitions, str):
            raw = [item.strip() for item in partitions.split(",") if item.strip()]
        else:
            raw = list(partitions or [])

        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for position, item in enumerate(raw):
            if isinstance(item, dict):
                shard_id = _text(
                    item.get("shard_id")
                    or item.get("分片编号")
                    or item.get("partition")
                    or item.get("name")
                )
                index = _as_index(
                    item.get("partition_index", item.get("分区序号", item.get("index")))
                )
            else:
                shard_id = _text(item)
                index = position
            if not shard_id:
                shard_id = str(index if index is not None else position)
            if index is None:
                index = position
            key = shard_id.casefold()
            if key in seen:
                continue
            seen.add(key)
            entries.append({"shard_id": shard_id, "partition_index": index})
        return entries

    # ── 分片回执 ────────────────────────────────────────────────
    def register_shard_receipt(
        self,
        batch_id: int,
        shard_id: str,
        rows: Iterable[dict[str, Any]] | None = None,
        *,
        model_version: str | None = None,
        partition_index: int | None = None,
        status: str = "已完成",
    ) -> dict[str, Any]:
        """登记一个分片的完成回执。

        同一分片重复回执按最后一次覆盖，不会把行重复累加；空结果行也落账，
        只要状态是已完成就计入实际完成分区。
        """
        task = store.find("predict_batch", batch_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {batch_id} 不存在"}

        shard_id = _text(shard_id)
        if not shard_id:
            return {"ok": False, "message": "分片编号不能为空"}

        plan_entry = next(
            (
                row for row in self.list_batch_partitions(batch_id)
                if str(row.get("shard_id", "")).casefold() == shard_id.casefold()
            ),
            None,
        )
        if partition_index is None and plan_entry is not None:
            partition_index = _as_index(plan_entry.get("partition_index"))

        version = _text(model_version) or self._default_model_version(task)
        normalized_rows, invalid = self._normalize_rows(
            rows or [], batch_id=batch_id, model_version=version, shard_id=shard_id,
        )
        completed = _text(status).casefold() in COMPLETED_STATUSES

        receipt = {
            "batch_id": batch_id,
            "model_version": version,
            "shard_id": shard_id,
            "partition_index": partition_index,
            "status": "已完成" if completed else _text(status) or "未完成",
            "rows": normalized_rows,
            "row_count": len(normalized_rows),
            "invalid_row_count": invalid,
            "received_at": _now(),
        }

        table = store.aux_rows(RECEIPT_TABLE)
        existing = next(
            (
                row for row in table
                if int(row.get("batch_id", 0)) == batch_id
                and _text(row.get("shard_id")).casefold() == shard_id.casefold()
                and _text(row.get("model_version")) == version
            ),
            None,
        )
        duplicated = existing is not None
        if existing is None:
            existing = {
                "id": max((int(row.get("id", 0)) for row in table), default=0) + 1,
                "batch_id": batch_id,
                "model_version": version,
                "shard_id": shard_id,
                "receipt_no": 0,
            }
            table.append(existing)
        else:
            # 重复回执：同一分片再次投递时按 prediction_id 合并，
            # 既不会把重复行累加，也不会丢掉后续补投的新行。
            receipt["rows"] = self._union_rows(existing.get("rows", []), normalized_rows)
            receipt["row_count"] = len(receipt["rows"])

        existing.update(receipt)
        existing["receipt_no"] = int(existing.get("receipt_no", 0)) + 1
        existing["partition_index"] = partition_index

        history = store.aux_rows(RECEIPT_HISTORY_TABLE)
        history.append({
            "id": max((int(row.get("id", 0)) for row in history), default=0) + 1,
            "receipt_id": existing["id"],
            "receipt_no": existing["receipt_no"],
            "batch_id": batch_id,
            "model_version": version,
            "shard_id": shard_id,
            "status": existing["status"],
            "row_count": len(normalized_rows),
            "received_at": existing["received_at"],
        })

        self._refresh_task_progress(batch_id)
        return {
            "ok": True,
            "receipt_id": existing["id"],
            "receipt_no": existing["receipt_no"],
            "duplicated": duplicated,
            "batch_id": batch_id,
            "shard_id": shard_id,
            "model_version": version,
            "status": existing["status"],
            "row_count": len(normalized_rows),
            "invalid_row_count": invalid,
            "message": (
                f"分片 {shard_id} 回执已覆盖更新（第 {existing['receipt_no']} 次回执）"
                if duplicated else f"分片 {shard_id} 回执已登记"
            ),
        }

    @staticmethod
    def _union_rows(
        previous: list[dict[str, Any]], incoming: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """合并重复回执的行：prediction_id 唯一，先到的行优先。"""
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in [*previous, *incoming]:
            prediction_id = _text(row.get("prediction_id"))
            if not prediction_id or prediction_id in seen:
                continue
            seen.add(prediction_id)
            merged.append(dict(row))
        return merged

    def _normalize_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        batch_id: int,
        model_version: str,
        shard_id: str,
    ) -> tuple[list[dict[str, str]], int]:
        batch = store.find("predict_batch", batch_id) or {}
        batch_model = self._resolve_model_id(batch)
        normalized: list[dict[str, str]] = []
        invalid = 0
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                invalid += 1
                continue
            prediction_id = _text(
                row.get("prediction_id")
                or row.get("预测ID")
                or row.get("预测编号")
                or row.get("预测id")
            )
            if not prediction_id:
                invalid += 1
                continue
            # 同一分片内的重复行先就地合并，保留首次出现的值。
            if prediction_id in seen:
                continue
            seen.add(prediction_id)
            normalized.append({
                "prediction_id": prediction_id,
                "model_id": _text(row.get("model_id") or row.get("模型编号")) or batch_model,
                "score": _text(row.get("score", row.get("分数", ""))),
                "label": _text(row.get("label", row.get("标签", ""))),
                "dt": _text(row.get("dt", row.get("预测日期", ""))),
                "model_version": _text(row.get("model_version")) or model_version,
                "source_shard_id": shard_id,
            })
        return normalized, invalid

    def _default_model_version(self, task: dict[str, Any]) -> str:
        model = self._resolve_registry_model(task)
        version = _text(model.get("模型版本")) if model else ""
        return version or _text(task.get("模型版本")) or "v1"

    @staticmethod
    def _resolve_registry_model(task: dict[str, Any]) -> dict[str, Any] | None:
        model_ref = _text(task.get("模型编号") or task.get("model_id"))
        if model_ref.isdigit():
            model = store.find("model_registry", int(model_ref))
            if model is not None:
                return model
        if model_ref:
            return next(
                (
                    row for row in store.rows("model_registry")
                    if _text(row.get("模型编号")) == model_ref
                    or str(row.get("id")) == model_ref
                ),
                None,
            )
        return None

    def _resolve_model_id(self, task: dict[str, Any]) -> str:
        """对外输出的模型编号：优先取注册中心里的模型编号。"""
        model = self._resolve_registry_model(task)
        if model is not None and _text(model.get("模型编号")):
            return _text(model.get("模型编号"))
        return _text(task.get("模型编号") or task.get("model_id"))

    # ── 分片清单 ────────────────────────────────────────────────
    def collect_completed_shards(
        self, batch_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """收集原预测批次的已完成分片（可按模型版本过滤）。"""
        manifest = self.build_shard_manifest(batch_id, model_version=model_version)
        if not manifest.get("ok"):
            return manifest
        return {
            "ok": True,
            "batch_id": batch_id,
            "model_version": manifest["model_version"],
            "shards": manifest["shards"],
            "shard_ids": manifest["shard_ids"],
            "completed_count": manifest["completed_count"],
        }

    def merge_shard_results(
        self, batch_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """按同批同模型版本合并已完成分片的实际结果行并去重。"""
        manifest = self.build_shard_manifest(batch_id, model_version=model_version)
        if not manifest.get("ok"):
            return manifest
        return {
            "ok": True,
            "batch_id": batch_id,
            "model_version": manifest["model_version"],
            "rows": manifest["rows"],
            "row_count": manifest["row_count"],
            "unique_prediction_id_count": manifest["unique_prediction_id_count"],
            "duplicate_row_count": manifest["duplicate_row_count"],
            "shard_ids": manifest["shard_ids"],
        }

    def build_shard_manifest(
        self, batch_id: int, model_version: str | None = None
    ) -> dict[str, Any]:
        """生成带覆盖范围的预测分片文件清单。"""
        task = store.find("predict_batch", batch_id)
        if task is None:
            return {"ok": False, "message": f"预测任务 {batch_id} 不存在"}

        receipts = [
            row for row in store.aux_rows(RECEIPT_TABLE)
            if int(row.get("batch_id", 0)) == batch_id
        ]
        version = self._resolve_model_version(task, receipts, model_version)
        # 同批同模型版本：只合并版本完全一致的已完成分片，其他版本列入排除项。
        version_receipts = [
            row for row in receipts
            if _text(row.get("model_version")) == version
        ]
        completed = [
            row for row in version_receipts
            if _text(row.get("status")) == "已完成"
        ]
        completed.sort(key=_shard_sort_key)
        mismatched = [
            row for row in receipts if _text(row.get("model_version")) != version
        ]

        plan = self.list_batch_partitions(batch_id)
        expected_labels = [str(row.get("shard_id")) for row in plan]
        covered_labels: list[str] = []
        covered_indexes: list[int | None] = []
        for receipt in completed:
            covered_labels.append(str(receipt.get("shard_id")))
            covered_indexes.append(
                receipt.get("partition_index")
                if isinstance(receipt.get("partition_index"), int) else None
            )

        missing_labels = self._missing_partitions(plan, completed)
        complete = bool(plan) and not missing_labels
        if not plan:
            complete = False

        merged_rows, duplicate_count = self._merge_rows(completed)
        coverage_range = self._format_coverage(plan, completed)

        shards = [
            {
                "receipt_id": receipt.get("id"),
                "shard_id": str(receipt.get("shard_id")),
                "partition_index": receipt.get("partition_index"),
                "status": receipt.get("status"),
                "row_count": int(receipt.get("row_count", 0)),
                "receipt_no": int(receipt.get("receipt_no", 1)),
                "received_at": receipt.get("received_at"),
                "file": f"predict_output/{batch_id}/part_{receipt.get('shard_id')}.csv",
            }
            for receipt in completed
        ]

        return {
            "ok": True,
            "batch_id": batch_id,
            "task_no": _text(task.get("任务编号")),
            "model_id": self._resolve_model_id(task),
            "model_version": version,
            "available_model_versions": sorted({
                _text(row.get("model_version")) for row in receipts if _text(row.get("model_version"))
            }),
            "excluded_model_versions": sorted({
                _text(row.get("model_version")) for row in mismatched if _text(row.get("model_version"))
            }),
            "expected_partitions": expected_labels,
            "covered_partitions": covered_labels,
            "covered_partition_indexes": covered_indexes,
            "missing_partitions": missing_labels,
            "coverage_range": coverage_range,
            "complete": complete,
            "full_coverage": complete,
            "expected_count": len(plan),
            "completed_count": len(completed),
            "shards": shards,
            "shard_ids": [shard["shard_id"] for shard in shards],
            "rows": merged_rows,
            "row_count": len(merged_rows),
            "unique_prediction_id_count": len(merged_rows),
            "duplicate_row_count": duplicate_count,
            "invalid_row_count": sum(int(row.get("invalid_row_count", 0)) for row in version_receipts),
        }

    def _resolve_model_version(
        self,
        task: dict[str, Any],
        receipts: list[dict[str, Any]],
        requested: str | None,
    ) -> str:
        requested = _text(requested)
        if requested:
            return requested
        default = self._default_model_version(task)
        versions = sorted({
            _text(row.get("model_version")) for row in receipts if _text(row.get("model_version"))
        })
        if default in versions:
            return default
        if len(versions) == 1:
            return versions[0]
        return default or "v1"

    @staticmethod
    def _missing_partitions(
        plan: list[dict[str, Any]], completed: list[dict[str, Any]]
    ) -> list[str]:
        missing: list[str] = []
        for entry in plan:
            shard_id = _text(entry.get("shard_id")).casefold()
            index = entry.get("partition_index")
            hit = any(
                _text(receipt.get("shard_id")).casefold() == shard_id
                or (
                    isinstance(index, int)
                    and isinstance(receipt.get("partition_index"), int)
                    and receipt.get("partition_index") == index
                )
                for receipt in completed
            )
            if not hit:
                missing.append(_text(entry.get("shard_id")))
        return missing

    @staticmethod
    def _format_coverage(
        plan: list[dict[str, Any]], completed: list[dict[str, Any]]
    ) -> str:
        """覆盖范围：在预计分区顺序上连续时用「首片-末片」，否则逐片列出。"""
        if not completed:
            return ""

        positions: dict[str, int] = {}
        indexes: dict[str, int] = {}
        for position, entry in enumerate(plan):
            shard = _text(entry.get("shard_id")).casefold()
            if shard:
                positions[shard] = position
            index = entry.get("partition_index")
            if isinstance(index, int):
                indexes[f"#{index}"] = position

        covered_positions: list[int] = []
        unknown: list[str] = []
        for receipt in completed:
            shard = _text(receipt.get("shard_id")).casefold()
            position = positions.get(shard)
            index = receipt.get("partition_index")
            if position is None and isinstance(index, int):
                position = indexes.get(f"#{index}")
            if position is None:
                unknown.append(_text(receipt.get("shard_id")))
            else:
                covered_positions.append(position)

        if unknown or len(covered_positions) != len(completed):
            labels = sorted(str(receipt.get("shard_id")) for receipt in completed)
            return ",".join(labels)

        ordered = sorted(set(covered_positions))
        contiguous = ordered == list(range(ordered[0], ordered[-1] + 1))
        if not contiguous:
            labels = [
                _text(plan[position].get("shard_id"))
                for position in ordered
            ]
            return ",".join(labels)

        first = _text(plan[ordered[0]].get("shard_id"))
        last = _text(plan[ordered[-1]].get("shard_id"))
        return first if first == last else f"{first}-{last}"

    @staticmethod
    def _merge_rows(
        completed: list[dict[str, Any]]
    ) -> tuple[list[dict[str, str]], int]:
        """按分区顺序合并，prediction_id 取首次出现，处理跨片重叠。"""
        merged: list[dict[str, str]] = []
        seen: set[str] = set()
        duplicate = 0
        for receipt in sorted(completed, key=_shard_sort_key):
            for row in receipt.get("rows", []):
                prediction_id = _text(row.get("prediction_id"))
                if not prediction_id:
                    continue
                if prediction_id in seen:
                    duplicate += 1
                    continue
                seen.add(prediction_id)
                merged.append(dict(row))
        return merged, duplicate

    def _refresh_task_progress(self, batch_id: int) -> None:
        task = store.find("predict_batch", batch_id)
        if task is None:
            return
        manifest = self.build_shard_manifest(batch_id)
        if not manifest.get("ok"):
            return
        task["模型版本"] = manifest["model_version"]
        task["分片总数"] = manifest["expected_count"]
        task["已收分片"] = manifest["completed_count"]
        task["缺失分片"] = ",".join(manifest["missing_partitions"])
        task["覆盖范围"] = manifest["coverage_range"]
        task["结果校验"] = "全量可复核" if manifest["complete"] else "缺片仅可抽查"
        if not _text(task.get("预测状态")) or str(task.get("预测状态")).startswith("批量预测任务样例"):
            task["预测状态"] = "分片齐全" if manifest["complete"] else "分片接收中"

    # ── 版本发布 / 归档 / 重新打开 ─────────────────────────────
    def publish_version(
        self,
        batch_id: int,
        model_version: str | None = None,
        *,
        require_full: bool = False,
    ) -> dict[str, Any]:
        """按当前分片清单发布一个冻结版本；缺片时是抽查版。"""
        manifest = self.build_shard_manifest(batch_id, model_version=model_version)
        if not manifest.get("ok"):
            return manifest
        if not manifest["completed_count"]:
            return {"ok": False, "message": f"预测任务 {batch_id} 尚无已完成分片，无法发布版本"}
        if require_full and not manifest["complete"]:
            return {
                "ok": False,
                "message": (
                    "分片未到齐，不能发布全量版；缺失分区："
                    + (",".join(manifest["missing_partitions"]) or "无预计分区")
                ),
                "missing_partitions": manifest["missing_partitions"],
            }

        table = store.aux_rows(VERSION_TABLE)
        siblings = [row for row in table if int(row.get("batch_id", 0)) == batch_id]
        version_no = max((int(row.get("version_no", 0)) for row in siblings), default=0) + 1
        version = {
            "id": max((int(row.get("id", 0)) for row in table), default=0) + 1,
            "batch_id": batch_id,
            "task_no": manifest["task_no"],
            "version_no": version_no,
            "version_label": f"v{version_no}",
            "status": "已发布",
            "full_coverage": manifest["complete"],
            "model_id": manifest["model_id"],
            "model_version": manifest["model_version"],
            "expected_partitions": list(manifest["expected_partitions"]),
            "covered_partitions": list(manifest["covered_partitions"]),
            "missing_partitions": list(manifest["missing_partitions"]),
            "coverage_range": manifest["coverage_range"],
            "shard_ids": list(manifest["shard_ids"]),
            "shards": copy.deepcopy(manifest["shards"]),
            "rows": copy.deepcopy(manifest["rows"]),
            "row_count": manifest["row_count"],
            "duplicate_row_count": manifest["duplicate_row_count"],
            "created_at": _now(),
            "archived_at": None,
            "reopened_at": None,
        }
        table.append(version)
        return {
            "ok": True,
            "version": copy.deepcopy(version),
            "version_id": version["id"],
            "version_label": version["version_label"],
            "full_coverage": version["full_coverage"],
            "mode": "全量版" if version["full_coverage"] else "抽查版",
            "message": (
                f"已发布全量版 {version['version_label']}（{version['row_count']} 行，"
                f"覆盖 {version['coverage_range'] or '无'}）"
                if version["full_coverage"] else
                f"已发布抽查版 {version['version_label']}（{version['row_count']} 行，"
                f"缺片 {','.join(version['missing_partitions']) or '无预计分区'}）"
            ),
        }

    def list_versions(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(row) for row in store.aux_rows(VERSION_TABLE)
            if batch_id is None or int(row.get("batch_id", 0)) == batch_id
        ]
        return sorted(rows, key=lambda row: (int(row.get("batch_id", 0)), int(row.get("version_no", 0))))

    def get_version(self, version_id: int) -> dict[str, Any] | None:
        row = store.aux_find(VERSION_TABLE, version_id)
        return copy.deepcopy(row) if row is not None else None

    def archive_version(self, version_id: int) -> dict[str, Any]:
        """归档已发布版本；归档后结果行和覆盖分区保持不变，仍可下载。"""
        version = store.aux_find(VERSION_TABLE, version_id)
        if version is None:
            return {"ok": False, "message": f"预测版本 {version_id} 不存在"}
        if version.get("status") == "已归档":
            return {"ok": True, "version": copy.deepcopy(version), "message": "版本已处于归档状态"}
        version["status"] = "已归档"
        version["archived_at"] = _now()
        return {"ok": True, "version": copy.deepcopy(version), "message": f"版本 {version.get('version_label')} 已归档"}

    def reopen_version(self, version_id: int) -> dict[str, Any]:
        """重新打开已归档版本；不会重新合并迟到的分片。"""
        version = store.aux_find(VERSION_TABLE, version_id)
        if version is None:
            return {"ok": False, "message": f"预测版本 {version_id} 不存在"}
        if version.get("status") != "已归档":
            return {"ok": False, "message": "只有已归档版本可以重新打开"}
        version["status"] = "已发布"
        version["reopened_at"] = _now()
        return {"ok": True, "version": copy.deepcopy(version), "message": f"版本 {version.get('version_label')} 已重新打开"}

    # ── CSV 生成 ────────────────────────────────────────────────
    def render_version_csv(
        self, version_id: int, scope: str = "spot"
    ) -> dict[str, Any]:
        """从冻结版本生成 CSV；全量复核必须有全量标记。"""
        version = store.aux_find(VERSION_TABLE, version_id)
        if version is None:
            return {"ok": False, "message": f"预测版本 {version_id} 不存在"}

        normalized_scope = self._normalize_scope(scope)
        if normalized_scope == "full" and not version.get("full_coverage"):
            return {
                "ok": False,
                "message": (
                    f"版本 {version.get('version_label')} 缺少分区 "
                    f"{','.join(version.get('missing_partitions', [])) or '未知'}，"
                    "仅可抽查，不能用于全量复核"
                ),
                "missing_partitions": list(version.get("missing_partitions", [])),
            }

        # 版本 rows 已冻结且去过重；这里再做一次防御性去重，保证同文件 prediction_id 唯一。
        rows: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in version.get("rows", []):
            prediction_id = _text(row.get("prediction_id"))
            if not prediction_id or prediction_id in seen:
                continue
            seen.add(prediction_id)
            rows.append(row)

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(PREDICTION_FIELDS)
        for row in rows:
            writer.writerow([
                _text(row.get("prediction_id")),
                _text(row.get("model_id")),
                _text(row.get("score")),
                _text(row.get("label")),
                _text(row.get("dt")),
            ])

        version_label = _text(version.get("version_label")) or f"v{version.get('version_no')}"
        suffix = "full" if normalized_scope == "full" else "spot"
        filename = f"prediction_batch{version.get('batch_id')}_{version_label}_{suffix}.csv"
        return {
            "ok": True,
            "version_id": int(version["id"]),
            "version_label": version_label,
            "batch_id": int(version["batch_id"]),
            "model_id": _text(version.get("model_id")),
            "model_version": _text(version.get("model_version")),
            "full_coverage": bool(version.get("full_coverage")),
            "status": version.get("status"),
            "scope": normalized_scope,
            "shard_ids": list(version.get("shard_ids", [])),
            "covered_partitions": list(version.get("covered_partitions", [])),
            "expected_partitions": list(version.get("expected_partitions", [])),
            "missing_partitions": list(version.get("missing_partitions", [])),
            "coverage_range": _text(version.get("coverage_range")),
            "row_count": len(rows),
            "duplicate_row_count": int(version.get("duplicate_row_count", 0)),
            "filename": filename,
            "content": buffer.getvalue(),
        }

    @staticmethod
    def _normalize_scope(scope: Any) -> str:
        return "full" if _text(scope).casefold() in FULL_SCOPES else "spot"


prediction_delivery = PredictionDeliveryService()
