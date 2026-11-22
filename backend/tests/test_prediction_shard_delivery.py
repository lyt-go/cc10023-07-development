"""分片交付场景测试：三片批次日终抽查 → 归档 → 迟到分片 → 全量版复核。

覆盖点：
1. A、B 分片各含一条相同 prediction_id 的预测行，C 未到 → 抽查版；
2. 抽查版 CSV 逐行核对分数，覆盖分区为 0-1，重复行只出现一次；
3. 抽查版可归档，归档后仍能下载且内容与首次下载完全一致；
4. 缺片版本禁止全量复核导出；
5. 迟到的 C 到货并发布全量版后，全量 CSV 多出 C 的新行；
6. 下载全量版并按全量标记复核；抽查版与全量版互不影响。
"""
from __future__ import annotations

import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services import service_for


def assert_ok(result, msg=""):
    ok = result.get("ok") if isinstance(result, dict) else getattr(result, "ok", False)
    assert ok, f"{msg}: {result}"
    return result


def parse_csv(content: str) -> tuple[list[str], list[dict[str, str]]]:
    rows = list(csv.DictReader(io.StringIO(content)))
    header = list(rows[0].keys()) if rows else []
    return header, rows


def create_batch(model_id: int = 3) -> int:
    svc_predict = service_for("predict_batch")
    task, missing = svc_predict.create_entry({
        "任务编号": "PRED-SHARD-TEST",
        "模型编号": str(model_id),
        "数据切片": "daily_slice",
        "输出格式": "csv",
        "分片策略": "round_robin",
        "并行度": "3",
        "预测状态": "运行中",
        "结果校验": "auto_validate",
    })
    assert task is not None and not missing, f"创建预测任务失败：{missing}"
    return int(task["id"])


def test_shard_delivery_scenario():
    svc_predict = service_for("predict_batch")
    svc_export = service_for("data_export")
    task_id = create_batch()

    # ── 预计三片：A、B、C ──
    plan = assert_ok(svc_predict.register_batch_partitions(task_id, ["A", "B", "C"]),
                     "register_batch_partitions")
    assert plan["expected_count"] == 3, plan

    shared_row = {
        "prediction_id": "P0001", "score": "0.91", "label": "1", "dt": "2026-11-22",
    }
    # A、B 都含同一条内容相同的预测行；A 还含一条独立行。
    assert_ok(svc_predict.register_shard_receipt(
        task_id, "A",
        [dict(shared_row), {"prediction_id": "P0002", "score": "0.22", "label": "0", "dt": "2026-11-22"}],
    ), "receipt A")
    assert_ok(svc_predict.register_shard_receipt(
        task_id, "B", [dict(shared_row)],
    ), "receipt B")

    # 重复回执：A 再投一次，行不会翻倍。
    duplicate = assert_ok(svc_predict.register_shard_receipt(
        task_id, "A",
        [dict(shared_row), {"prediction_id": "P0002", "score": "0.22", "label": "0", "dt": "2026-11-22"}],
    ), "duplicate receipt A")
    assert duplicate["duplicated"] is True, duplicate

    manifest = assert_ok(svc_predict.build_shard_manifest(task_id), "manifest")
    assert manifest["expected_partitions"] == ["A", "B", "C"], manifest
    assert manifest["covered_partitions"] == ["A", "B"], manifest
    assert manifest["missing_partitions"] == ["C"], manifest
    assert manifest["coverage_range"] == "A-B", manifest
    assert manifest["row_count"] == 2, manifest
    assert manifest["duplicate_row_count"] >= 1, manifest
    assert manifest["complete"] is False, "C 未到，不应是全量"

    # ── 发布抽查版并下载逐行核对 ──
    spot = assert_ok(svc_predict.publish_delivery_version(task_id), "publish spot")
    assert spot["full_coverage"] is False, spot
    spot_version_id = spot["version_id"]

    spot_export = assert_ok(
        svc_export.create_export_from_manifest(task_id, version_id=spot_version_id, scope="spot"),
        "export spot",
    )
    assert spot_export["export"]["全量标记"] is False, spot_export

    spot_csv = assert_ok(svc_export.export_manifest_to_csv(spot_export["export_id"]),
                         "spot csv")
    header, rows = parse_csv(spot_csv["content"])
    assert header == ["prediction_id", "模型编号", "分数", "标签", "预测日期"], header
    assert spot_csv["coverage_range"] == "A-B", spot_csv
    assert spot_csv["full_coverage"] is False, spot_csv
    assert sorted(row["prediction_id"] for row in rows) == ["P0001", "P0002"], rows
    by_id = {row["prediction_id"]: row for row in rows}
    assert by_id["P0001"]["分数"] == "0.91", by_id
    assert by_id["P0002"]["分数"] == "0.22", by_id
    assert len({row["prediction_id"] for row in rows}) == len(rows), "同一文件 prediction_id 必须唯一"
    assert all(row["模型编号"] == "MODE-0003" for row in rows), rows
    assert all(row["预测日期"] == "2026-11-22" for row in rows), rows

    # 缺片版本不能做全量复核。
    blocked = svc_export.export_manifest_to_csv(spot_export["export_id"], scope="full")
    assert blocked.get("ok") is False, blocked

    # ── 归档抽查版；归档后重新下载内容一致 ──
    archived = assert_ok(svc_predict.archive_delivery_version(spot_version_id), "archive spot")
    assert archived["version"]["status"] == "已归档", archived

    archived_export = assert_ok(
        svc_export.create_export_from_manifest(task_id, version_id=spot_version_id, scope="spot"),
        "export archived spot",
    )
    archived_csv = assert_ok(
        svc_export.export_manifest_to_csv(archived_export["export_id"]), "archived csv")
    assert archived_csv["content"] == spot_csv["content"], "归档抽查版内容必须与首次下载一致"
    assert archived_csv["covered_partitions"] == spot_csv["covered_partitions"], archived_csv
    assert archived_csv["coverage_range"] == "A-B", archived_csv

    # ── 迟到的 C 到货：含一条新行，并重复 P0001 ──
    late = assert_ok(svc_predict.register_shard_receipt(
        task_id, "C",
        [
            {"prediction_id": "P0003", "score": "0.77", "label": "1", "dt": "2026-11-22"},
            dict(shared_row),
        ],
    ), "receipt C")
    assert late["model_version"] == manifest["model_version"], late

    full_manifest = assert_ok(svc_predict.build_shard_manifest(task_id), "full manifest")
    assert full_manifest["missing_partitions"] == [], full_manifest
    assert full_manifest["complete"] is True, full_manifest
    assert full_manifest["row_count"] == 3, full_manifest
    assert full_manifest["duplicate_row_count"] == 2, full_manifest

    full = assert_ok(svc_predict.publish_delivery_version(task_id, require_full=True), "publish full")
    assert full["full_coverage"] is True, full
    assert full["version_id"] != spot_version_id, "全量版必须是新版本"

    full_export = assert_ok(
        svc_export.create_export_from_manifest(task_id, version_id=full["version_id"], scope="full"),
        "export full",
    )
    assert full_export["export"]["全量标记"] is True, full_export
    full_csv = assert_ok(svc_export.export_manifest_to_csv(full_export["export_id"], scope="full"),
                         "full csv")
    _, full_rows = parse_csv(full_csv["content"])
    assert full_csv["coverage_range"] == "A-C", full_csv
    assert full_csv["full_coverage"] is True, full_csv
    assert sorted(row["prediction_id"] for row in full_rows) == ["P0001", "P0002", "P0003"], full_rows
    assert len({row["prediction_id"] for row in full_rows}) == len(full_rows), "全量文件也不能重复"

    # 抽查版仍保持原覆盖与内容，不因 C 到货而变化。
    reopened = assert_ok(svc_predict.reopen_delivery_version(spot_version_id), "reopen spot")
    assert reopened["version"]["status"] == "已发布", reopened
    reexport = assert_ok(
        svc_export.create_export_from_manifest(task_id, version_id=spot_version_id, scope="spot"),
        "re-export archived spot",
    )
    reexport_csv = assert_ok(svc_export.export_manifest_to_csv(reexport["export_id"]))
    assert reexport_csv["content"] == spot_csv["content"], "重新打开后抽查版内容仍应与首次一致"
    assert "P0003" not in reexport_csv["content"], "抽查版不应包含迟到的 C 新行"


def test_empty_shard_receipt_counts_as_covered():
    """分片没有结果行时，回执仍要落实覆盖，否则全量版永远凑不齐。"""
    svc_predict = service_for("predict_batch")
    task_id = create_batch()
    assert_ok(svc_predict.register_batch_partitions(task_id, ["A", "B"]), "plan")
    assert_ok(svc_predict.register_shard_receipt(
        task_id, "A", [{"prediction_id": "E1", "score": "0.5", "label": "1", "dt": "2026-11-22"}],
    ), "A")
    assert_ok(svc_predict.register_shard_receipt(task_id, "B", [], status="已完成"), "empty B")

    manifest = assert_ok(svc_predict.build_shard_manifest(task_id), "manifest")
    assert manifest["missing_partitions"] == [], manifest
    assert manifest["complete"] is True, manifest
    assert manifest["completed_count"] == 2, manifest
    assert manifest["row_count"] == 1, manifest


def test_different_model_versions_are_not_merged():
    """同批不同模型版本不混行；只有请求的版本进入清单。"""
    svc_predict = service_for("predict_batch")
    task_id = create_batch()
    assert_ok(svc_predict.register_batch_partitions(task_id, ["A", "B"]), "plan")
    assert_ok(svc_predict.register_shard_receipt(
        task_id, "A", [{"prediction_id": "V1-1", "score": "0.1", "label": "0", "dt": "2026-11-22"}],
        model_version="model-v1"), "A v1")
    assert_ok(svc_predict.register_shard_receipt(
        task_id, "B", [{"prediction_id": "V2-1", "score": "0.9", "label": "1", "dt": "2026-11-22"}],
        model_version="model-v2"), "B v2")

    v1 = assert_ok(svc_predict.build_shard_manifest(task_id, model_version="model-v1"), "v1 manifest")
    assert [row["prediction_id"] for row in v1["rows"]] == ["V1-1"], v1
    assert v1["complete"] is False, v1
    assert v1["missing_partitions"] == ["B"], v1

    v2 = assert_ok(svc_predict.build_shard_manifest(task_id, model_version="model-v2"), "v2 manifest")
    assert [row["prediction_id"] for row in v2["rows"]] == ["V2-1"], v2
    assert v2["missing_partitions"] == ["A"], v2

    merged = assert_ok(svc_predict.merge_shard_results(task_id, model_version="model-v1"), "merge v1")
    assert all(row["model_version"] == "model-v1" for row in merged["rows"]), merged


if __name__ == "__main__":
    tests = [
        test_shard_delivery_scenario,
        test_empty_shard_receipt_counts_as_covered,
        test_different_model_versions_are_not_merged,
    ]
    passed = failed = 0
    for test in tests:
        try:
            test()
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    print(f"\n总测试: {len(tests)}, 通过: {passed}, 失败: {failed}")
    if failed:
        sys.exit(1)
