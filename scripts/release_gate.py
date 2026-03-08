#!/usr/bin/env python3
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.evaluation import evaluate_golden_cases


def main() -> int:
    report_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("reports_agentic_small")
    threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 95.0
    edge_f1_threshold = float(sys.argv[3]) if len(sys.argv) > 3 else 0.9

    reports = sorted(report_dir.glob("*.report.json"))
    if not reports:
        print("No report files found.")
        return 2

    passed = 0
    for report in reports:
        payload = json.loads(report.read_text(encoding="utf-8"))
        if payload.get("validation_ok", False):
            passed += 1

    pass_rate = (passed / len(reports)) * 100.0
    print(f"validation_pass_rate={pass_rate:.2f}% ({passed}/{len(reports)})")
    eval_summary = evaluate_golden_cases(ROOT, ROOT / "tests" / "golden" / "cases")
    edge_f1 = float(eval_summary.get("edge_micro_f1", 0.0))
    print(f"golden_edge_micro_f1={edge_f1:.4f}")
    if pass_rate < threshold:
        return 1
    if edge_f1 < edge_f1_threshold:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

