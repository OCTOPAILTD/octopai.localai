#!/usr/bin/env python3
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.evaluation import evaluate_golden_cases


def main() -> int:
    cases_dir = ROOT / "tests" / "golden" / "cases"
    min_edge_f1 = float(sys.argv[1]) if len(sys.argv) > 1 else 0.9
    summary = evaluate_golden_cases(ROOT, cases_dir)
    print(json.dumps(summary, indent=2))
    ok = summary["pass_rate"] == 100.0 and summary.get("edge_micro_f1", 0.0) >= min_edge_f1
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

