import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover
    sqlglot = None
    exp = None


@dataclass
class CaseResult:
    case_name: str
    passed: bool
    missing: list[str]
    forbidden_found: list[str]
    edge_precision: float = 0.0
    edge_recall: float = 0.0
    edge_f1: float = 0.0
    edge_tp: int = 0
    edge_fp: int = 0
    edge_fn: int = 0


def _safe_name(text: str) -> str:
    return text.strip().strip("\"'").lower()


def _extract_insert_edges(stmt: Any) -> set[str]:
    if exp is None:
        return set()
    edges: set[str] = set()
    if not isinstance(stmt, exp.Insert):
        return edges
    target = _safe_name(stmt.this.sql(dialect="hive")) if stmt.this else ""
    if not target:
        return edges
    select_expr = stmt.find(exp.Select)
    if not isinstance(select_expr, exp.Select):
        return edges
    for projection in select_expr.expressions:
        target_col = _safe_name(projection.alias_or_name or projection.sql(dialect="hive"))
        source_cols = [
            _safe_name(col.sql(dialect="hive"))
            for col in projection.find_all(exp.Column)
        ]
        if not source_cols:
            source_cols = ["<derived>"]
        for src in source_cols:
            edges.add(f"{target}.{target_col}<-{src}")
    return edges


def _extract_update_edges(stmt: Any) -> set[str]:
    if exp is None:
        return set()
    edges: set[str] = set()
    if not isinstance(stmt, exp.Update):
        return edges
    target = _safe_name(stmt.this.sql(dialect="hive")) if stmt.this else ""
    if not target:
        return edges
    for assignment in stmt.expressions or []:
        left = assignment.this if isinstance(assignment, exp.EQ) else None
        right = assignment.expression if isinstance(assignment, exp.EQ) else None
        if left is None or right is None:
            continue
        target_col = _safe_name(left.sql(dialect="hive"))
        source_cols = [_safe_name(col.sql(dialect="hive")) for col in right.find_all(exp.Column)]
        if not source_cols:
            source_cols = ["<derived>"]
        for src in source_cols:
            edges.add(f"{target}.{target_col}<-{src}")
    return edges


def extract_lineage_edges(sql_text: str) -> set[str]:
    if not sql_text:
        return set()
    if sqlglot is None or exp is None:
        return set()
    edges: set[str] = set()
    try:
        statements = sqlglot.parse(sql_text, read="hive")
    except Exception:
        return edges
    for stmt in statements:
        edges.update(_extract_insert_edges(stmt))
        edges.update(_extract_update_edges(stmt))
    return edges


def evaluate_golden_cases(workspace_root: Path, cases_dir: Path) -> dict[str, object]:
    case_results: list[CaseResult] = []
    for case_path in sorted(cases_dir.glob("*.json")):
        case = json.loads(case_path.read_text(encoding="utf-8"))
        output_sql_path = workspace_root / case["output_sql"]
        sql_text = output_sql_path.read_text(encoding="utf-8") if output_sql_path.exists() else ""
        sql_lower = sql_text.lower()

        expected_contains = case.get("expected_contains", [])
        expected_not_contains = case.get("expected_not_contains", [])
        expected_edges = {str(x).lower() for x in case.get("expected_edges", [])}
        forbidden_edges = {str(x).lower() for x in case.get("forbidden_edges", [])}
        produced_edges = {x.lower() for x in extract_lineage_edges(sql_text)}

        missing = [item for item in expected_contains if item.lower() not in sql_lower]
        forbidden_found = [item for item in expected_not_contains if item.lower() in sql_lower]
        missing_edges = sorted(expected_edges - produced_edges)
        found_forbidden_edges = sorted(produced_edges & forbidden_edges)
        tp = len(expected_edges & produced_edges)
        fp = len(produced_edges - expected_edges) if expected_edges else 0
        fn = len(expected_edges - produced_edges)
        precision = (tp / (tp + fp)) if (tp + fp) else (1.0 if not expected_edges else 0.0)
        recall = (tp / (tp + fn)) if (tp + fn) else (1.0 if not expected_edges else 0.0)
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

        passed = not missing and not forbidden_found and not missing_edges and not found_forbidden_edges
        case_results.append(
            CaseResult(
                case_name=case_path.stem,
                passed=passed,
                missing=missing + missing_edges,
                forbidden_found=forbidden_found + found_forbidden_edges,
                edge_precision=round(precision, 4),
                edge_recall=round(recall, 4),
                edge_f1=round(f1, 4),
                edge_tp=tp,
                edge_fp=fp,
                edge_fn=fn,
            )
        )

    passed_count = sum(1 for r in case_results if r.passed)
    total = len(case_results)
    total_tp = sum(r.edge_tp for r in case_results)
    total_fp = sum(r.edge_fp for r in case_results)
    total_fn = sum(r.edge_fn for r in case_results)
    micro_precision = (total_tp / (total_tp + total_fp)) if (total_tp + total_fp) else 1.0
    micro_recall = (total_tp / (total_tp + total_fn)) if (total_tp + total_fn) else 1.0
    micro_f1 = (
        (2 * micro_precision * micro_recall / (micro_precision + micro_recall))
        if (micro_precision + micro_recall)
        else 0.0
    )
    return {
        "total_cases": total,
        "passed_cases": passed_count,
        "pass_rate": round((passed_count / total) * 100, 2) if total else 0.0,
        "edge_micro_precision": round(micro_precision, 4),
        "edge_micro_recall": round(micro_recall, 4),
        "edge_micro_f1": round(micro_f1, 4),
        "results": [
            {
                "case": r.case_name,
                "passed": r.passed,
                "missing": r.missing,
                "forbidden_found": r.forbidden_found,
                "edge_precision": r.edge_precision,
                "edge_recall": r.edge_recall,
                "edge_f1": r.edge_f1,
            }
            for r in case_results
        ],
    }

