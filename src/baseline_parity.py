from dataclasses import dataclass, asdict
from pathlib import Path
import re
from typing import Any, Optional

from src.validator.sql_parser import split_sql_statements


@dataclass
class FileParityReport:
    file_name: str
    baseline_found: bool
    statement_count_match: bool
    statement_order_match: bool
    target_sequence_match: bool
    projection_policy_match: bool
    clause_shape_match: bool
    score: float
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _statement_kind(stmt: str) -> str:
    upper = stmt.strip().upper()
    if upper.startswith("WITH "):
        return "with_insert" if "INSERT INTO" in upper else "with"
    if upper.startswith("INSERT INTO"):
        return "insert"
    if upper.startswith("UPDATE "):
        return "update"
    if upper.startswith("MERGE INTO"):
        return "merge"
    return "other"


def _insert_target(stmt: str) -> Optional[str]:
    m = re.search(r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b", stmt, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip().lower()


def _select_star_policy(stmt: str) -> Optional[str]:
    if not re.search(r"\bINSERT\s+INTO\b", stmt, flags=re.IGNORECASE):
        return None
    if re.search(r"\bSELECT\s+\*\s+FROM\b", stmt, flags=re.IGNORECASE | re.DOTALL):
        return "star"
    return "explicit"


def _shape_signature(stmt: str) -> dict[str, int]:
    upper = stmt.upper()
    return {
        "has_with": 1 if upper.strip().startswith("WITH ") else 0,
        "union_all": len(re.findall(r"\bUNION\s+ALL\b", upper)),
        "where": len(re.findall(r"\bWHERE\b", upper)),
        "partition": len(re.findall(r"\bPARTITION\s*\(", upper)),
        "left_join_subquery": len(re.findall(r"\bLEFT\s+JOIN\s*\(", upper)),
    }


def parity_score(report: FileParityReport) -> float:
    checks = [
        report.statement_count_match,
        report.statement_order_match,
        report.target_sequence_match,
        report.projection_policy_match,
        report.clause_shape_match,
    ]
    return sum(1 for ok in checks if ok) / len(checks)


def all_parity_gates_pass(report: FileParityReport) -> bool:
    return (
        report.baseline_found
        and report.statement_count_match
        and report.statement_order_match
        and report.target_sequence_match
        and report.projection_policy_match
        and report.clause_shape_match
    )


def compare_sql_to_baseline(file_name: str, baseline_sql: str, generated_sql: str) -> FileParityReport:
    baseline_stmts = [s.strip() for s in split_sql_statements(baseline_sql) if s.strip()]
    generated_stmts = [s.strip() for s in split_sql_statements(generated_sql) if s.strip()]

    baseline_kinds = [_statement_kind(s) for s in baseline_stmts]
    generated_kinds = [_statement_kind(s) for s in generated_stmts]
    baseline_targets = [t for t in (_insert_target(s) for s in baseline_stmts) if t]
    generated_targets = [t for t in (_insert_target(s) for s in generated_stmts) if t]
    baseline_policy = [p for p in (_select_star_policy(s) for s in baseline_stmts) if p]
    generated_policy = [p for p in (_select_star_policy(s) for s in generated_stmts) if p]
    baseline_shape = [_shape_signature(s) for s in baseline_stmts]
    generated_shape = [_shape_signature(s) for s in generated_stmts]

    report = FileParityReport(
        file_name=file_name,
        baseline_found=True,
        statement_count_match=(len(baseline_stmts) == len(generated_stmts)),
        statement_order_match=(baseline_kinds == generated_kinds),
        target_sequence_match=(baseline_targets == generated_targets),
        projection_policy_match=(baseline_policy == generated_policy),
        clause_shape_match=(baseline_shape == generated_shape),
        score=0.0,
        details={
            "baseline_statement_count": len(baseline_stmts),
            "generated_statement_count": len(generated_stmts),
            "baseline_kinds": baseline_kinds,
            "generated_kinds": generated_kinds,
            "baseline_targets": baseline_targets,
            "generated_targets": generated_targets,
            "baseline_projection_policy": baseline_policy,
            "generated_projection_policy": generated_policy,
            "baseline_shape": baseline_shape,
            "generated_shape": generated_shape,
        },
    )
    report.score = parity_score(report)
    return report


def compare_sql_with_baseline_dir(file_stem: str, baseline_dir: Optional[Path], generated_sql: str) -> FileParityReport:
    file_name = f"{file_stem}.sql"
    if not baseline_dir:
        return FileParityReport(
            file_name=file_name,
            baseline_found=False,
            statement_count_match=False,
            statement_order_match=False,
            target_sequence_match=False,
            projection_policy_match=False,
            clause_shape_match=False,
            score=0.0,
            details={"reason": "baseline_dir_not_set"},
        )
    baseline_path = baseline_dir / file_name
    if not baseline_path.exists():
        return FileParityReport(
            file_name=file_name,
            baseline_found=False,
            statement_count_match=False,
            statement_order_match=False,
            target_sequence_match=False,
            projection_policy_match=False,
            clause_shape_match=False,
            score=0.0,
            details={"reason": f"baseline_file_not_found:{baseline_path}"},
        )
    baseline_sql = baseline_path.read_text(encoding="utf-8")
    return compare_sql_to_baseline(file_name, baseline_sql, generated_sql)

