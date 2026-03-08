import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class FileReport:
    file_path: str
    unit_count: int
    candidate_sql_count: int
    validation_ok: bool
    validation_errors: list[str] = field(default_factory=list)
    retries: int = 0
    statement_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


def write_file_report(report_dir: Path, stem: str, report: FileReport) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"{stem}.report.json"
    payload = {
        "file_path": report.file_path,
        "unit_count": report.unit_count,
        "candidate_sql_count": report.candidate_sql_count,
        "validation_ok": report.validation_ok,
        "validation_errors": report.validation_errors,
        "retries": report.retries,
        "statement_count": report.statement_count,
        "metadata": report.metadata,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path

