from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional


@dataclass
class RuntimeConfig:
    python_base_url: str
    python_model: str
    base_url: str
    sql_repair_base_url: Optional[str]
    api_key: str
    model: str
    sql_repair_model: Optional[str]
    temperature: float
    request_timeout: int
    chunk_max_tokens: int
    refine_max_tokens: int
    input_dir: Path
    output_dir: Path
    prompt_file: Path
    report_dir: Path
    file: Optional[Path]
    chunk_lines: int
    overlap_lines: int
    max_validation_retries: int
    max_model_retries: int
    max_file_bytes: int
    max_batch_files: int
    strict_validation: bool
    prompt_compliance_pass: bool
    compliance_no_where: bool
    compliance_profile: str
    dialect: str
    baseline_sql_dir: Optional[Path]
    dry_run: bool


def build_runtime_config(args: object) -> RuntimeConfig:
    dialect = str(getattr(args, "dialect", os.getenv("SQL_DIALECT", "tsql"))).strip().lower()
    if dialect != "tsql":
        raise SystemExit(f"Unsupported SQL dialect '{dialect}'. Only 'tsql' is supported.")
    profile = str(getattr(args, "compliance_profile", os.getenv("PROMPT_COMPLIANCE_PROFILE", "strict"))).strip().lower()
    if profile not in {"strict", "baseline_parity", "visual_parity", "baseline_sql_parity"}:
        profile = "strict"
    allow_where = bool(getattr(args, "allow_where", False)) or (
        profile in {"baseline_parity", "visual_parity", "baseline_sql_parity"}
    )
    baseline_sql_dir_raw = str(
        getattr(args, "baseline_sql_dir", os.getenv("BASELINE_SQL_DIR", "~/Downloads/SQL"))
    ).strip()
    baseline_sql_dir = Path(baseline_sql_dir_raw).expanduser().resolve() if baseline_sql_dir_raw else None
    sql_base_url = os.getenv("LOCAL_LLM_BASE_URL", "http://127.0.0.1:8000/v1")
    sql_model = os.getenv("LOCAL_LLM_MODEL", "ibm-granite/granite-3.1-3b-a800m-instruct")
    python_base_url = (getattr(args, "python_base_url", "") or os.getenv("LOCAL_LLM_PYTHON_BASE_URL", "")).strip()
    python_model = (getattr(args, "python_model", "") or os.getenv("LOCAL_LLM_PYTHON_MODEL", "")).strip()
    return RuntimeConfig(
        python_base_url=python_base_url or sql_base_url,
        python_model=python_model or sql_model,
        base_url=sql_base_url,
        sql_repair_base_url=(args.sql_repair_base_url or os.getenv("LOCAL_LLM_SQL_REPAIR_BASE_URL", "")).strip() or None,
        api_key=os.getenv("LOCAL_LLM_API_KEY", ""),
        model=sql_model,
        sql_repair_model=(args.sql_repair_model or os.getenv("LOCAL_LLM_SQL_REPAIR_MODEL", "")).strip() or None,
        temperature=args.temperature,
        request_timeout=args.request_timeout,
        chunk_max_tokens=args.chunk_max_tokens,
        refine_max_tokens=args.refine_max_tokens,
        input_dir=Path(args.input_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        prompt_file=Path(args.prompt_file).resolve(),
        report_dir=Path(args.report_dir).resolve(),
        file=Path(args.file).resolve() if args.file else None,
        chunk_lines=args.chunk_lines,
        overlap_lines=args.overlap_lines,
        max_validation_retries=args.max_validation_retries,
        max_model_retries=int(os.getenv("LOCAL_LLM_MAX_MODEL_RETRIES", "3")),
        max_file_bytes=int(os.getenv("PARSER_MAX_FILE_BYTES", "1048576")),
        max_batch_files=int(os.getenv("PARSER_MAX_BATCH_FILES", "50")),
        strict_validation=bool(getattr(args, "strict_validation", False)),
        prompt_compliance_pass=bool(getattr(args, "prompt_compliance_pass", True)),
        compliance_no_where=not allow_where,
        compliance_profile=profile,
        dialect=dialect,
        baseline_sql_dir=baseline_sql_dir,
        dry_run=args.dry_run,
    )

