#!/usr/bin/env python3
import argparse
import os

from src.config import build_runtime_config
from src.runner import run_agentic_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Agentic Python->SQL lineage runner.")
    parser.add_argument("--input-dir", default="python_files", help="Directory containing source files.")
    parser.add_argument("--file", default=None, help="Single Python file to process.")
    parser.add_argument("--prompt-file", default="prompt_short.txt", help="Base SQL generation prompt file.")
    parser.add_argument("--output-dir", default="sql_outputs_agentic", help="Directory for SQL outputs.")
    parser.add_argument(
        "--python-model",
        default="",
        help="Optional model override for Python structuring/chunk understanding stage.",
    )
    parser.add_argument(
        "--python-base-url",
        default="",
        help="Optional endpoint override for Python structuring/chunk understanding stage.",
    )
    parser.add_argument(
        "--sql-repair-model",
        default="",
        help="Optional model override for SQL refine/repair stages.",
    )
    parser.add_argument(
        "--sql-repair-base-url",
        default="",
        help="Optional endpoint override for SQL refine/repair stages.",
    )
    parser.add_argument("--temperature", type=float, default=0.0, help="Model temperature.")
    parser.add_argument(
        "--dialect",
        default=os.getenv("SQL_DIALECT", "tsql"),
        help="SQL dialect lock. Only 'tsql' is supported.",
    )
    parser.add_argument("--chunk-lines", type=int, default=140, help="Lines per chunk.")
    parser.add_argument("--overlap-lines", type=int, default=25, help="Overlapping lines between chunks.")
    parser.add_argument(
        "--chunk-max-tokens",
        type=int,
        default=int(os.getenv("LOCAL_LLM_CHUNK_MAX_TOKENS", "512")),
        help="Max output tokens per chunk call.",
    )
    parser.add_argument(
        "--refine-max-tokens",
        type=int,
        default=int(os.getenv("LOCAL_LLM_REFINE_MAX_TOKENS", "1536")),
        help="Max output tokens for final consolidation pass.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=int(os.getenv("LOCAL_LLM_REQUEST_TIMEOUT", "1800")),
        help="HTTP timeout in seconds for each model request.",
    )
    parser.add_argument(
        "--report-dir",
        default="reports_agentic",
        help="Directory for per-file JSON execution reports.",
    )
    parser.add_argument(
        "--max-validation-retries",
        type=int,
        default=int(os.getenv("LOCAL_LLM_MAX_VALIDATION_RETRIES", "1")),
        help="Number of validator-driven repair retries.",
    )
    parser.add_argument(
        "--strict-validation",
        action="store_true",
        help="Fail the run when a file does not pass validation.",
    )
    parser.add_argument(
        "--no-prompt-compliance-pass",
        action="store_false",
        dest="prompt_compliance_pass",
        help="Disable prompt compliance post-processing pass.",
    )
    parser.add_argument(
        "--allow-where",
        action="store_true",
        help="Allow WHERE clauses in output SQL (overrides strict prompt compliance rule).",
    )
    parser.add_argument(
        "--compliance-profile",
        choices=["strict", "baseline_parity", "visual_parity", "baseline_sql_parity"],
        default=os.getenv("PROMPT_COMPLIANCE_PROFILE", "strict"),
        help="Prompt compliance profile.",
    )
    parser.add_argument(
        "--baseline-sql-dir",
        default=os.getenv("BASELINE_SQL_DIR", "~/Downloads/SQL"),
        help="Directory containing baseline SQL files for parity scoring/lockdown.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute pipeline and write reports without writing SQL files.",
    )
    args = parser.parse_args()
    cfg = build_runtime_config(args)
    return run_agentic_pipeline(cfg)


if __name__ == "__main__":
    raise SystemExit(main())

