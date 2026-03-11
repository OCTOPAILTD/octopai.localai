#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from pathlib import Path
from urllib import request, error


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def sanitize_sql_output(text: str) -> str:
    cleaned = text.replace("<|im_end|>", "").strip()
    lines = cleaned.splitlines()
    filtered: list[str] = []
    for line in lines:
        stripped = line.strip().lower()
        if stripped in {"```sql", "```"}:
            continue
        filtered.append(line)
    return "\n".join(filtered).strip()


def list_python_files(input_dir: Path) -> list[Path]:
    # Strictly include only .py files from the target directory.
    return sorted([p for p in input_dir.glob("*.py") if p.is_file()])


def call_chat_completion(
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    code: str,
    temperature: float,
    max_tokens: int,
    request_timeout: int,
) -> str:
    endpoint = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": code},
        ],
    }
    data = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = request.Request(endpoint, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=request_timeout) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from model endpoint: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Could not reach model endpoint: {exc}") from exc

    try:
        return response_data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected response format: {response_data}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local Qwen model on Python files only.")
    parser.add_argument(
        "--mode",
        choices=["monolithic", "agentic"],
        default=os.getenv("RUN_PARSER_MODE", "monolithic"),
        help="Run mode: monolithic (single pass) or agentic (chunked/refined).",
    )
    parser.add_argument("--input-dir", default="python_files", help="Directory containing source files.")
    parser.add_argument("--file", default=None, help="Single Python file to process.")
    parser.add_argument("--prompt-file", default="prompt.txt", help="System prompt file.")
    parser.add_argument("--output-dir", default="sql_outputs", help="Directory for SQL outputs.")
    parser.add_argument(
        "--python-model",
        default="",
        help="Agentic only: optional model override for Python structuring/chunk stage.",
    )
    parser.add_argument(
        "--python-base-url",
        default="",
        help="Agentic only: optional endpoint override for Python structuring/chunk stage.",
    )
    parser.add_argument(
        "--sql-repair-model",
        default="",
        help="Agentic only: optional model override for SQL refine/repair stages.",
    )
    parser.add_argument(
        "--sql-repair-base-url",
        default="",
        help="Agentic only: optional endpoint override for SQL refine/repair stages.",
    )
    parser.add_argument("--temperature", type=float, default=0.0, help="Model temperature.")
    parser.add_argument(
        "--dialect",
        default=os.getenv("SQL_DIALECT", "tsql"),
        help="SQL dialect lock. Only 'tsql' is supported.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=int(os.getenv("LOCAL_LLM_MAX_TOKENS", "4096")),
        help="Maximum output tokens per file.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=int(os.getenv("LOCAL_LLM_REQUEST_TIMEOUT", "1200")),
        help="HTTP timeout in seconds for each model request.",
    )
    parser.add_argument(
        "--report-dir",
        default="reports_agentic",
        help="Directory for agentic JSON execution reports.",
    )
    parser.add_argument(
        "--max-validation-retries",
        type=int,
        default=int(os.getenv("LOCAL_LLM_MAX_VALIDATION_RETRIES", "1")),
        help="Agentic validation repair retries.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Agentic: run pipeline and reports without writing SQL output.",
    )
    parser.add_argument(
        "--strict-validation",
        action="store_true",
        help="Agentic: fail run when a file does not pass validation.",
    )
    parser.add_argument(
        "--no-prompt-compliance-pass",
        action="store_true",
        help="Agentic: disable prompt compliance post-processing pass.",
    )
    parser.add_argument(
        "--allow-where",
        action="store_true",
        help="Agentic: allow WHERE clauses in output SQL.",
    )
    parser.add_argument(
        "--compliance-profile",
        choices=["strict", "baseline_parity", "visual_parity", "baseline_sql_parity"],
        default=os.getenv("PROMPT_COMPLIANCE_PROFILE", "strict"),
        help="Agentic: prompt compliance profile.",
    )
    parser.add_argument(
        "--baseline-sql-dir",
        default=os.getenv("BASELINE_SQL_DIR", "~/Downloads/SQL"),
        help="Agentic: directory containing baseline SQL files.",
    )
    args = parser.parse_args()

    if args.mode == "agentic":
        agentic_script = Path(__file__).resolve().with_name("run_parser_agentic.py")
        if not agentic_script.exists():
            raise SystemExit(f"Agentic runner not found: {agentic_script}")

        cmd = [
            "python3",
            str(agentic_script),
            "--input-dir",
            args.input_dir,
            "--prompt-file",
            args.prompt_file,
            "--output-dir",
            args.output_dir,
            "--python-model",
            args.python_model,
            "--python-base-url",
            args.python_base_url,
            "--sql-repair-model",
            args.sql_repair_model,
            "--sql-repair-base-url",
            args.sql_repair_base_url,
            "--temperature",
            str(args.temperature),
            "--dialect",
            args.dialect,
            "--request-timeout",
            str(args.request_timeout),
            "--chunk-max-tokens",
            str(max(256, args.max_tokens // 4)),
            "--refine-max-tokens",
            str(args.max_tokens),
            "--report-dir",
            args.report_dir,
            "--max-validation-retries",
            str(args.max_validation_retries),
        ]
        if args.file:
            cmd.extend(["--file", args.file])
        if args.dry_run:
            cmd.append("--dry-run")
        if args.strict_validation:
            cmd.append("--strict-validation")
        if args.no_prompt_compliance_pass:
            cmd.append("--no-prompt-compliance-pass")
        if args.allow_where:
            cmd.append("--allow-where")
        if args.compliance_profile:
            cmd.extend(["--compliance-profile", args.compliance_profile])
        if args.baseline_sql_dir:
            cmd.extend(["--baseline-sql-dir", args.baseline_sql_dir])

        print("Delegating to agentic mode...")
        result = subprocess.run(cmd, check=False)
        return result.returncode

    input_dir = Path(args.input_dir).resolve()
    if str(args.dialect).strip().lower() != "tsql":
        raise SystemExit(f"Unsupported SQL dialect '{args.dialect}'. Only 'tsql' is supported.")

    prompt_file = Path(args.prompt_file).resolve()
    output_dir = Path(args.output_dir).resolve()

    if args.file is None and (not input_dir.exists() or not input_dir.is_dir()):
        raise SystemExit(f"Input directory does not exist: {input_dir}")
    if not prompt_file.exists() or not prompt_file.is_file():
        raise SystemExit(f"Prompt file does not exist: {prompt_file}")

    if args.file:
        file_path = Path(args.file).resolve()
        if not file_path.exists() or not file_path.is_file():
            raise SystemExit(f"File does not exist: {file_path}")
        if file_path.suffix.lower() != ".py":
            raise SystemExit(f"Only .py files are supported: {file_path}")
        python_files = [file_path]
    else:
        python_files = list_python_files(input_dir)
    if not python_files:
        print(f"No .py files found in {input_dir}")
        return 0

    system_prompt = read_text_file(prompt_file)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = os.getenv("LOCAL_LLM_BASE_URL", "http://127.0.0.1:8000/v1")
    api_key = os.getenv("LOCAL_LLM_API_KEY", "")
    model = os.getenv("LOCAL_LLM_MODEL", "ibm-granite/granite-3.1-3b-a800m-instruct")

    print(f"Using model: {model}")
    print(f"Endpoint: {base_url}")
    print(f"Max tokens per file: {args.max_tokens}")
    print(f"Request timeout (s): {args.request_timeout}")
    print(f"Found {len(python_files)} Python file(s).")

    for py_file in python_files:
        code = read_text_file(py_file)
        sql_raw = call_chat_completion(
            base_url=base_url,
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            code=code,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            request_timeout=args.request_timeout,
        )
        sql = sanitize_sql_output(sql_raw)

        out_file = output_dir / f"{py_file.stem}.sql"
        out_file.write_text(sql + "\n", encoding="utf-8")
        print(f"Wrote: {out_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

