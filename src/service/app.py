import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
from threading import BoundedSemaphore

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from src.config import build_runtime_config
from src.errors import ParserServiceError
from src.logging_utils import get_logger
from src.metrics import MetricsRegistry
from src.runner import run_agentic_pipeline
from src.service.models import BatchParseRequest, BatchParseResponse, ParseRequest, ParseResponse


app = FastAPI(title="Python-to-SQL Parser Service", version="1.0.0")
logger = get_logger("parser.service")
metrics_registry = MetricsRegistry()
_max_concurrency = int(os.getenv("PARSER_MAX_CONCURRENCY", "2"))
semaphore = BoundedSemaphore(value=max(1, _max_concurrency))


def _build_cfg_for_request(req: ParseRequest, file_path: str) -> object:
    args = SimpleNamespace(
        input_dir=str(Path(file_path).parent),
        file=file_path,
        prompt_file=req.prompt_file,
        output_dir=req.output_dir,
        report_dir=req.report_dir,
        sql_repair_model=os.getenv("LOCAL_LLM_SQL_REPAIR_MODEL", ""),
        sql_repair_base_url=os.getenv("LOCAL_LLM_SQL_REPAIR_BASE_URL", ""),
        temperature=0.0,
        chunk_lines=140,
        overlap_lines=25,
        chunk_max_tokens=max(128, req.max_tokens // 4),
        refine_max_tokens=req.max_tokens,
        request_timeout=int(os.getenv("LOCAL_LLM_REQUEST_TIMEOUT", "1800")),
        max_validation_retries=int(os.getenv("LOCAL_LLM_MAX_VALIDATION_RETRIES", "1")),
        strict_validation=req.strict_validation,
        dialect=req.dialect,
        compliance_profile=req.compliance_profile,
        dry_run=False,
    )
    return build_runtime_config(args)


def _run_single(req: ParseRequest) -> ParseResponse:
    if not req.python_code and not req.file_path:
        raise HTTPException(status_code=400, detail="Either python_code or file_path is required.")
    if req.python_code and req.file_path:
        raise HTTPException(status_code=400, detail="Provide only one of python_code or file_path.")

    max_file_bytes = int(os.getenv("PARSER_MAX_FILE_BYTES", "1048576"))
    if req.python_code and len(req.python_code.encode("utf-8")) > max_file_bytes:
        raise HTTPException(status_code=413, detail="python_code exceeds max allowed bytes.")

    with semaphore:
        started = time.perf_counter()
        try:
            if req.python_code:
                with TemporaryDirectory(prefix="py2sql_") as tmp:
                    tmp_file = Path(tmp) / req.file_name
                    tmp_file.write_text(req.python_code, encoding="utf-8")
                    cfg = _build_cfg_for_request(req, str(tmp_file))
                    run_agentic_pipeline(cfg)
                    out_file = cfg.output_dir / f"{tmp_file.stem}.sql"
                    rep_file = cfg.report_dir / f"{tmp_file.stem}.report.json"
                    sql = out_file.read_text(encoding="utf-8") if out_file.exists() else ""
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    metrics_registry.record_request(file_count=1, latency_ms=elapsed_ms, failed=False, validation_failed=0)
                    return ParseResponse(sql=sql, report_path=str(rep_file), output_path=str(out_file))
            else:
                path = Path(req.file_path).resolve()
                if not path.exists():
                    raise HTTPException(status_code=404, detail=f"File not found: {path}")
                cfg = _build_cfg_for_request(req, str(path))
                run_agentic_pipeline(cfg)
                out_file = cfg.output_dir / f"{path.stem}.sql"
                rep_file = cfg.report_dir / f"{path.stem}.report.json"
                sql = out_file.read_text(encoding="utf-8") if out_file.exists() else ""
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                metrics_registry.record_request(file_count=1, latency_ms=elapsed_ms, failed=False, validation_failed=0)
                return ParseResponse(sql=sql, report_path=str(rep_file), output_path=str(out_file))
        except HTTPException:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            metrics_registry.record_request(file_count=1, latency_ms=elapsed_ms, failed=True, validation_failed=0)
            raise
        except ParserServiceError as exc:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            metrics_registry.record_request(file_count=1, latency_ms=elapsed_ms, failed=True, validation_failed=0)
            logger.error("parse_failed", extra={"extras": {"error": str(exc)}})
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            metrics_registry.record_request(file_count=1, latency_ms=elapsed_ms, failed=True, validation_failed=0)
            logger.error("unexpected_failure", extra={"extras": {"error": str(exc)}})
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> dict[str, int | float]:
    return metrics_registry.snapshot()


@app.post("/parse", response_model=ParseResponse)
def parse(request: ParseRequest) -> ParseResponse:
    return _run_single(request)


@app.post("/parse/file", response_model=ParseResponse)
async def parse_file(
    file: UploadFile = File(...),
    prompt_file: str = Form("prompt_short.txt"),
    output_dir: str = Form("sql_outputs_service"),
    report_dir: str = Form("reports_service"),
    max_tokens: int = Form(1536),
    strict_validation: bool = Form(False),
    dialect: str = Form("tsql"),
    compliance_profile: str = Form("strict"),
) -> ParseResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")
    if not file.filename.lower().endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are supported.")

    payload = await file.read()
    max_file_bytes = int(os.getenv("PARSER_MAX_FILE_BYTES", "1048576"))
    if len(payload) > max_file_bytes:
        raise HTTPException(status_code=413, detail="Uploaded file exceeds max allowed bytes.")

    try:
        python_code = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Uploaded file must be UTF-8 encoded.") from exc

    request = ParseRequest(
        python_code=python_code,
        file_name=Path(file.filename).name,
        prompt_file=prompt_file,
        output_dir=output_dir,
        report_dir=report_dir,
        max_tokens=max_tokens,
        strict_validation=strict_validation,
        dialect=dialect,
        compliance_profile=compliance_profile,
    )
    return _run_single(request)


@app.post("/parse/batch", response_model=BatchParseResponse)
def parse_batch(request: BatchParseRequest) -> BatchParseResponse:
    max_batch = int(os.getenv("PARSER_MAX_BATCH_FILES", "50"))
    if len(request.requests) > max_batch:
        raise HTTPException(status_code=400, detail=f"Batch too large. Max {max_batch}.")
    results = [_run_single(item) for item in request.requests]
    return BatchParseResponse(results=results)

