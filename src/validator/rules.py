from dataclasses import dataclass, field
import re

from src.ir.models import LineageIR
from src.validator.sql_parser import split_sql_statements


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    metrics: dict[str, int] = field(default_factory=dict)


def _normalize_asset(asset: str) -> str:
    return asset.strip().strip("\"'").lower()


def _normalize_target_asset(asset: str) -> str:
    normalized = _normalize_asset(asset)
    normalized = re.sub(r"\s+partition\s*\([^)]+\)\s*$", "", normalized, flags=re.IGNORECASE)
    return normalized.strip()


def _substitute_known_variables(asset: str, variables: dict[str, str]) -> str:
    current = asset
    for key, value in variables.items():
        if not isinstance(value, str) or not value:
            continue
        current = current.replace(f"{{{key}}}", value)
        current = re.sub(rf"\b{re.escape(key)}\b", value, current)
    return current


def _asset_in_text(asset: str, text: str) -> bool:
    normalized = _normalize_target_asset(asset)
    if normalized in text:
        return True
    if normalized.startswith("#") and normalized[1:] in text:
        return True
    if ("#" + normalized) in text:
        return True
    return False


def _extract_generated_sources(sql_text: str) -> set[str]:
    sources: set[str] = set()
    cte_names: set[str] = set()
    for stmt in split_sql_statements(sql_text):
        for cte in re.findall(r"\bWITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", stmt, flags=re.IGNORECASE):
            cte_names.add(_normalize_asset(cte))
        for src in re.findall(
            r"\b(?:FROM|JOIN|USING)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            stmt,
            flags=re.IGNORECASE,
        ):
            normalized = _normalize_target_asset(src)
            if not normalized:
                continue
            if normalized in {"select", "where", "on", "when"}:
                continue
            sources.add(normalized)
    return {s for s in sources if s not in cte_names}


def _normalize_source_asset(asset: str) -> str:
    token = asset.strip().rstrip(",")
    token = re.sub(r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", token, flags=re.IGNORECASE)
    token = re.sub(r"\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", token)
    return _normalize_target_asset(token)


def _build_allowed_sources(ir: LineageIR) -> set[str]:
    allowed: set[str] = set()
    for op in ir.operations:
        for asset in op.source_assets:
            if asset:
                replaced = _substitute_known_variables(asset, ir.variables)
                allowed.add(_normalize_source_asset(replaced))
        for asset in op.target_assets:
            if asset:
                allowed.add(_normalize_target_asset(_substitute_known_variables(asset, ir.variables)))
    for temp in ir.allowed_temp_assets:
        if temp:
            allowed.add(_normalize_target_asset(temp))
    return {a for a in allowed if a}


def _is_source_allowed(source: str, allowed_sources: set[str]) -> bool:
    if source in allowed_sources:
        return True
    if source.startswith("#") and source[1:] in allowed_sources:
        return True
    if ("#" + source) in allowed_sources:
        return True
    return False


def _has_source_evidence(ir: LineageIR) -> bool:
    for op in ir.operations:
        if any(a for a in op.source_assets if a):
            return True
    return False


def validate_sql(
    sql_text: str,
    ir: LineageIR,
    *,
    enforce_explicit_insert_columns: bool = True,
    enforce_required_targets: bool = True,
    enforce_evidence_sources: bool = True,
) -> ValidationResult:
    statements = split_sql_statements(sql_text)
    errors: list[str] = []
    metrics: dict[str, int] = {"statement_count": len(statements)}

    if not statements and ir.operations:
        errors.append("No SQL statements generated for file with detected operations.")
        return ValidationResult(ok=False, errors=errors, metrics=metrics)

    for idx, statement in enumerate(statements, start=1):
        upper_stmt = statement.upper().strip()
        is_insert = upper_stmt.startswith("INSERT INTO")
        is_update = upper_stmt.startswith("UPDATE ")
        is_merge = upper_stmt.startswith("MERGE INTO")
        has_from = bool(re.search(r"\bFROM\b", upper_stmt))
        has_set = bool(re.search(r"\bSET\b", upper_stmt))
        has_using = bool(re.search(r"\bUSING\b", upper_stmt))
        if not (is_insert or is_update or is_merge):
            errors.append(f"Statement {idx} missing recognized write operation.")
        if is_insert and not has_from:
            errors.append(f"Statement {idx} missing FROM for INSERT.")
        if enforce_explicit_insert_columns and is_insert and re.search(
            r"^\s*INSERT\s+INTO\s+[A-Z0-9_#\".`:/\\\-\{\}]+(?:\s+PARTITION\s*\([^)]+\))?\s+SELECT\s+\*\s+FROM\b",
            upper_stmt,
            flags=re.IGNORECASE,
        ):
            errors.append(f"Statement {idx} uses SELECT * for INSERT; explicit columns are required.")
        if is_update and not has_set:
            errors.append(f"Statement {idx} missing SET for UPDATE.")
        if is_merge and not has_using:
            errors.append(f"Statement {idx} missing USING for MERGE.")
        if not statement.strip().endswith(";"):
            errors.append(f"Statement {idx} does not end with ';'.")

    if enforce_required_targets:
        required_targets = {
            _normalize_target_asset(_substitute_known_variables(asset, ir.variables))
            for op in ir.operations
            if op.op_type == "write"
            for asset in op.target_assets
            if asset
        }
        if required_targets:
            produced = _normalize_asset(sql_text)
            missing = [target for target in sorted(required_targets) if not _asset_in_text(target, produced)]
            if missing:
                errors.append(f"Missing write targets in SQL output: {', '.join(missing)}")

    if enforce_evidence_sources:
        generated_sources = _extract_generated_sources(sql_text)
        allowed_sources = _build_allowed_sources(ir)
        if _has_source_evidence(ir) and allowed_sources:
            unknown_sources = sorted(s for s in generated_sources if not _is_source_allowed(s, allowed_sources))
            if unknown_sources:
                errors.append(
                    "Generated SQL contains non-evidenced sources: " + ", ".join(unknown_sources)
                )

    return ValidationResult(ok=not errors, errors=errors, metrics=metrics)

