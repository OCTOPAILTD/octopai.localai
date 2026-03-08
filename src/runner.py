from pathlib import Path
import json
import re
import time
from typing import Optional

from src.config import RuntimeConfig
from src.baseline_parity import all_parity_gates_pass, compare_sql_with_baseline_dir
from src.errors import ValidationFailure
from src.extractor.ast_extractor import extract_lineage_ir
from src.ir.models import LineageIR
from src.generator.llm_client import call_chat_completion
from src.logging_utils import get_logger
from src.generator.prompts import (
    build_chunk_system_prompt,
    build_chunk_user_prompt,
    build_refine_system_prompt,
    build_refine_user_prompt,
    build_validation_system_prompt,
    build_validation_user_prompt,
)
from src.merger.merge import merge_and_deduplicate_sql
from src.planner.unitizer import build_work_units
from src.reporting import FileReport, write_file_report
from src.resolver.functions import annotate_function_scopes
from src.resolver.vars import resolve_variables
from src.validator.rules import validate_sql
from src.validator.repair import drop_hallucinated_and_synthetic, remove_forbidden_where_clauses, _strip_sql_comments
from src.validator.sql_parser import split_sql_statements


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


def keep_insert_select_statements(sql_text: str) -> str:
    kept: list[str] = []
    for stmt in split_sql_statements(sql_text):
        upper_stmt = stmt.upper()
        starts = [idx for idx in (
            upper_stmt.find("INSERT INTO"),
            upper_stmt.find("UPDATE "),
            upper_stmt.find("MERGE INTO"),
        ) if idx >= 0]
        if starts:
            stmt = stmt[min(starts):].strip()
            upper_stmt = stmt.upper()
        has_from = bool(re.search(r"\bFROM\b", upper_stmt))
        has_set = bool(re.search(r"\bSET\b", upper_stmt))
        has_using = bool(re.search(r"\bUSING\b", upper_stmt))
        if "INSERT INTO" in upper_stmt and has_from:
            kept.append(stmt if stmt.strip().endswith(";") else f"{stmt.strip()};")
            continue
        if "UPDATE " in upper_stmt and has_set:
            kept.append(stmt if stmt.strip().endswith(";") else f"{stmt.strip()};")
            continue
        if "MERGE INTO" in upper_stmt and has_using:
            kept.append(stmt if stmt.strip().endswith(";") else f"{stmt.strip()};")
    return "\n".join(kept).strip()


def substitute_known_variables(sql_text: str, variables: dict[str, str]) -> str:
    if not sql_text:
        return sql_text
    current = sql_text
    for key, value in variables.items():
        if not isinstance(value, str) or not value:
            continue
        current = current.replace(f"{{{key}}}", value)
        # Also replace plain variable tokens (e.g. TARGET_TABLE) when models omit braces.
        token_pattern = re.compile(rf"\b{re.escape(key)}\b")
        current = token_pattern.sub(value, current)
    return current


def _normalize_target_asset(asset: str) -> str:
    normalized = _normalize_asset(asset)
    # Some extracted write targets can include trailing partition spec.
    normalized = re.sub(r"\s+partition\s*\([^)]+\)\s*$", "", normalized, flags=re.IGNORECASE)
    return normalized.strip()


def _resolve_target_asset(asset: str, variables: dict[str, str]) -> str:
    return _normalize_target_asset(substitute_known_variables(asset, variables))


def extract_known_insert_columns(ir: LineageIR) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    pattern = re.compile(
        r"INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+SELECT\s+(.*?)\s+FROM\s",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for op in ir.operations:
        sql_text = str(op.metadata.get("sql_text", ""))
        if not sql_text:
            continue
        if str(op.metadata.get("statement_kind", "")).strip().lower() != "insert":
            continue
        target_assets = op.target_assets or []
        target = _resolve_target_asset(target_assets[0], ir.variables) if target_assets else ""
        select_items = op.metadata.get("select_items", [])
        projections: list[str] = []
        if isinstance(select_items, list):
            for item in select_items:
                if not isinstance(item, dict):
                    continue
                expr_text = str(item.get("expression", "")).strip().rstrip(";")
                if expr_text:
                    projections.append(expr_text)
        if target and projections:
            mapping[target] = projections
            continue
        sql_text = substitute_known_variables(sql_text, ir.variables)
        match = pattern.search(sql_text)
        if not match:
            continue
        target = _normalize_target_asset(match.group(1))
        select_part = match.group(2).strip()
        if not select_part or select_part == "*":
            continue
        cols = [c.strip() for c in _split_select_items(select_part) if c.strip()]
        if cols:
            mapping[target] = cols
    return mapping


def enforce_known_columns(sql_text: str, known_columns_by_target: dict[str, list[str]]) -> str:
    if not sql_text or not known_columns_by_target:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        target_match = re.search(
            r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
            current,
            flags=re.IGNORECASE,
        )
        if target_match:
            target = _normalize_target_asset(target_match.group(1))
            # Keep lineage-surrogate self-update inserts as authored (do not expand projection).
            first_from = re.search(
                r"\bFROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
                current,
                flags=re.IGNORECASE,
            )
            if first_from and _assets_match(_normalize_target_asset(first_from.group(1)), target):
                statements.append(current if current.endswith(";") else f"{current};")
                continue
            cols = known_columns_by_target.get(target)
            if not cols:
                # Fallback: if INSERT uses SELECT * FROM known source, reuse source columns.
                star_src = re.search(
                    r"\bSELECT\s+\*\s+FROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
                    current,
                    flags=re.IGNORECASE,
                )
                if star_src:
                    src_key = _normalize_target_asset(star_src.group(1))
                    cols = known_columns_by_target.get(src_key)
            if cols:
                cleaned_cols = [c.strip().rstrip(";") for c in cols if isinstance(c, str) and c.strip()]
                if not cleaned_cols:
                    statements.append(current if current.endswith(";") else f"{current};")
                    continue
                upper_current = current.upper()
                has_select_star = bool(re.search(r"\bSELECT\s+\*\s+FROM\b", current, flags=re.IGNORECASE))
                # Guardrail: avoid rewriting highly complex evidence SQL with inline comments/case blocks.
                safe_to_force = (
                    has_select_star
                    or (
                        "/*" not in current
                        and "--" not in current
                        and " CASE " not in upper_current
                        and len(current) < 3000
                    )
                )
                if safe_to_force:
                    replacement = f"SELECT {', '.join(cleaned_cols)} FROM"
                    current = re.sub(
                        r"\bSELECT\s+.*?\s+FROM\b",
                        replacement,
                        current,
                        count=1,
                        flags=re.IGNORECASE | re.DOTALL,
                    )
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def normalize_and_dedupe_statements(sql_text: str, ir: Optional[LineageIR] = None) -> str:
    if not sql_text:
        return sql_text
    current = sql_text
    if ir is not None:
        current = _canonicalize_temp_assets(current, ir)
        current = _dedupe_write_signatures(current)
    return merge_and_deduplicate_sql([current])


def apply_prompt_compliance_pass(
    sql_text: str,
    ir: LineageIR,
    known_columns_by_target: dict[str, list[str]],
    no_where: bool,
    compliance_profile: str = "strict",
    write_only: bool = True,
    add_partition_surrogate: bool = False,
    strip_partition_for_visual: bool = False,
) -> str:
    if not sql_text:
        return sql_text
    current = sql_text
    if compliance_profile != "baseline_sql_parity":
        # Comments can confuse downstream lineage parsers; strip them in final output.
        cleaned_stmts: list[str] = []
        for stmt in split_sql_statements(current):
            stripped = _strip_sql_comments(stmt).strip()
            if stripped:
                cleaned_stmts.append(stripped if stripped.endswith(";") else f"{stripped};")
        current = "\n".join(cleaned_stmts).strip()
    if no_where:
        current = remove_forbidden_where_clauses(current)
    if compliance_profile != "baseline_sql_parity":
        current = enforce_known_columns(current, known_columns_by_target)
        current = remove_where_on_temp_source_inserts(current)
        current = normalize_partition_clauses(current)
    current = substitute_known_variables(current, ir.variables)
    if compliance_profile != "baseline_sql_parity":
        current = fix_missing_window_by_spacing(current)
        current = convert_updates_to_lineage_inserts(current, known_columns_by_target)
    if write_only and compliance_profile != "baseline_sql_parity":
        current = keep_write_statements_only(current)
    current = remove_metric_and_noop_self_writes(current)
    if compliance_profile != "baseline_sql_parity":
        current = fix_missing_subquery_closing_parenthesis(current)
        current = fix_missing_join_wrapper_parentheses(current)
        current = fix_missing_rownum_wrapper_parentheses(current)
        current = normalize_and_dedupe_statements(current, ir)
    if strip_partition_for_visual:
        current = strip_partition_clause_in_inserts(current)
        current = rewrite_temp_hop_writes_to_direct_source(current, ir)
        current = fix_missing_rownum_wrapper_parentheses(current)
        current = normalize_and_dedupe_statements(current, ir)
    if add_partition_surrogate:
        current = add_partition_lineage_surrogates(current)
        current = add_transitive_temp_target_surrogates(current, ir)
        current = collapse_equivalent_partition_writes(current)
    return current


def keep_write_statements_only(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    kept: list[str] = []
    for stmt in split_sql_statements(sql_text):
        s = stmt.strip()
        if not s:
            continue
        upper = s.upper()
        has_from = bool(re.search(r"\bFROM\b", upper))
        has_set = bool(re.search(r"\bSET\b", upper))
        has_using = bool(re.search(r"\bUSING\b", upper))
        insert_shape_ok = bool(
            re.search(
                r"^\s*INSERT\s+INTO\s+[A-Za-z0-9_#\".`:/\\\-\{\}]+(?:\s*\([^)]*\))?(?:\s+PARTITION\s*\([^)]+\))?\s+SELECT\b.*\bFROM\b",
                s,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        if insert_shape_ok and has_from:
            kept.append(s if s.endswith(";") else f"{s};")
            continue
        if upper.startswith("UPDATE ") and has_set:
            kept.append(s if s.endswith(";") else f"{s};")
            continue
        if upper.startswith("MERGE INTO") and has_using:
            kept.append(s if s.endswith(";") else f"{s};")
    return "\n".join(kept).strip()


def remove_metric_and_noop_self_writes(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    cleaned: list[str] = []
    self_insert_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+SELECT\s+\*\s+FROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s*;?\s*$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for stmt in split_sql_statements(sql_text):
        s = stmt.strip()
        if not s:
            continue
        match = self_insert_re.match(s)
        if match:
            target = _normalize_target_asset(match.group(1))
            source = _normalize_target_asset(match.group(2))
            if _assets_match(target, source):
                # No-op self copy is typically metrics/validation noise.
                continue
        cleaned.append(s if s.endswith(";") else f"{s};")
    return "\n".join(cleaned).strip()


def fix_missing_window_by_spacing(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    repaired: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        # e.g. PARTITION BYID_NUMBER -> PARTITION BY ID_NUMBER
        current = re.sub(
            r"(\bPARTITION\s+BY)(?=[A-Za-z_#\"`])",
            r"\1 ",
            current,
            flags=re.IGNORECASE,
        )
        repaired.append(current if current.endswith(";") else f"{current};")
    return "\n".join(repaired).strip()


def normalize_tsql_placeholders(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    current = sql_text
    # Quoted brace placeholders -> T-SQL variable token.
    current = re.sub(r"'\{([A-Za-z_][A-Za-z0-9_]*)\}'", r"@\1", current)
    # Unquoted brace placeholders -> T-SQL variable token.
    current = re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", r"@\1", current)
    return current


def _extract_tables_from_statement(statement: str) -> list[str]:
    tables = re.findall(
        r"\b(?:FROM|JOIN|USING)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        statement,
        flags=re.IGNORECASE,
    )
    seen: set[str] = set()
    ordered: list[str] = []
    for table in tables:
        normalized = _normalize_target_asset(table)
        if not normalized or normalized in {"select", "where", "on", "when"}:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _build_update_lineage_projection(
    update_stmt: str, known_columns_by_target: dict[str, list[str]], target_norm: str
) -> list[str]:
    known_cols = known_columns_by_target.get(target_norm, [])
    known_lookup = {c.lower(): c for c in known_cols if isinstance(c, str)}
    picked: list[str] = []

    for match in re.finditer(
        r"\bWHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s+IN\s*\(",
        update_stmt,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        col = match.group(1)
        picked.append(known_lookup.get(col.lower(), col))

    set_match = re.search(
        r"\bSET\b(.*?)(?:\bWHERE\b|$)",
        update_stmt,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if set_match:
        set_clause = set_match.group(1)
        for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=", set_clause, flags=re.IGNORECASE):
            col = match.group(1)
            if col.lower() in {"update", "set", "where", "and", "or"}:
                continue
            picked.append(known_lookup.get(col.lower(), col))

    for match in re.finditer(
        r"\bBETWEEN\s+([A-Za-z_][A-Za-z0-9_]*)\s+AND\s+([A-Za-z_][A-Za-z0-9_]*)",
        update_stmt,
        flags=re.IGNORECASE,
    ):
        left_col = match.group(1)
        right_col = match.group(2)
        picked.append(known_lookup.get(left_col.lower(), left_col))
        picked.append(known_lookup.get(right_col.lower(), right_col))

    out: list[str] = []
    seen: set[str] = set()
    for col in picked:
        key = col.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(col)

    if out:
        return out

    clean_known = [c for c in known_cols if isinstance(c, str) and c.strip()]
    if clean_known:
        return clean_known[: min(6, len(clean_known))]
    return ["lineage_touch_col"]


def convert_updates_to_lineage_inserts(sql_text: str, known_columns_by_target: dict[str, list[str]]) -> str:
    if not sql_text:
        return sql_text
    converted: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        match = re.search(
            r"^\s*UPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
            current,
            flags=re.IGNORECASE,
        )
        if not match:
            converted.append(current if current.endswith(";") else f"{current};")
            continue
        target_norm = _normalize_target_asset(match.group(1))
        rendered_target = _render_output_asset(target_norm)
        sources = [src for src in _extract_tables_from_statement(current) if not _assets_match(src, target_norm)]
        if not sources:
            converted.append(current if current.endswith(";") else f"{current};")
            continue

        projection = _build_update_lineage_projection(current, known_columns_by_target, target_norm)
        select_expr = ", ".join([f"tgt.{col}" for col in projection])
        from_lines = [f"FROM {rendered_target} AS tgt"]
        for idx, src in enumerate(sources, start=1):
            from_lines.append(f"JOIN {_render_output_asset(src)} AS src{idx} ON 1=1")
        lineage_stmt = (
            f"INSERT INTO {rendered_target}\n"
            f"SELECT {select_expr}\n"
            + "\n".join(from_lines)
            + ";"
        )
        converted.append(lineage_stmt)
    return "\n".join(converted).strip()


def normalize_partition_clauses(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        # Many engines reject INSERT target column list with PARTITION clause.
        current = re.sub(
            r"(\bINSERT\s+INTO\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s*\([^)]*\)(\s+PARTITION\s*\([^)]+\))",
            r"\1\2\3",
            current,
            flags=re.IGNORECASE,
        )
        def _dedupe_partition(match: re.Match) -> str:
            head = match.group(1)
            partitions = match.group(2)
            first_partition = re.search(r"PARTITION\s*\([^)]+\)", partitions, flags=re.IGNORECASE)
            if not first_partition:
                return match.group(0)
            return f"{head} {first_partition.group(0)} "
        current = re.sub(
            r"(\bINSERT\s+INTO\s+[A-Za-z0-9_#\".`:/\\\-\{\}]+(?:\s*\([^)]*\))?)\s+((?:PARTITION\s*\([^)]+\)\s*){2,})",
            _dedupe_partition,
            current,
            flags=re.IGNORECASE,
        ).strip()
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def add_partition_lineage_surrogates(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements = [s.strip() for s in split_sql_statements(sql_text) if s.strip()]
    existing = {s.upper().strip() for s in statements}
    additions: list[str] = []
    pattern = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+PARTITION\s*\([^)]+\)\s+SELECT\s+(.*?)\s+FROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s*;?\s*$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for stmt in statements:
        match = pattern.match(stmt)
        if not match:
            continue
        target, select_list, source = match.group(1), match.group(2).strip(), match.group(3)
        surrogate = f"INSERT INTO {target} SELECT {select_list} FROM {source};"
        key = surrogate.upper().strip()
        if key in existing:
            continue
        additions.append(surrogate)
        existing.add(key)
    if not additions:
        return sql_text
    payload = "\n".join([*(s if s.endswith(";") else f"{s};" for s in statements), *additions])
    return payload.strip()


def add_transitive_temp_target_surrogates(sql_text: str, ir: LineageIR) -> str:
    if not sql_text:
        return sql_text
    statements = [s.strip() for s in split_sql_statements(sql_text) if s.strip()]
    if not statements:
        return sql_text
    insert_target_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
        flags=re.IGNORECASE,
    )
    producer_by_temp: dict[str, str] = {}
    for stmt in statements:
        m = insert_target_re.match(stmt)
        if not m:
            continue
        tgt = m.group(1).strip()
        tgt_norm = _normalize_asset(tgt)
        if tgt_norm.startswith("#"):
            producer_by_temp[tgt_norm] = stmt
            producer_by_temp[tgt_norm[1:]] = stmt

    existing = {s.upper().strip() for s in statements}
    additions: list[str] = []
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write" or not op.target_assets:
            continue
        target_asset = _resolve_target_asset(_normalize_temp_asset_name(op.target_assets[0], ir), ir.variables)
        if target_asset.startswith("#"):
            continue
        sources = _sources_for_write_op(ir, idx)
        if not sources:
            continue
        temp_src_key = _normalize_asset(_normalize_temp_asset_name(sources[0], ir))
        producer_stmt = producer_by_temp.get(temp_src_key)
        if not producer_stmt:
            continue
        surrogate = re.sub(
            r"^(\s*INSERT\s+INTO\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            r"\1" + target_asset,
            producer_stmt,
            count=1,
            flags=re.IGNORECASE,
        ).strip()
        key = surrogate.upper().strip()
        if key in existing:
            continue
        additions.append(surrogate if surrogate.endswith(";") else f"{surrogate};")
        existing.add(key)

    if not additions:
        return sql_text
    return "\n".join([*(s if s.endswith(";") else f"{s};" for s in statements), *additions]).strip()


def collapse_equivalent_partition_writes(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements = [s.strip() for s in split_sql_statements(sql_text) if s.strip()]
    plain_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+SELECT\s+(.*?)\s+FROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s*;?\s*$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    part_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+PARTITION\s*\([^)]+\)\s+SELECT\s+(.*?)\s+FROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s*;?\s*$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    plain_keys: set[tuple[str, str, str]] = set()
    for stmt in statements:
        m = plain_re.match(stmt)
        if not m:
            continue
        plain_keys.add(
            (
                _normalize_asset(m.group(1)),
                re.sub(r"\s+", " ", m.group(2)).strip().lower(),
                _normalize_asset(m.group(3)),
            )
        )
    kept: list[str] = []
    for stmt in statements:
        m = part_re.match(stmt)
        if m:
            key = (
                _normalize_asset(m.group(1)),
                re.sub(r"\s+", " ", m.group(2)).strip().lower(),
                _normalize_asset(m.group(3)),
            )
            if key in plain_keys:
                continue
        kept.append(stmt if stmt.endswith(";") else f"{stmt};")
    return "\n".join(kept).strip()


def strip_partition_clause_in_inserts(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        current = re.sub(
            r"(\bINSERT\s+INTO\s+[A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+PARTITION\s*\([^)]+\)\s+",
            r"\1 ",
            current,
            flags=re.IGNORECASE,
        )
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def rewrite_temp_hop_writes_to_direct_source(sql_text: str, ir: LineageIR) -> str:
    if not sql_text:
        return sql_text
    statements = [s.strip() for s in split_sql_statements(sql_text) if s.strip()]
    if not statements:
        return sql_text

    insert_target_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
        flags=re.IGNORECASE,
    )
    temp_source_re = re.compile(
        r"^\s*INSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+SELECT\s+.*?\s+FROM\s+(#[A-Za-z0-9_#\".`:/\\\-\{\}]+)\b",
        flags=re.IGNORECASE | re.DOTALL,
    )

    producer_by_temp: dict[str, str] = {}
    for stmt in statements:
        m = insert_target_re.match(stmt)
        if not m:
            continue
        tgt = m.group(1).strip()
        tgt_norm = _normalize_asset(tgt)
        if tgt_norm.startswith("#"):
            producer_by_temp[tgt_norm] = stmt
            producer_by_temp[tgt_norm[1:]] = stmt

    rewritten: list[str] = []
    for stmt in statements:
        m = temp_source_re.match(stmt)
        if not m:
            rewritten.append(stmt if stmt.endswith(";") else f"{stmt};")
            continue
        final_target = m.group(1).strip()
        temp_src = _normalize_asset(m.group(2))
        producer = producer_by_temp.get(temp_src)
        if not producer:
            rewritten.append(stmt if stmt.endswith(";") else f"{stmt};")
            continue
        direct = re.sub(
            r"^(\s*INSERT\s+INTO\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            r"\1" + final_target,
            producer,
            count=1,
            flags=re.IGNORECASE,
        ).strip()
        rewritten.append(direct if direct.endswith(";") else f"{direct};")
    return "\n".join(rewritten).strip()


def fix_missing_rownum_wrapper_parentheses(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        # Repair a common truncation shape:
        #   ) AS alias WHERE RN = 1 ) AS alias ON ...
        # where one wrapper ") AS alias" before WHERE was dropped.
        pattern = re.compile(
            r"\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*1\s+\)\s+AS\s+\1\s+ON\b",
            flags=re.IGNORECASE,
        )
        for match in pattern.finditer(current):
            alias = match.group(1)
            rn_col = match.group(2)
            fixed_fragment = f") AS {alias} ) AS {alias} WHERE {rn_col} = 1 ) AS {alias} ON"
            if fixed_fragment.lower() in current.lower():
                continue
            current = f"{current[:match.start()]}{fixed_fragment}{current[match.end():]}"
            break
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def fix_missing_subquery_closing_parenthesis(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        if re.search(r"\bFROM\s*\(", current, flags=re.IGNORECASE):
            opens = current.count("(")
            closes = current.count(")")
            if opens == closes + 1:
                # Common truncated form: "... FROM (<subquery> alias;" -> "... FROM (<subquery>) alias;"
                current = re.sub(
                    r"\s+([A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$",
                    r") \1;",
                    current,
                )
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def fix_missing_join_wrapper_parentheses(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    statements: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        if "LEFT JOIN (" not in current.upper():
            statements.append(current if current.endswith(";") else f"{current};")
            continue
        # Repair shape:
        #   LEFT JOIN ( ... FROM ( ... ) AS alias ON ...
        # where one wrapper ") AS alias" before ON is missing.
        pattern = re.compile(
            r"\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\b",
            flags=re.IGNORECASE,
        )
        for match in pattern.finditer(current):
            alias = match.group(1)
            fixed_fragment = f") AS {alias} ) AS {alias} ON"
            if fixed_fragment.lower() in current.lower():
                continue
            current = f"{current[:match.start()]}{fixed_fragment}{current[match.end():]}"
            break
        statements.append(current if current.endswith(";") else f"{current};")
    return "\n".join(statements).strip()


def remove_where_on_temp_source_inserts(sql_text: str) -> str:
    if not sql_text:
        return sql_text
    fixed: list[str] = []
    pattern = re.compile(
        r"^(\s*INSERT\s+INTO\s+[A-Za-z0-9_#\".`:/\\\-\{\}]+(?:\s*\([^)]*\))?(?:\s+PARTITION\s*\([^)]+\))?\s+SELECT\b.*?\bFROM\s+#[A-Za-z0-9_#\".`:/\\\-\{\}]+)\s+\bWHERE\b.*$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        match = pattern.match(current)
        if match:
            current = match.group(1).strip()
        fixed.append(current if current.endswith(";") else f"{current};")
    return "\n".join(fixed).strip()


def _normalize_temp_asset_name(asset: str, ir: LineageIR) -> str:
    if asset.startswith("#"):
        return asset
    candidate = f"#{asset}"
    if candidate.lower() in {t.lower() for t in ir.allowed_temp_assets}:
        return candidate
    return asset


def _render_output_asset(asset: str) -> str:
    return asset


def _source_for_write_op(ir: LineageIR, op_index: int) -> Optional[str]:
    op = ir.operations[op_index]
    for src in op.source_assets:
        normalized = _normalize_temp_asset_name(src, ir)
        return normalized

    # For temp-view writes, backfill from closest preceding read operation.
    if op.target_assets and op.target_assets[0].startswith("#"):
        for prev in range(op_index - 1, -1, -1):
            prev_op = ir.operations[prev]
            if prev_op.op_type != "read":
                continue
            if prev_op.source_assets:
                return prev_op.source_assets[0]
    return None


def _sources_for_write_op(ir: LineageIR, op_index: int) -> list[str]:
    op = ir.operations[op_index]
    sources: list[str] = []
    for src in op.source_assets:
        normalized = _normalize_temp_asset_name(src, ir)
        if normalized and normalized not in sources:
            sources.append(normalized)

    # For temp-view writes, backfill from closest preceding read operation.
    if (not sources) and op.target_assets and op.target_assets[0].startswith("#"):
        for prev in range(op_index - 1, -1, -1):
            prev_op = ir.operations[prev]
            if prev_op.op_type != "read":
                continue
            for src in prev_op.source_assets:
                if src and src not in sources:
                    sources.append(src)
            if sources:
                break
    return sources


def _write_kind_from_sql_text(sql_text: str) -> str:
    head = sql_text.strip().upper()
    if head.startswith("UPDATE "):
        return "update"
    if head.startswith("MERGE INTO"):
        return "merge"
    return "insert"


def _write_kind_from_op(op: object) -> str:
    kind = str(getattr(op, "metadata", {}).get("statement_kind", "")).strip().lower()
    if kind in {"insert", "update", "merge"}:
        return kind
    return _write_kind_from_sql_text(str(getattr(op, "metadata", {}).get("sql_text", "")))


def _normalize_asset(asset: str) -> str:
    return asset.strip().strip("\"'").lower()


def _assets_match(lhs: str, rhs: str) -> bool:
    l = _normalize_asset(lhs)
    r = _normalize_asset(rhs)
    if l == r:
        return True
    if l.startswith("#") and l[1:] == r:
        return True
    if r.startswith("#") and r[1:] == l:
        return True
    return False


def _statement_write_coverage(sql_text: str) -> dict[tuple[str, str], set[str]]:
    coverage: dict[tuple[str, str], set[str]] = {}
    for statement in split_sql_statements(sql_text):
        stmt = statement.strip()
        if not stmt:
            continue
        upper_stmt = stmt.upper()
        kind = ""
        target = ""
        if upper_stmt.startswith("INSERT INTO"):
            kind = "insert"
            match = re.search(r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if match:
                target = _normalize_asset(match.group(1))
        elif upper_stmt.startswith("UPDATE "):
            kind = "update"
            match = re.search(r"\bUPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if match:
                target = _normalize_asset(match.group(1))
        elif upper_stmt.startswith("MERGE INTO"):
            kind = "merge"
            match = re.search(r"\bMERGE\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if match:
                target = _normalize_asset(match.group(1))
        if not kind or not target:
            continue
        sources = {
            _normalize_asset(m)
            for m in re.findall(r"\b(?:FROM|JOIN|USING)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
        }
        key = (kind, target)
        coverage.setdefault(key, set()).update(sources)
    return coverage


def _normalize_signature_asset(asset: str) -> str:
    normalized = _normalize_asset(asset)
    return normalized[1:] if normalized.startswith("#") else normalized


def _canonicalize_temp_assets(sql_text: str, ir: LineageIR) -> str:
    current = sql_text
    for temp in sorted(ir.allowed_temp_assets):
        if not temp.startswith("#"):
            continue
        bare = temp[1:]
        if not bare:
            continue
        pattern = re.compile(rf"(?<![A-Za-z0-9_#])#?{re.escape(bare)}(?![A-Za-z0-9_])", flags=re.IGNORECASE)
        current = pattern.sub(f"#{bare}", current)
    return current


def _statement_signature(statement: str) -> tuple[str, str, tuple[str, ...]]:
    stmt = statement.strip()
    upper_stmt = stmt.upper()
    kind = ""
    target = ""
    if upper_stmt.startswith("INSERT INTO"):
        kind = "insert"
        match = re.search(r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
        if match:
            target = _normalize_signature_asset(match.group(1))
    elif upper_stmt.startswith("UPDATE "):
        kind = "update"
        match = re.search(r"\bUPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
        if match:
            target = _normalize_signature_asset(match.group(1))
    elif upper_stmt.startswith("MERGE INTO"):
        kind = "merge"
        match = re.search(r"\bMERGE\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
        if match:
            target = _normalize_signature_asset(match.group(1))
    sources = tuple(
        sorted(
            {
                _normalize_signature_asset(m)
                for m in re.findall(
                    r"\b(?:FROM|JOIN|USING)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
                    stmt,
                    flags=re.IGNORECASE,
                )
            }
        )
    )
    return kind, target, sources


def _statement_quality_score(statement: str) -> tuple[int, int, int]:
    upper = statement.upper()
    has_select_star = bool(re.search(r"\bSELECT\s+\*\s+FROM\b", upper))
    prefer_hash_temp = 1 if "#" in statement else 0
    explicit_columns = 0 if has_select_star else 1
    return explicit_columns, prefer_hash_temp, len(statement)


def _dedupe_write_signatures(sql_text: str) -> str:
    chosen: dict[tuple[str, str, tuple[str, ...]], tuple[tuple[int, int, int], str]] = {}
    passthrough: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        signature = _statement_signature(current)
        if not signature[0] or not signature[1]:
            passthrough.append(current if current.endswith(";") else f"{current};")
            continue
        quality = _statement_quality_score(current)
        existing = chosen.get(signature)
        if existing is None or quality > existing[0]:
            chosen[signature] = (quality, current if current.endswith(";") else f"{current};")
    ordered = [payload for _, payload in chosen.values()]
    ordered.extend(passthrough)
    return "\n".join(ordered).strip()


def _required_write_contract(ir: LineageIR) -> list[tuple[str, str, set[str]]]:
    contract: list[tuple[str, str, set[str]]] = []
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write" or not op.target_assets:
            continue
        kind = _write_kind_from_op(op)
        target = _resolve_target_asset(_normalize_temp_asset_name(op.target_assets[0], ir), ir.variables)
        sources = {_normalize_asset(s) for s in _sources_for_write_op(ir, idx) if s}
        contract.append((kind, target, sources))
    return contract


def _lookup_sources_for_signature(
    coverage: dict[tuple[str, str], set[str]], kind: str, target: str
) -> set[str]:
    for (k, t), sources in coverage.items():
        if k != kind:
            continue
        if _assets_match(t, target):
            return sources
    return set()


def _contract_score(sql_text: str, ir: LineageIR) -> tuple[int, int]:
    coverage = _statement_write_coverage(sql_text)
    sig_hits = 0
    source_hits = 0
    for kind, target, required_sources in _required_write_contract(ir):
        present_sources = _lookup_sources_for_signature(coverage, kind, target)
        if present_sources:
            sig_hits += 1
        if required_sources and present_sources:
            if all(any(_assets_match(req, got) for got in present_sources) for req in required_sources):
                source_hits += 1
    return sig_hits, source_hits


def _build_source_clause(sources: list[str]) -> str:
    cleaned = [_render_output_asset(s) for s in sources if s]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return ", ".join(cleaned)


def _rewrite_target_for_kind(statement: str, kind: str, rendered_target: str) -> str:
    if kind == "insert":
        return re.sub(
            r"(\bINSERT\s+INTO\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            r"\1" + rendered_target,
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
    if kind == "update":
        return re.sub(
            r"(\bUPDATE\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            r"\1" + rendered_target,
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
    if kind == "merge":
        return re.sub(
            r"(\bMERGE\s+INTO\s+)([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
            r"\1" + rendered_target,
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
    return statement


def _build_evidence_write_statement(ir: LineageIR, op_index: int) -> str:
    op = ir.operations[op_index]
    if op.op_type != "write":
        return ""
    if not op.target_assets:
        return ""
    target = _normalize_temp_asset_name(op.target_assets[0], ir)
    rendered_target = _render_output_asset(target)
    kind = _write_kind_from_op(op)

    sql_text = substitute_known_variables(str(op.metadata.get("sql_text", "")), ir.variables).strip()
    if sql_text:
        upper_sql = sql_text.upper()
        has_write_keyword = (
            ("INSERT INTO" in upper_sql)
            or ("UPDATE " in upper_sql)
            or ("MERGE INTO" in upper_sql)
        )
        if not has_write_keyword:
            sql_text = ""
    if sql_text:
        statement = _rewrite_target_for_kind(sql_text, kind, rendered_target).strip()
        if statement:
            return statement if statement.endswith(";") else f"{statement};"

    # For temp-view writes, derive statement from immediately preceding read SQL.
    if str(op.metadata.get("statement_kind", "")).strip().lower() == "create_temp_view":
        for prev in range(op_index - 1, -1, -1):
            prev_op = ir.operations[prev]
            if prev_op.op_type != "read":
                continue
            read_sql = substitute_known_variables(str(prev_op.metadata.get("sql_text", "")), ir.variables).strip()
            if read_sql:
                statement = f"INSERT INTO {rendered_target} {read_sql}".strip()
                return statement if statement.endswith(";") else f"{statement};"
    return ""


def _split_select_items(select_part: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    for ch in select_part:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                piece = "".join(current).strip()
                if piece:
                    items.append(piece)
                current = []
                continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def _extract_output_column_name(expr: str) -> Optional[str]:
    alias_match = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b\s*$", expr, flags=re.IGNORECASE)
    if alias_match:
        return alias_match.group(1)
    trailing = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", expr)
    if trailing:
        return trailing.group(1)
    return None


def extract_known_temp_columns(ir: LineageIR) -> dict[str, list[str]]:
    temp_cols: dict[str, list[str]] = {}
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write" or not op.target_assets:
            continue
        target = _normalize_temp_asset_name(op.target_assets[0], ir)
        if not target.startswith("#"):
            continue
        for prev in range(idx - 1, -1, -1):
            read_op = ir.operations[prev]
            if read_op.op_type != "read":
                continue
            sql_text = str(read_op.metadata.get("sql_text", ""))
            if not sql_text:
                continue
            select_items = read_op.metadata.get("select_items", [])
            cols: list[str] = []
            if isinstance(select_items, list):
                for item in select_items:
                    if not isinstance(item, dict):
                        continue
                    alias_text = str(item.get("alias", "")).strip()
                    expr_text = str(item.get("expression", "")).strip().rstrip(";")
                    # Temp view schema is based on output column names.
                    col_name = alias_text or (_extract_output_column_name(expr_text) or "")
                    if col_name:
                        cols.append(col_name)
            if cols:
                target_norm = _normalize_asset(target)
                temp_cols[target_norm] = cols
                if target_norm.startswith("#"):
                    temp_cols[target_norm[1:]] = cols
                break
            match = re.search(r"\bSELECT\s+(.*?)\s+\bFROM\b", sql_text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            select_items = _split_select_items(match.group(1))
            cols = []
            for item in select_items:
                col = _extract_output_column_name(item.strip())
                if col:
                    cols.append(col)
            if cols:
                target_norm = _normalize_asset(target)
                temp_cols[target_norm] = cols
                if target_norm.startswith("#"):
                    temp_cols[target_norm[1:]] = cols
                break
    return temp_cols


def build_known_columns_from_temp_sources(
    ir: LineageIR, temp_columns: dict[str, list[str]]
) -> dict[str, list[str]]:
    target_cols: dict[str, list[str]] = {}
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write" or not op.target_assets:
            continue
        sql_text = str(op.metadata.get("sql_text", ""))
        if "SELECT *" not in sql_text.upper():
            continue
        target = _resolve_target_asset(_normalize_temp_asset_name(op.target_assets[0], ir), ir.variables)
        for src in _sources_for_write_op(ir, idx):
            cols = temp_columns.get(_normalize_asset(_normalize_temp_asset_name(src, ir)))
            if cols:
                target_cols[target] = cols
                break
    return target_cols


def reorder_statements_by_ir(sql_text: str, ir: LineageIR) -> str:
    statements = [s.strip() for s in split_sql_statements(sql_text) if s.strip()]
    if len(statements) <= 1:
        return sql_text
    op_keys: list[tuple[str, str]] = []
    for op in ir.operations:
        if op.op_type != "write" or not op.target_assets:
            continue
        kind = _write_kind_from_sql_text(str(op.metadata.get("sql_text", "")))
        target = _resolve_target_asset(_normalize_temp_asset_name(op.target_assets[0], ir), ir.variables)
        op_keys.append((kind, target))

    ranked: list[tuple[int, int, str]] = []
    for idx, stmt in enumerate(statements):
        k = _write_kind_from_sql_text(stmt)
        tgt = ""
        if k == "insert":
            m = re.search(r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if m:
                tgt = _normalize_asset(m.group(1))
        elif k == "update":
            m = re.search(r"\bUPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if m:
                tgt = _normalize_asset(m.group(1))
        elif k == "merge":
            m = re.search(r"\bMERGE\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)", stmt, flags=re.IGNORECASE)
            if m:
                tgt = _normalize_asset(m.group(1))

        rank = len(op_keys) + idx
        for op_idx, (ok, ot) in enumerate(op_keys):
            if ok != k:
                continue
            if _assets_match(tgt, ot):
                rank = op_idx
                break
        ranked.append((rank, idx, stmt if stmt.endswith(";") else f"{stmt};"))
    ranked.sort(key=lambda x: (x[0], x[1]))
    return "\n".join([stmt for _, _, stmt in ranked]).strip()


def augment_missing_write_flows_from_ir(
    sql_text: str, ir: LineageIR, known_columns_by_target: dict[str, list[str]]
) -> str:
    coverage = _statement_write_coverage(sql_text)
    additions: list[str] = []
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write":
            continue
        if not op.target_assets:
            continue
        target = _normalize_temp_asset_name(op.target_assets[0], ir)
        target_norm = _resolve_target_asset(target, ir.variables)
        write_kind = _write_kind_from_op(op)
        op_sources = _sources_for_write_op(ir, idx)
        if not op_sources:
            continue
        existing_sources = _lookup_sources_for_signature(coverage, write_kind, target_norm)
        if existing_sources and all(
            any(_assets_match(req, present) for present in existing_sources)
            for req in op_sources
        ):
            continue

        evidence_stmt = _build_evidence_write_statement(ir, idx)
        if evidence_stmt:
            evidence_cov = _statement_write_coverage(evidence_stmt)
            evidence_sources = _lookup_sources_for_signature(evidence_cov, write_kind, target_norm)
            if existing_sources and (
                not evidence_sources
                or all(any(_assets_match(src, cur) for cur in existing_sources) for src in evidence_sources)
            ):
                continue
            additions.append(evidence_stmt)
            coverage.update(_statement_write_coverage(evidence_stmt))
            continue

        source_clause = _build_source_clause(op_sources)
        known_cols = known_columns_by_target.get(target_norm, [])
        rendered_target = _render_output_asset(target)
        if write_kind == "update":
            if not source_clause:
                continue
            assign_col = known_cols[0] if known_cols else "lineage_touch_col"
            additions.append(f"UPDATE {rendered_target} SET {assign_col} = {assign_col} FROM {source_clause};")
        elif write_kind == "merge":
            if not source_clause:
                continue
            assign_col = known_cols[0] if known_cols else "lineage_touch_col"
            additions.append(
                f"MERGE INTO {rendered_target} USING {source_clause} ON 1=1 "
                f"WHEN MATCHED THEN UPDATE SET {assign_col} = {assign_col};"
            )
        else:
            select_expr = ", ".join(known_cols) if known_cols else "*"
            additions.append(f"INSERT INTO {rendered_target} SELECT {select_expr} FROM {source_clause};")
        coverage.setdefault((write_kind, target_norm), set()).update(_normalize_asset(s) for s in op_sources)

    if not additions:
        return sql_text
    base = sql_text.strip()
    payload = "\n".join(additions)
    return f"{base}\n{payload}".strip() if base else payload


def build_recovery_sql_from_ir(ir: LineageIR, known_columns_by_target: dict[str, list[str]]) -> str:
    statements: list[str] = []
    allowed_temp_assets = {x.lower() for x in ir.allowed_temp_assets}
    for idx, op in enumerate(ir.operations):
        if op.op_type != "write":
            continue
        targets = [t for t in op.target_assets if t]
        sources = _sources_for_write_op(ir, idx)
        if not targets or not sources:
            continue
        write_kind = _write_kind_from_op(op)
        evidence_stmt = _build_evidence_write_statement(ir, idx)
        if evidence_stmt:
            statements.append(evidence_stmt)
            continue
        source_clause = _build_source_clause(sources)
        if not source_clause:
            continue
        for target in targets:
            target_norm = target.lower()
            if target_norm.startswith("#") and target_norm not in allowed_temp_assets:
                continue
            known_cols = known_columns_by_target.get(_resolve_target_asset(target, ir.variables), [])
            rendered_target = _render_output_asset(target)
            if write_kind == "update":
                assign_col = known_cols[0] if known_cols else "lineage_touch_col"
                statements.append(f"UPDATE {rendered_target} SET {assign_col} = {assign_col} FROM {source_clause};")
            elif write_kind == "merge":
                assign_col = known_cols[0] if known_cols else "lineage_touch_col"
                statements.append(
                    f"MERGE INTO {rendered_target} USING {source_clause} ON 1=1 "
                    f"WHEN MATCHED THEN UPDATE SET {assign_col} = {assign_col};"
                )
            else:
                select_expr = ", ".join(known_cols) if known_cols else "*"
                statements.append(f"INSERT INTO {rendered_target} SELECT {select_expr} FROM {source_clause};")
    return normalize_and_dedupe_statements("\n".join(statements).strip(), ir)


def list_python_files(input_dir: Path) -> list[Path]:
    return sorted([p for p in input_dir.glob("*.py") if p.is_file()])


def resolve_input_files(cfg: RuntimeConfig) -> list[Path]:
    if cfg.file:
        if not cfg.file.exists() or not cfg.file.is_file():
            raise SystemExit(f"File does not exist: {cfg.file}")
        if cfg.file.suffix.lower() != ".py":
            raise SystemExit(f"Only .py files are supported: {cfg.file}")
        return [cfg.file]

    if not cfg.input_dir.exists() or not cfg.input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {cfg.input_dir}")
    files = list_python_files(cfg.input_dir)
    if len(files) > cfg.max_batch_files:
        raise SystemExit(
            f"Too many files in batch ({len(files)}). Max allowed is {cfg.max_batch_files}."
        )
    if not files:
        print(f"No .py files found in {cfg.input_dir}")
    return files


def _maybe_apply_compliance_pass_with_parity_gate(
    current_sql: str,
    cfg: RuntimeConfig,
    py_file: Path,
    ir: LineageIR,
    known_insert_cols: dict[str, list[str]],
    *,
    write_only: bool,
    add_partition_surrogate: bool,
    strip_partition_for_visual: bool,
) -> str:
    if not cfg.prompt_compliance_pass or not current_sql:
        return current_sql

    candidate_sql = apply_prompt_compliance_pass(
        current_sql,
        ir,
        known_insert_cols,
        no_where=cfg.compliance_no_where,
        compliance_profile=cfg.compliance_profile,
        write_only=write_only,
        add_partition_surrogate=add_partition_surrogate,
        strip_partition_for_visual=strip_partition_for_visual,
    )
    if cfg.compliance_profile != "baseline_sql_parity":
        return candidate_sql

    # SQL-first parity profile: only keep a structural transform if parity improves
    # against baseline; otherwise preserve the pre-transform SQL.
    before_report = compare_sql_with_baseline_dir(py_file.stem, cfg.baseline_sql_dir, current_sql)
    after_report = compare_sql_with_baseline_dir(py_file.stem, cfg.baseline_sql_dir, candidate_sql)
    if not before_report.baseline_found or not after_report.baseline_found:
        return candidate_sql
    if after_report.score >= before_report.score:
        return candidate_sql
    return current_sql


def run_agentic_pipeline(cfg: RuntimeConfig) -> int:
    logger = get_logger("parser.runner")
    if cfg.dialect != "tsql":
        raise SystemExit(f"Unsupported SQL dialect '{cfg.dialect}'. Only 'tsql' is supported.")
    if not cfg.prompt_file.exists() or not cfg.prompt_file.is_file():
        raise SystemExit(f"Prompt file does not exist: {cfg.prompt_file}")

    python_files = resolve_input_files(cfg)
    if not python_files:
        return 0

    base_sql_prompt = read_text_file(cfg.prompt_file)
    chunk_system_prompt = build_chunk_system_prompt(base_sql_prompt)
    refine_system_prompt = build_refine_system_prompt(base_sql_prompt)
    validation_system_prompt = build_validation_system_prompt(base_sql_prompt)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using model: {cfg.model}")
    print(f"SQL refine/repair model: {cfg.sql_repair_model or cfg.model}")
    print(f"SQL refine/repair endpoint: {cfg.sql_repair_base_url or cfg.base_url}")
    print(f"Endpoint: {cfg.base_url}")
    print(f"Found {len(python_files)} Python file(s).")
    print(f"Chunk lines/overlap: {cfg.chunk_lines}/{cfg.overlap_lines}")
    print(f"Prompt compliance pass: {'yes' if cfg.prompt_compliance_pass else 'no'}")
    print(f"Dialect lock: {cfg.dialect}")
    if cfg.prompt_compliance_pass:
        print(f"Compliance profile: {cfg.compliance_profile}")
        print(f"Compliance no-WHERE: {'yes' if cfg.compliance_no_where else 'no'}")
    print(f"Dry run: {'yes' if cfg.dry_run else 'no'}")
    logger.info("pipeline_start", extra={"extras": {"files": len(python_files), "model": cfg.model}})
    run_started = time.perf_counter()
    total_files = len(python_files)
    success_count = 0

    for file_idx, py_file in enumerate(python_files, start=1):
        file_started = time.perf_counter()
        pct = int((file_idx / max(1, total_files)) * 100)
        print(f"[{file_idx}/{total_files} | {pct}%] Starting {py_file.name}")
        sql_model = cfg.sql_repair_model or cfg.model
        sql_base_url = cfg.sql_repair_base_url or cfg.base_url
        code = read_text_file(py_file)
        if len(code.encode("utf-8")) > cfg.max_file_bytes:
            raise SystemExit(
                f"File exceeds max size {cfg.max_file_bytes} bytes: {py_file}"
            )
        ir = extract_lineage_ir(py_file, code)
        resolve_variables(ir)
        annotate_function_scopes(ir)
        units = build_work_units(code, ir, cfg.chunk_lines, cfg.overlap_lines)

        print(f"  extract: detected_ops={len(ir.operations)}, units={len(units)}")
        candidate_sql_parts: list[str] = []
        for idx, unit in enumerate(units, start=1):
            user_prompt = build_chunk_user_prompt(py_file.name, unit)
            unit_started = time.perf_counter()
            sql_raw = call_chat_completion(
                base_url=cfg.base_url,
                api_key=cfg.api_key,
                model=cfg.model,
                system_prompt=chunk_system_prompt,
                user_content=user_prompt,
                temperature=cfg.temperature,
                max_tokens=cfg.chunk_max_tokens,
                request_timeout=cfg.request_timeout,
                max_retries=cfg.max_model_retries,
            )
            sql = sanitize_sql_output(sql_raw)
            if sql and sql.upper() != "NO_SQL":
                candidate_sql_parts.append(sql)
            unit_elapsed = time.perf_counter() - unit_started
            print(f"  chunk: unit {idx}/{len(units)} done in {unit_elapsed:.1f}s")

        merged_candidate_sql = merge_and_deduplicate_sql(candidate_sql_parts)
        if merged_candidate_sql:
            refine_started = time.perf_counter()
            refine_user_prompt = build_refine_user_prompt(py_file.name, merged_candidate_sql)
            final_sql_raw = call_chat_completion(
                base_url=sql_base_url,
                api_key=cfg.api_key,
                model=sql_model,
                system_prompt=refine_system_prompt,
                user_content=refine_user_prompt,
                temperature=cfg.temperature,
                max_tokens=cfg.refine_max_tokens,
                request_timeout=cfg.request_timeout,
                max_retries=cfg.max_model_retries,
            )
            final_sql = sanitize_sql_output(final_sql_raw)
            final_sql = keep_insert_select_statements(final_sql)
            print(f"  refine: done in {time.perf_counter() - refine_started:.1f}s")
        else:
            final_sql = ""

        if final_sql:
            known_insert_cols = extract_known_insert_columns(ir)
            temp_columns = extract_known_temp_columns(ir)
            known_insert_cols.update(temp_columns)
            known_insert_cols.update(build_known_columns_from_temp_sources(ir, temp_columns))
            final_sql = substitute_known_variables(final_sql, ir.variables)
            if cfg.compliance_profile != "baseline_sql_parity":
                final_sql = enforce_known_columns(final_sql, known_insert_cols)
                filtered_sql = drop_hallucinated_and_synthetic(final_sql, ir)
                final_sql = filtered_sql
                if not final_sql:
                    final_sql = build_recovery_sql_from_ir(ir, known_insert_cols)
            final_sql = substitute_known_variables(final_sql, ir.variables)
            if cfg.compliance_profile != "baseline_sql_parity":
                final_sql = augment_missing_write_flows_from_ir(final_sql, ir, known_insert_cols)
                # Augmentation can introduce new SELECT * writes; enforce explicit columns again.
                final_sql = enforce_known_columns(final_sql, known_insert_cols)
                final_sql = reorder_statements_by_ir(final_sql, ir)
            if cfg.prompt_compliance_pass:
                final_sql = _maybe_apply_compliance_pass_with_parity_gate(
                    final_sql,
                    cfg,
                    py_file,
                    ir,
                    known_insert_cols,
                    write_only=True,
                    add_partition_surrogate=(cfg.compliance_profile == "baseline_parity"),
                    strip_partition_for_visual=(cfg.compliance_profile == "visual_parity"),
                )
                # Compliance shaping can drop malformed writes; recover missing write targets from IR evidence.
                if cfg.compliance_profile != "baseline_sql_parity":
                    final_sql = augment_missing_write_flows_from_ir(final_sql, ir, known_insert_cols)
                    final_sql = _maybe_apply_compliance_pass_with_parity_gate(
                        final_sql,
                        cfg,
                        py_file,
                        ir,
                        known_insert_cols,
                        write_only=True,
                        add_partition_surrogate=(cfg.compliance_profile == "baseline_parity"),
                        strip_partition_for_visual=(cfg.compliance_profile == "visual_parity"),
                    )
            else:
                final_sql = keep_write_statements_only(final_sql)
                final_sql = normalize_partition_clauses(final_sql)
                final_sql = normalize_and_dedupe_statements(final_sql, ir)

        validation = validate_sql(
            final_sql,
            ir,
            enforce_explicit_insert_columns=(cfg.compliance_profile != "baseline_sql_parity"),
            enforce_required_targets=(cfg.compliance_profile != "baseline_sql_parity"),
            enforce_evidence_sources=(cfg.compliance_profile != "baseline_sql_parity"),
        )
        best_sql = final_sql
        best_validation = validation
        best_score = _contract_score(final_sql, ir)
        retries = 0
        while (not best_validation.ok) and retries < cfg.max_validation_retries and best_sql:
            retries += 1
            retry_started = time.perf_counter()
            print(f"  validate: retry {retries}/{cfg.max_validation_retries}")
            repair_prompt = build_validation_user_prompt(py_file.name, best_sql, best_validation.errors)
            repaired_raw = call_chat_completion(
                base_url=sql_base_url,
                api_key=cfg.api_key,
                model=sql_model,
                system_prompt=validation_system_prompt,
                user_content=repair_prompt,
                temperature=cfg.temperature,
                max_tokens=cfg.refine_max_tokens,
                request_timeout=cfg.request_timeout,
                max_retries=cfg.max_model_retries,
            )
            repaired_sql = sanitize_sql_output(repaired_raw)
            repaired_sql = keep_insert_select_statements(repaired_sql)
            if repaired_sql:
                candidate_sql = repaired_sql
            else:
                candidate_sql = best_sql
            if candidate_sql:
                known_insert_cols = extract_known_insert_columns(ir)
                temp_columns = extract_known_temp_columns(ir)
                known_insert_cols.update(temp_columns)
                known_insert_cols.update(build_known_columns_from_temp_sources(ir, temp_columns))
                candidate_sql = substitute_known_variables(candidate_sql, ir.variables)
                if cfg.compliance_profile != "baseline_sql_parity":
                    candidate_sql = enforce_known_columns(candidate_sql, known_insert_cols)
                    filtered_sql = drop_hallucinated_and_synthetic(candidate_sql, ir)
                    candidate_sql = filtered_sql
                    if not candidate_sql:
                        candidate_sql = build_recovery_sql_from_ir(ir, known_insert_cols)
                candidate_sql = substitute_known_variables(candidate_sql, ir.variables)
                if cfg.compliance_profile != "baseline_sql_parity":
                    candidate_sql = augment_missing_write_flows_from_ir(candidate_sql, ir, known_insert_cols)
                    # Augmentation can introduce new SELECT * writes; enforce explicit columns again.
                    candidate_sql = enforce_known_columns(candidate_sql, known_insert_cols)
                    candidate_sql = reorder_statements_by_ir(candidate_sql, ir)
                if cfg.prompt_compliance_pass:
                    candidate_sql = _maybe_apply_compliance_pass_with_parity_gate(
                        candidate_sql,
                        cfg,
                        py_file,
                        ir,
                        known_insert_cols,
                        write_only=True,
                        add_partition_surrogate=(cfg.compliance_profile == "baseline_parity"),
                        strip_partition_for_visual=(cfg.compliance_profile == "visual_parity"),
                    )
                    if cfg.compliance_profile != "baseline_sql_parity":
                        candidate_sql = augment_missing_write_flows_from_ir(candidate_sql, ir, known_insert_cols)
                        candidate_sql = _maybe_apply_compliance_pass_with_parity_gate(
                            candidate_sql,
                            cfg,
                            py_file,
                            ir,
                            known_insert_cols,
                            write_only=True,
                            add_partition_surrogate=(cfg.compliance_profile == "baseline_parity"),
                            strip_partition_for_visual=(cfg.compliance_profile == "visual_parity"),
                        )
                else:
                    candidate_sql = keep_write_statements_only(candidate_sql)
                    candidate_sql = normalize_partition_clauses(candidate_sql)
                    candidate_sql = normalize_and_dedupe_statements(candidate_sql, ir)
            candidate_validation = validate_sql(
                candidate_sql,
                ir,
                enforce_explicit_insert_columns=(cfg.compliance_profile != "baseline_sql_parity"),
                enforce_required_targets=(cfg.compliance_profile != "baseline_sql_parity"),
                enforce_evidence_sources=(cfg.compliance_profile != "baseline_sql_parity"),
            )
            candidate_score = _contract_score(candidate_sql, ir)
            should_take = False
            if candidate_validation.ok and not best_validation.ok:
                should_take = True
            elif candidate_score > best_score and len(candidate_validation.errors) <= len(best_validation.errors):
                should_take = True
            elif candidate_score == best_score and len(candidate_validation.errors) < len(best_validation.errors):
                should_take = True
            if should_take:
                best_sql = candidate_sql
                best_validation = candidate_validation
                best_score = candidate_score
            print(f"  validate: retry {retries} done in {time.perf_counter() - retry_started:.1f}s")
        final_sql = best_sql
        validation = best_validation

        if cfg.strict_validation and not validation.ok:
            raise ValidationFailure(
                f"Validation failed for {py_file.name}: {', '.join(validation.errors)}"
            )

        # Final safety pass for unresolved python variable tokens.
        final_sql = substitute_known_variables(final_sql, ir.variables)
        if cfg.dialect == "tsql":
            final_sql = fix_missing_window_by_spacing(final_sql)
            final_sql = normalize_tsql_placeholders(final_sql)
        # Final safety pass for column fidelity after any downstream rewrites.
        if final_sql:
            known_insert_cols = extract_known_insert_columns(ir)
            temp_columns = extract_known_temp_columns(ir)
            known_insert_cols.update(temp_columns)
            known_insert_cols.update(build_known_columns_from_temp_sources(ir, temp_columns))
            if cfg.prompt_compliance_pass:
                final_sql = _maybe_apply_compliance_pass_with_parity_gate(
                    final_sql,
                    cfg,
                    py_file,
                    ir,
                    known_insert_cols,
                    write_only=True,
                    add_partition_surrogate=(cfg.compliance_profile == "baseline_parity"),
                    strip_partition_for_visual=(cfg.compliance_profile == "visual_parity"),
                )
            else:
                if cfg.compliance_profile != "baseline_sql_parity":
                    final_sql = enforce_known_columns(final_sql, known_insert_cols)
                    final_sql = normalize_and_dedupe_statements(final_sql, ir)

        if not cfg.dry_run:
            out_file = cfg.output_dir / f"{py_file.stem}.sql"
            out_file.write_text((final_sql + "\n") if final_sql else "", encoding="utf-8")
            print(f"Wrote: {out_file}")
        else:
            print(f"Dry-run: skipped writing SQL for {py_file.name}")

        baseline_parity = compare_sql_with_baseline_dir(py_file.stem, cfg.baseline_sql_dir, final_sql or "")
        if cfg.compliance_profile == "baseline_sql_parity" and baseline_parity.baseline_found and not all_parity_gates_pass(
            baseline_parity
        ):
            validation.ok = False
            validation.errors.append(
                f"Baseline parity gate failed for {py_file.stem}.sql: score={baseline_parity.score:.3f}"
            )
        parity_report_path = cfg.report_dir / f"{py_file.stem}.parity.json"
        parity_report_path.write_text(json.dumps(baseline_parity.to_dict(), indent=2) + "\n", encoding="utf-8")
        print(f"Wrote: {parity_report_path}")

        report = FileReport(
            file_path=str(py_file),
            unit_count=len(units),
            candidate_sql_count=len(candidate_sql_parts),
            validation_ok=validation.ok,
            validation_errors=validation.errors,
            retries=retries,
            statement_count=len(split_sql_statements(final_sql)) if final_sql else 0,
            metadata={
                "model": cfg.model,
                "sql_repair_model": sql_model,
                "sql_repair_base_url": sql_base_url,
                "chunk_max_tokens": cfg.chunk_max_tokens,
                "refine_max_tokens": cfg.refine_max_tokens,
                "dry_run": cfg.dry_run,
                "detected_ops": len(ir.operations),
                "baseline_parity": baseline_parity.to_dict(),
            },
        )
        report_path = write_file_report(cfg.report_dir, py_file.stem, report)
        print(f"Wrote: {report_path}")
        if validation.ok:
            success_count += 1
        file_elapsed = time.perf_counter() - file_started
        print(
            f"[{file_idx}/{total_files}] Completed {py_file.name} in {file_elapsed:.1f}s "
            f"(validation_ok={validation.ok}, statements={report.statement_count})"
        )
        logger.info(
            "file_complete",
            extra={
                "extras": {
                    "file": py_file.name,
                    "validation_ok": validation.ok,
                    "statements": report.statement_count,
                    "elapsed_s": round(file_elapsed, 2),
                }
            },
        )

    run_elapsed = time.perf_counter() - run_started
    print(
        f"Run complete: {success_count}/{total_files} files validated in "
        f"{run_elapsed:.1f}s"
    )
    logger.info(
        "pipeline_complete",
        extra={
            "extras": {
                "validated_files": success_count,
                "total_files": total_files,
                "elapsed_s": round(run_elapsed, 2),
            }
        },
    )
    return 0

