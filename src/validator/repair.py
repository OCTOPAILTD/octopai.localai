import re
from typing import Optional, Tuple

from src.ir.models import LineageIR
from src.validator.sql_parser import split_sql_statements


_CLAUSE_MARKERS = (
    " GROUP BY ",
    " HAVING ",
    " ORDER BY ",
    " UNION ",
    " INTERSECT ",
    " EXCEPT ",
    " LIMIT ",
)


def _normalize_asset(asset: str) -> str:
    return asset.strip().strip("\"'").lower()


def _strip_sql_comments(statement: str) -> str:
    without_line = re.sub(r"--.*?$", "", statement, flags=re.MULTILINE)
    return re.sub(r"/\*.*?\*/", "", without_line, flags=re.DOTALL)


def _is_token_at(text: str, idx: int, token: str) -> bool:
    end = idx + len(token)
    if end > len(text):
        return False
    if text[idx:end] != token:
        return False
    prev = text[idx - 1] if idx > 0 else " "
    nxt = text[end] if end < len(text) else " "
    return (not (prev.isalnum() or prev == "_")) and (not (nxt.isalnum() or nxt == "_"))


def _find_where_range(statement: str) -> Optional[Tuple[int, int]]:
    upper = statement.upper()
    in_single = False
    in_double = False
    depth = 0

    where_start = -1
    where_depth = 0
    i = 0
    while i < len(statement):
        ch = statement[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif _is_token_at(upper, i, "WHERE"):
                where_start = i
                where_depth = depth
                break
        i += 1

    if where_start < 0:
        return None

    where_end = len(statement)
    j = where_start + 7
    while j < len(statement):
        ch = statement[j]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            else:
                if depth < where_depth:
                    return where_start, j
                for marker in _CLAUSE_MARKERS:
                    if depth == where_depth and upper.startswith(marker, j):
                        where_end = j
                        return where_start, where_end
        j += 1

    return where_start, where_end


def contains_forbidden_where(statement: str) -> bool:
    cleaned = _strip_sql_comments(statement).upper()
    for i in range(len(cleaned)):
        if _is_token_at(cleaned, i, "WHERE"):
            return True
    return False


def remove_forbidden_where_clauses(sql_text: str) -> str:
    repaired: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = _strip_sql_comments(stmt).strip()
        if not current:
            continue
        # Keep UPDATE/MERGE predicates: they often carry lineage-driving source references.
        upper = current.upper()
        if upper.startswith("UPDATE ") or upper.startswith("MERGE INTO "):
            if not current.endswith(";"):
                current += ";"
            repaired.append(current)
            continue
        while True:
            where_range = _find_where_range(current)
            if where_range is None:
                break
            start, end = where_range
            current = (current[:start] + current[end:]).strip()
        if not current.endswith(";"):
            current += ";"
        repaired.append(current)
    return "\n".join(repaired).strip()


def _extract_statement_assets(statement: str) -> set[str]:
    cleaned = _strip_sql_comments(statement)
    assets: set[str] = set()
    patterns = (
        r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bUPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bMERGE\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bFROM\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bJOIN\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
    )
    for pattern in patterns:
        for match in re.findall(pattern, cleaned, flags=re.IGNORECASE):
            assets.add(_normalize_asset(match))
    return assets


def _extract_temp_assets(statement: str) -> set[str]:
    cleaned = _strip_sql_comments(statement)
    temps = re.findall(r"(#[_A-Za-z0-9]+)", cleaned)
    return {_normalize_asset(t) for t in temps}


def _extract_target_assets(statement: str) -> set[str]:
    cleaned = _strip_sql_comments(statement)
    assets: set[str] = set()
    patterns = (
        r"\bINSERT\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bUPDATE\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        r"\bMERGE\s+INTO\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
    )
    for pattern in patterns:
        for match in re.findall(pattern, cleaned, flags=re.IGNORECASE):
            assets.add(_normalize_asset(match))
    return assets


def _is_synthetic_temp_chain(statement: str) -> bool:
    cleaned = _strip_sql_comments(statement)
    tgt_match = re.search(r"\bINSERT\s+INTO\s+(#temp_table_\d+)\b", cleaned, flags=re.IGNORECASE)
    src_match = re.search(r"\bFROM\s+(#temp_table_\d+)\b", cleaned, flags=re.IGNORECASE)
    if not tgt_match:
        return False
    if src_match:
        return True
    return False


def drop_hallucinated_and_synthetic(sql_text: str, ir: LineageIR) -> str:
    allowed_assets = {
        _normalize_asset(asset)
        for op in ir.operations
        for asset in (op.source_assets + op.target_assets)
        if asset
    }
    allowed_assets.update(
        _normalize_asset(value)
        for value in ir.variables.values()
        if isinstance(value, str) and value.strip()
    )
    # Always allow temp objects and unresolved placeholders.
    filtered: list[str] = []
    for stmt in split_sql_statements(sql_text):
        current = stmt.strip()
        if not current:
            continue
        if _is_synthetic_temp_chain(current):
            continue

        if "hive_metastore.db.table" in current.lower():
            continue

        temp_assets = _extract_temp_assets(current)
        if temp_assets:
            disallowed = [
                t
                for t in temp_assets
                if t not in {x.lower() for x in ir.allowed_temp_assets}
            ]
            if disallowed:
                continue

        target_assets = _extract_target_assets(current)
        if target_assets:
            unknown_targets = [
                asset
                for asset in target_assets
                if not asset.startswith("#")
                and "{" not in asset
                and asset not in allowed_assets
            ]
            if len(unknown_targets) == len(target_assets):
                continue

        stmt_assets = _extract_statement_assets(current)
        unknown_assets = [
            asset
            for asset in stmt_assets
            if not asset.startswith("#")
            and "{" not in asset
            and asset not in allowed_assets
        ]
        if unknown_assets and not target_assets:
            continue
        filtered.append(current if current.endswith(";") else f"{current};")
    return "\n".join(filtered).strip()

