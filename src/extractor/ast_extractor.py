import ast
from pathlib import Path
import re
from typing import Any, Optional

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - runtime dependency guard
    sqlglot = None
    exp = None

from src.extractor.patterns import READ_PATTERNS, WRITE_PATTERNS
from src.ir.models import CodeSpan, LineageIR, LineageOp


def _line_is_read(line: str) -> bool:
    return any(pattern in line for pattern in READ_PATTERNS)


def _line_is_write(line: str) -> bool:
    return any(pattern in line for pattern in WRITE_PATTERNS)


def _extract_string_literals(node: ast.AST) -> list[str]:
    literals: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            literals.append(child.value)
    return literals


def _render_string_expr(node: ast.AST, variables: dict[str, str]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return variables.get(node.id)
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                if isinstance(value.value, ast.Name):
                    parts.append(f"{{{value.value.id}}}")
                else:
                    rendered = ast.unparse(value.value).strip()
                    parts.append("{" + rendered + "}")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render_string_expr(node.left, variables)
        right = _render_string_expr(node.right, variables)
        if left is not None and right is not None:
            return left + right
    return None


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        parts: list[str] = []
        current: ast.AST = func
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    return ""


def _extract_sql_assets(sql_text: str) -> tuple[list[str], list[str]]:
    src_matches = re.findall(
        r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        sql_text,
        flags=re.IGNORECASE,
    )
    tgt_matches = re.findall(
        r"\b(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)\s+([A-Za-z0-9_#\".`:/\\\-\{\}]+)",
        sql_text,
        flags=re.IGNORECASE,
    )
    source_assets = [m.strip() for m in src_matches if m.strip()]
    target_assets = [m.strip() for m in tgt_matches if m.strip()]
    return source_assets, target_assets


def _asset_sql(node: Any) -> str:
    rendered = node.sql(dialect="hive")
    return rendered.strip().strip("\"'")


def _preprocess_sql_for_parse(sql_text: str) -> str:
    # Keep placeholders parseable for SQL AST (`{VAR}` -> `VAR`)
    normalized = re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", r"\1", sql_text)
    return normalized.strip().rstrip(";")


def _extract_select_items(select_expr: Any) -> list[dict[str, str]]:
    if exp is None:
        return []
    select_items: list[dict[str, str]] = []
    if not isinstance(select_expr, exp.Select):
        return select_items
    for projection in select_expr.expressions:
        alias = projection.alias_or_name or ""
        select_items.append(
            {
                "expression": projection.sql(dialect="hive").strip(),
                "alias": alias.strip(),
            }
        )
    return select_items


def _extract_sql_details(sql_text: str) -> dict[str, Any]:
    source_assets, target_assets = _extract_sql_assets(sql_text)
    details: dict[str, Any] = {
        "statement_kind": "",
        "source_assets": source_assets,
        "target_assets": target_assets,
        "select_items": [],
        "join_predicates": [],
    }
    if not sql_text or sqlglot is None or exp is None:
        return details

    parsed = None
    cleaned = _preprocess_sql_for_parse(sql_text)
    if not cleaned:
        return details
    try:
        parsed = sqlglot.parse_one(cleaned, read="hive")
    except Exception:
        return details

    if isinstance(parsed, exp.Insert):
        details["statement_kind"] = "insert"
        if isinstance(parsed.this, exp.Table):
            details["target_assets"] = [_asset_sql(parsed.this)]
        source_tables = [_asset_sql(t) for t in parsed.find_all(exp.Table)]
        if details["target_assets"]:
            target_norm = details["target_assets"][0].lower()
            source_tables = [s for s in source_tables if s.lower() != target_norm]
        details["source_assets"] = source_tables
        select_node = parsed.find(exp.Select)
        details["select_items"] = _extract_select_items(select_node)

    elif isinstance(parsed, exp.Update):
        details["statement_kind"] = "update"
        if isinstance(parsed.this, exp.Table):
            details["target_assets"] = [_asset_sql(parsed.this)]
        source_tables = []
        from_expr = parsed.args.get("from")
        if from_expr:
            for table in from_expr.find_all(exp.Table):
                source_tables.append(_asset_sql(table))
        for join in parsed.args.get("joins") or []:
            for table in join.find_all(exp.Table):
                source_tables.append(_asset_sql(table))
            on_expr = join.args.get("on")
            if on_expr:
                details["join_predicates"].append(on_expr.sql(dialect="hive").strip())
        # Updates can reference sources only inside subqueries in WHERE/SET clauses.
        # Collect all table refs and exclude the update target itself.
        target_norm = ""
        if details["target_assets"]:
            target_norm = details["target_assets"][0].lower()
        all_tables = [_asset_sql(t) for t in parsed.find_all(exp.Table)]
        for table in all_tables:
            if target_norm and table.lower() == target_norm:
                continue
            source_tables.append(table)
        # Keep stable order while removing duplicates.
        seen: set[str] = set()
        deduped: list[str] = []
        for src in source_tables:
            key = src.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(src)
        details["source_assets"] = deduped

    elif isinstance(parsed, exp.Merge):
        details["statement_kind"] = "merge"
        if isinstance(parsed.this, exp.Table):
            details["target_assets"] = [_asset_sql(parsed.this)]
        using_expr = parsed.args.get("using")
        source_tables = []
        if using_expr:
            for table in using_expr.find_all(exp.Table):
                source_tables.append(_asset_sql(table))
        details["source_assets"] = source_tables
        on_expr = parsed.args.get("on")
        if on_expr:
            details["join_predicates"].append(on_expr.sql(dialect="hive").strip())

    elif isinstance(parsed, exp.Select):
        details["statement_kind"] = "select"
        details["source_assets"] = [_asset_sql(t) for t in parsed.find_all(exp.Table)]
        details["select_items"] = _extract_select_items(parsed)
        for join in parsed.args.get("joins") or []:
            on_expr = join.args.get("on")
            if on_expr:
                details["join_predicates"].append(on_expr.sql(dialect="hive").strip())

    else:
        head = cleaned.upper().strip()
        if head.startswith("INSERT "):
            details["statement_kind"] = "insert"
        elif head.startswith("UPDATE "):
            details["statement_kind"] = "update"
        elif head.startswith("MERGE "):
            details["statement_kind"] = "merge"
        elif head.startswith("SELECT "):
            details["statement_kind"] = "select"

    return details


def _infer_op_type(call_name: str, call: ast.Call, variables: dict[str, str]) -> Optional[str]:
    lowered = call_name.lower()
    if lowered.endswith("createorreplacetempview"):
        return "write"
    if lowered.endswith("executeupdate"):
        return "write"
    if lowered.endswith("save") or lowered.endswith("saveastable") or lowered.endswith("insertinto"):
        return "write"

    if lowered.endswith("sql"):
        sql_text = ""
        if call.args:
            maybe_sql = _render_string_expr(call.args[0], variables)
            if maybe_sql:
                sql_text = maybe_sql.strip().upper()
        if sql_text.startswith(("INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE ", "DROP ", "ALTER ")):
            return "write"
        return "read"

    line_hint = call_name + " " + " ".join(_extract_string_literals(call))
    if _line_is_read(line_hint):
        return "read"
    if _line_is_write(line_hint):
        return "write"
    return None


def extract_lineage_ir(file_path: Path, code: str) -> LineageIR:
    tree = ast.parse(code)
    ir = LineageIR(file_path=str(file_path))

    # First pass: resolve string-like variables for later SQL call interpretation.
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    rendered = _render_string_expr(node.value, ir.variables)
                    if rendered:
                        ir.variables[target.id] = rendered

        if isinstance(node, ast.FunctionDef):
            called: list[str] = []
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Name):
                    called.append(child.func.id)
            ir.call_graph[node.name] = sorted(set(called))

    # Second pass: detect read/write calls in source order.
    call_nodes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node, "lineno", 0) > 0
    ]
    call_nodes.sort(key=lambda n: (getattr(n, "lineno", 0), getattr(n, "end_lineno", 0)))

    for node in call_nodes:

        lineno = getattr(node, "lineno", 0)
        end_lineno = getattr(node, "end_lineno", lineno)

        call_name = _call_name(node)
        op_type = _infer_op_type(call_name, node, ir.variables)
        if not op_type:
            continue

        sql_text = ""
        if node.args:
            maybe_sql = _render_string_expr(node.args[0], ir.variables)
            if maybe_sql:
                sql_text = maybe_sql
        sql_details = _extract_sql_details(sql_text) if sql_text else {
            "statement_kind": "",
            "source_assets": [],
            "target_assets": [],
            "select_items": [],
            "join_predicates": [],
        }
        source_assets = list(sql_details.get("source_assets", []))
        target_assets = list(sql_details.get("target_assets", []))

        if call_name.lower().endswith("createorreplacetempview") and node.args:
            temp_name = _render_string_expr(node.args[0], ir.variables)
            if temp_name:
                temp_asset = f"#{temp_name}"
                target_assets = [temp_asset]
                ir.allowed_temp_assets.add(temp_asset.lower())
                sql_details["statement_kind"] = "create_temp_view"

        raw_segment = ast.get_source_segment(code, node) or call_name
        raw_line = raw_segment.strip().splitlines()[0]
        op = LineageOp(
            op_id=f"op_{len(ir.operations) + 1}",
            op_type=op_type,
            code_span=CodeSpan(start_line=lineno, end_line=end_lineno),
            raw_line=raw_line,
            source_assets=source_assets,
            target_assets=target_assets,
            metadata={
                "call_name": call_name,
                "sql_text": sql_text,
                "statement_kind": sql_details.get("statement_kind", ""),
                "select_items": sql_details.get("select_items", []),
                "join_predicates": sql_details.get("join_predicates", []),
            },
        )
        ir.operations.append(op)

    return ir

