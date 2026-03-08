from src.validator.sql_parser import split_sql_statements


def merge_and_deduplicate_sql(parts: list[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        for stmt in split_sql_statements(part):
            normalized = " ".join(stmt.split()).strip().lower()
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(stmt if stmt.endswith(";") else f"{stmt};")
    return "\n".join(ordered).strip()

