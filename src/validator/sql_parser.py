def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for char in sql_text:
        current.append(char)
        if char == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements

