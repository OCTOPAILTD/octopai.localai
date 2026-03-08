from src.planner.unitizer import WorkUnit


def build_chunk_system_prompt(base_sql_prompt: str) -> str:
    return (
        base_sql_prompt
        + "\n\nAdditional instructions for chunk mode:\n"
        + "- Only emit SQL supported by evidence in the provided chunk.\n"
        + "- If none, output exactly NO_SQL.\n"
    )


def build_refine_system_prompt(base_sql_prompt: str) -> str:
    return (
        base_sql_prompt
        + "\n\nAdditional instructions for refinement mode:\n"
        + "- Consolidate and deduplicate candidate SQL.\n"
        + "- Output SQL statements only.\n"
    )


def build_validation_system_prompt(base_sql_prompt: str) -> str:
    return (
        base_sql_prompt
        + "\n\nAdditional instructions for validation repair mode:\n"
        + "- Repair invalid SQL statements while preserving lineage semantics.\n"
        + "- Do not introduce new sources or targets not present in candidate SQL.\n"
        + "- Output SQL only.\n"
    )


def build_chunk_user_prompt(file_name: str, unit: WorkUnit) -> str:
    return (
        f"File: {file_name}\n"
        f"Unit: {unit.unit_id}\n"
        f"Line range: {unit.start_line}-{unit.end_line}\n"
        f"Related op ids: {', '.join(unit.op_ids) if unit.op_ids else 'none'}\n\n"
        "Generate SQL lineage only for flows evidenced in this unit.\n"
        "If this unit has no read/write lineage operations, output exactly: NO_SQL\n\n"
        "Python unit code:\n"
        f"{unit.context}"
    )


def build_refine_user_prompt(file_name: str, candidate_sql: str) -> str:
    return (
        f"File: {file_name}\n\n"
        "Consolidate the SQL below into a final result.\n"
        "Requirements:\n"
        "- Keep only valid SQL statements.\n"
        "- Remove duplicates.\n"
        "- Keep logical order where possible.\n"
        "- Output SQL only.\n\n"
        "Candidate SQL:\n"
        f"{candidate_sql}"
    )


def build_validation_user_prompt(file_name: str, candidate_sql: str, validation_errors: list[str]) -> str:
    errors = "\n".join(f"- {err}" for err in validation_errors)
    return (
        f"File: {file_name}\n\n"
        "Repair the SQL so that it passes validation checks.\n"
        "Validation failures:\n"
        f"{errors}\n\n"
        "Candidate SQL:\n"
        f"{candidate_sql}"
    )

