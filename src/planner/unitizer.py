from dataclasses import dataclass

from src.ir.models import LineageIR


@dataclass
class WorkUnit:
    unit_id: str
    start_line: int
    end_line: int
    context: str
    op_ids: list[str]


def split_code_chunks(code: str, chunk_lines: int, overlap_lines: int) -> list[tuple[int, int, str]]:
    lines = code.splitlines()
    if not lines:
        return []
    chunks: list[tuple[int, int, str]] = []
    start = 0
    total = len(lines)
    step = max(1, chunk_lines - overlap_lines)
    while start < total:
        end = min(total, start + chunk_lines)
        chunks.append((start + 1, end, "\n".join(lines[start:end])))
        if end >= total:
            break
        start += step
    return chunks


def build_work_units(code: str, ir: LineageIR, chunk_lines: int, overlap_lines: int) -> list[WorkUnit]:
    chunks = split_code_chunks(code, chunk_lines=chunk_lines, overlap_lines=overlap_lines)
    units: list[WorkUnit] = []
    for idx, (start, end, chunk_text) in enumerate(chunks, start=1):
        op_ids = [
            op.op_id
            for op in ir.operations
            if op.code_span.start_line <= end and op.code_span.end_line >= start
        ]
        units.append(
            WorkUnit(
                unit_id=f"unit_{idx}",
                start_line=start,
                end_line=end,
                context=chunk_text,
                op_ids=op_ids,
            )
        )
    return units

