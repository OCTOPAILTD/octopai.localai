from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CodeSpan:
    start_line: int
    end_line: int


@dataclass
class LineageOp:
    op_id: str
    op_type: str
    code_span: CodeSpan
    raw_line: str
    source_assets: list[str] = field(default_factory=list)
    target_assets: list[str] = field(default_factory=list)
    function_scope: Optional[str] = None
    loop_context: Optional[str] = None
    resolved_values: dict[str, str] = field(default_factory=dict)
    unresolved_placeholders: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageIR:
    file_path: str
    operations: list[LineageOp] = field(default_factory=list)
    variables: dict[str, str] = field(default_factory=dict)
    call_graph: dict[str, list[str]] = field(default_factory=dict)
    allowed_temp_assets: set[str] = field(default_factory=set)

