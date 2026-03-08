import re

from src.ir.models import LineageIR

_VAR_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def resolve_variables(ir: LineageIR) -> None:
    for op in ir.operations:
        resolved_sources: list[str] = []
        unresolved: list[str] = []

        for src in op.source_assets:
            replaced = src
            for match in _VAR_PATTERN.findall(src):
                value = ir.variables.get(match)
                if value is None:
                    unresolved.append(match)
                else:
                    replaced = replaced.replace(f"{{{match}}}", value)
                    op.resolved_values[match] = value
            resolved_sources.append(replaced)

        resolved_targets: list[str] = []
        for tgt in op.target_assets:
            replaced = tgt
            for match in _VAR_PATTERN.findall(tgt):
                value = ir.variables.get(match)
                if value is None:
                    unresolved.append(match)
                else:
                    replaced = replaced.replace(f"{{{match}}}", value)
                    op.resolved_values[match] = value
            resolved_targets.append(replaced)

        op.source_assets = resolved_sources
        op.target_assets = resolved_targets
        op.unresolved_placeholders = sorted(set(unresolved))

