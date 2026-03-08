from src.ir.models import LineageIR


def annotate_function_scopes(ir: LineageIR) -> None:
    function_names = set(ir.call_graph.keys())
    for op in ir.operations:
        scope = None
        for fn in function_names:
            if fn in op.raw_line:
                scope = fn
                break
        op.function_scope = scope

