import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ir.models import CodeSpan, LineageIR, LineageOp
from src.validator.rules import validate_sql


class ValidatorRulesTest(unittest.TestCase):
    def test_minimal_golden_output_passes(self) -> None:
        golden_dir = ROOT / "tests" / "golden"
        sql = (golden_dir / "minimal_expected.sql").read_text(encoding="utf-8")
        sql = sql.replace("SELECT * FROM", "SELECT id FROM")
        ir_payload = json.loads((golden_dir / "minimal_ir.json").read_text(encoding="utf-8"))

        op_payload = ir_payload["operations"][0]
        ir = LineageIR(
            file_path=ir_payload["file_path"],
            operations=[
                LineageOp(
                    op_id=op_payload["op_id"],
                    op_type=op_payload["op_type"],
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="df.write.saveAsTable('target_table')",
                    target_assets=op_payload["target_assets"],
                )
            ],
        )

        result = validate_sql(sql, ir)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_update_statement_is_accepted(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="hwc.executeUpdate(update_query)",
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "UPDATE target_tbl SET id = id FROM src_tbl;"
        result = validate_sql(sql, ir)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_temp_target_matches_without_hash(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                )
            ],
        )
        sql = "INSERT INTO temp_table SELECT * FROM src_tbl;"
        result = validate_sql(sql, ir)
        self.assertFalse(result.ok)
        self.assertTrue(any("SELECT *" in err for err in result.errors))

    def test_insert_select_star_is_rejected(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT * FROM src_tbl;"
        result = validate_sql(sql, ir)
        self.assertFalse(result.ok)
        self.assertTrue(any("SELECT *" in err for err in result.errors))

    def test_insert_select_star_allowed_when_rule_disabled(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT * FROM src_tbl;"
        result = validate_sql(sql, ir, enforce_explicit_insert_columns=False)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_nested_select_star_not_flagged_when_outer_explicit(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT a FROM (SELECT * FROM src_tbl) s;"
        result = validate_sql(sql, ir)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_missing_required_targets_can_be_disabled(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO other_tbl SELECT id FROM src_tbl;"
        result = validate_sql(sql, ir, enforce_required_targets=False)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_non_evidenced_source_is_rejected(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    source_assets=["src_tbl"],
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT id FROM madeup.tbl;"
        result = validate_sql(sql, ir, enforce_explicit_insert_columns=False)
        self.assertFalse(result.ok)
        self.assertTrue(any("non-evidenced sources" in err.lower() for err in result.errors))

    def test_non_evidenced_source_check_can_be_disabled(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    source_assets=["src_tbl"],
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT id FROM madeup.tbl;"
        result = validate_sql(
            sql,
            ir,
            enforce_explicit_insert_columns=False,
            enforce_evidence_sources=False,
        )
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")

    def test_evidenced_source_with_alias_is_accepted(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(start_line=1, end_line=1),
                    raw_line="spark.sql(write_query)",
                    source_assets=["db.source_table AS s"],
                    target_assets=["target_tbl"],
                )
            ],
        )
        sql = "INSERT INTO target_tbl SELECT id FROM db.source_table s;"
        result = validate_sql(sql, ir, enforce_explicit_insert_columns=False)
        self.assertTrue(result.ok, msg=f"Validation failed: {result.errors}")


if __name__ == "__main__":
    unittest.main()

