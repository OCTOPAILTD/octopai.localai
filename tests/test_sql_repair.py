from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ir.models import CodeSpan, LineageIR, LineageOp
from src.validator.repair import (
    contains_forbidden_where,
    drop_hallucinated_and_synthetic,
    remove_forbidden_where_clauses,
)


class SqlRepairTest(unittest.TestCase):
    def test_removes_top_level_where(self) -> None:
        sql = "INSERT INTO t SELECT a FROM src WHERE a > 10 ORDER BY a;"
        repaired = remove_forbidden_where_clauses(sql)
        self.assertNotIn(" WHERE ", repaired.upper())
        self.assertIn(" ORDER BY ", repaired.upper())

    def test_removes_nested_where(self) -> None:
        sql = "INSERT INTO t SELECT * FROM (SELECT * FROM src WHERE x = 1) s;"
        repaired = remove_forbidden_where_clauses(sql)
        self.assertNotIn(" WHERE ", repaired.upper())
        self.assertIn("FROM (SELECT * FROM SRC", repaired.upper())

    def test_detects_where_on_new_line(self) -> None:
        stmt = "INSERT INTO t SELECT * FROM src\nWHERE x = 1;"
        self.assertTrue(contains_forbidden_where(stmt))

    def test_where_removal_does_not_corrupt_sql_text(self) -> None:
        sql = "INSERT INTO t SELECT * FROM src WHERE as_of_date = '{TANGGAL_POSISI}';"
        repaired = remove_forbidden_where_clauses(sql)
        self.assertIn("INSERT INTO T SELECT * FROM SRC;", repaired.upper())
        self.assertNotIn("AS_OSISI", repaired)
        self.assertNotIn("WHERE", repaired.upper())

    def test_where_removal_keeps_update_predicates(self) -> None:
        sql = "UPDATE tgt SET end_date = x WHERE id IN (SELECT id FROM stg WHERE flag = 'U');"
        repaired = remove_forbidden_where_clauses(sql)
        self.assertIn("UPDATE TGT SET END_DATE = X WHERE", repaired.upper())
        self.assertIn("FROM STG WHERE FLAG = 'U'", repaired.upper())

    def test_drops_hallucinated_assets_and_temp_chains(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(write_query)",
                    source_assets=["acl_datalake.mart_cta"],
                    target_assets=["prd_acl_datalake.master_cta"],
                )
            ],
        )
        sql = "\n".join(
            [
                "INSERT INTO #temp_table_2 SELECT * FROM #temp_table_1;",
                "INSERT INTO #temp_table SELECT * FROM hive_metastore.db.table;",
                "INSERT INTO prd_acl_datalake.master_cta SELECT * FROM acl_datalake.mart_cta;",
            ]
        )
        repaired = drop_hallucinated_and_synthetic(sql, ir)
        self.assertNotIn("hive_metastore.db.table", repaired.lower())
        self.assertNotIn("#temp_table_2", repaired.lower())
        self.assertIn("prd_acl_datalake.master_cta", repaired.lower())


if __name__ == "__main__":
    unittest.main()

