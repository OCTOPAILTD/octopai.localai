from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baseline_parity import compare_sql_to_baseline, compare_sql_with_baseline_dir, all_parity_gates_pass


class BaselineParityTest(unittest.TestCase):
    def test_compare_sql_to_baseline_full_match(self) -> None:
        baseline = (
            "INSERT INTO #temp_table SELECT a, b FROM src;\n"
            "INSERT INTO tgt SELECT * FROM #temp_table;"
        )
        generated = (
            "INSERT INTO #temp_table SELECT a, b FROM src;\n"
            "INSERT INTO tgt SELECT * FROM #temp_table;"
        )
        report = compare_sql_to_baseline("x.sql", baseline, generated)
        self.assertTrue(report.baseline_found)
        self.assertEqual(report.score, 1.0)
        self.assertTrue(all_parity_gates_pass(report))
        self.assertTrue(report.statement_count_match)
        self.assertTrue(report.statement_order_match)
        self.assertTrue(report.target_sequence_match)
        self.assertTrue(report.projection_policy_match)
        self.assertTrue(report.clause_shape_match)

    def test_compare_sql_to_baseline_detects_mismatch(self) -> None:
        baseline = (
            "INSERT INTO #temp_table SELECT a, b FROM src;\n"
            "INSERT INTO tgt SELECT * FROM #temp_table;"
        )
        generated = (
            "INSERT INTO tgt SELECT a, b FROM src;\n"
            "INSERT INTO #temp_table SELECT a, b FROM src;"
        )
        report = compare_sql_to_baseline("x.sql", baseline, generated)
        self.assertLess(report.score, 1.0)
        self.assertFalse(report.target_sequence_match)
        self.assertFalse(all_parity_gates_pass(report))

    def test_compare_sql_with_missing_baseline_file(self) -> None:
        report = compare_sql_with_baseline_dir("nope", ROOT / "tests" / "missing", "INSERT INTO t SELECT a FROM s;")
        self.assertFalse(report.baseline_found)
        self.assertEqual(report.score, 0.0)

    def test_compare_sql_with_baseline_dir_reads_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            baseline_path = Path(tmp) / "etl_job.sql"
            baseline_path.write_text("INSERT INTO t SELECT a FROM s;\n", encoding="utf-8")
            report = compare_sql_with_baseline_dir("etl_job", Path(tmp), "INSERT INTO t SELECT a FROM s;")
            self.assertTrue(report.baseline_found)
            self.assertEqual(report.score, 1.0)


if __name__ == "__main__":
    unittest.main()

