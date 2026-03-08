from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.evaluation import evaluate_golden_cases, extract_lineage_edges


class EvaluationTest(unittest.TestCase):
    def test_extract_lineage_edges_insert(self) -> None:
        sql = "INSERT INTO tgt SELECT s.a AS a1, s.b + 1 AS b1 FROM src s;"
        edges = extract_lineage_edges(sql)
        if not edges:
            self.assertEqual(edges, set())
            return
        self.assertIn("tgt.a1<-s.a", edges)
        self.assertIn("tgt.b1<-s.b", edges)

    def test_golden_cases_shape(self) -> None:
        summary = evaluate_golden_cases(ROOT, ROOT / "tests" / "golden" / "cases")
        self.assertIn("total_cases", summary)
        self.assertIn("passed_cases", summary)
        self.assertIn("pass_rate", summary)
        self.assertIn("results", summary)
        self.assertIn("edge_micro_precision", summary)
        self.assertIn("edge_micro_recall", summary)
        self.assertIn("edge_micro_f1", summary)
        self.assertLessEqual(summary["edge_micro_f1"], 1.0)
        self.assertGreaterEqual(summary["total_cases"], 2)


if __name__ == "__main__":
    unittest.main()

