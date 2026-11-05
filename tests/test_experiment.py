from pathlib import Path
import tempfile
import unittest

from replaylab.experiment import run_experiment


class ExperimentTests(unittest.TestCase):
    def test_restarts_finish_with_no_duplicate_receiver_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            results = run_experiment(Path(tmp) / "results.json")
        for row in results["scenarios"]:
            with self.subTest(scenario=row["scenario"]):
                self.assertEqual(row["durable"]["completed_tasks"], 4)
                self.assertEqual(row["durable"]["unique_effects"], 4)
                self.assertEqual(row["durable"]["duplicate_effects"], 0)
                self.assertEqual(row["durable"]["reserved_units"], 0)
                self.assertLessEqual(row["durable"]["charged_units"], 30)
        retry = next(r for r in results["scenarios"] if r["scenario"] == "after_receiver_before_ack")
        self.assertEqual(retry["durable"]["deduplicated_deliveries"], 1)
        self.assertGreater(retry["naive"]["duplicate_effects"], 0)
        self.assertTrue(results["fencing_probe"]["stale_completion_rejected"])


if __name__ == "__main__":
    unittest.main()
