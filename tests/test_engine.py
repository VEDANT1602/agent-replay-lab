from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from replaylab.engine import ADAPTERS, Engine, Receiver, StateError

ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / "data/workflow.json").read_text())


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "engine.db"
        self.engine = Engine(self.path)
        self.engine.create("run", SPEC["tasks"], SPEC["budget"])

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def one(self, now=1):
        claim = self.engine.claim("run", "worker", now)
        self.assertIsNotNone(claim)
        self.engine.complete(claim, claim.execute(), now + 0.1)
        return claim

    def test_topological_execution_and_budget(self):
        for now in range(1, 5):
            self.one(now)
        snapshot = self.engine.snapshot("run")
        self.assertEqual({t["state"] for t in snapshot["tasks"]}, {"succeeded"})
        self.assertEqual(snapshot["account"]["spent"], 8)
        self.assertEqual(snapshot["account"]["reserved"], 0)
        self.assertIsNone(self.engine.claim("run", "other", 6))

    def test_completed_tasks_survive_connection_restart(self):
        self.one()
        self.engine.close()
        self.engine = Engine(self.path)
        self.assertEqual(self.engine.claim("run", "restart", 2).task_id, "summary")

    def test_only_one_concurrent_worker_claims_ready_root(self):
        def claim(worker):
            e = Engine(self.path)
            try:
                c = e.claim("run", worker, 1)
                return None if c is None else c.task_id
            finally:
                e.close()
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(claim, ["a", "b"]))
        self.assertEqual(results.count("normalize"), 1)
        self.assertEqual(results.count(None), 1)

    def test_independent_branches_can_be_claimed_concurrently(self):
        self.one()
        a = self.engine.claim("run", "a", 2)
        b = self.engine.claim("run", "b", 2)
        self.assertEqual({a.task_id, b.task_id}, {"summary", "flags"})

    def test_stale_worker_cannot_complete_after_reclaim(self):
        old = self.engine.claim("run", "old", 1, 2)
        new = self.engine.claim("run", "new", 3, 2)
        with self.assertRaises(StateError):
            self.engine.complete(old, old.execute(), 3)
        self.engine.complete(new, new.execute(), 3.1)
        account = self.engine.snapshot("run")["account"]
        self.assertEqual((account["spent"], account["reserved"]), (4, 0))

    def test_exact_lease_boundary_is_expired(self):
        claim = self.engine.claim("run", "w", 1, 2)
        with self.assertRaises(StateError):
            self.engine.complete(claim, claim.execute(), 3)

    def test_repeated_completion_is_idempotent(self):
        c = self.one()
        self.engine.complete(c, c.execute(), 8)
        self.assertEqual(self.engine.snapshot("run")["account"]["spent"], 2)
        self.assertEqual(len(self.engine.pending_events("run")), 1)

    def test_completed_result_is_immutable(self):
        c = self.one()
        with self.assertRaises(StateError):
            self.engine.complete(c, {"changed": True}, 2)

    def test_nontransient_failure_blocks_all_descendants(self):
        c = self.engine.claim("run", "w", 1)
        self.engine.fail(c, 1.1, transient=False)
        self.assertIsNone(self.engine.claim("run", "w", 2))
        self.assertEqual([t["state"] for t in self.engine.snapshot("run")["tasks"]],
                         ["failed", "blocked", "blocked", "blocked"])

    def test_transient_retries_are_bounded(self):
        for now in range(1, 4):
            c = self.engine.claim("run", "w", now)
            self.engine.fail(c, now + 0.1, transient=True)
        self.assertIsNone(self.engine.claim("run", "w", 4))
        self.assertEqual(self.engine.snapshot("run")["tasks"][0]["attempts"], 3)

    def test_repeated_crash_recovery_is_bounded(self):
        for now in (1, 3, 5):
            c = self.engine.claim("run", "w", now, 1)
            self.assertIsNotNone(c)
        self.assertIsNone(self.engine.claim("run", "w", 7, 1))
        s = self.engine.snapshot("run")
        self.assertEqual(s["tasks"][0]["state"], "failed")
        self.assertEqual(s["account"]["spent"], 6)

    def test_reservation_prevents_oversubscription(self):
        tasks = [deepcopy(SPEC["tasks"][0]) for _ in range(2)]
        tasks[1]["id"] = "other"
        self.engine.create("tight", tasks, budget=3)
        self.assertIsNotNone(self.engine.claim("tight", "a", 1))
        self.assertIsNone(self.engine.claim("tight", "b", 1))
        self.assertEqual(self.engine.snapshot("tight")["account"]["reserved"], 2)

    def test_unknown_crashed_attempt_is_charged_conservatively(self):
        task = deepcopy(SPEC["tasks"][0])
        self.engine.create("tight", [task], budget=2)
        self.engine.claim("tight", "a", 1, 1)
        self.assertIsNone(self.engine.claim("tight", "b", 2, 1))
        account = self.engine.snapshot("tight")["account"]
        self.assertEqual((account["spent"], account["reserved"]), (2, 0))

    def test_invalid_clocks_rejected_before_claim_and_completion(self):
        c = self.engine.claim("run", "w", 1)
        for bad in (float("nan"), float("inf"), -1, True, "2", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.engine.claim("run", "w", bad)
                with self.assertRaises(ValueError):
                    self.engine.complete(c, c.execute(), bad)
                with self.assertRaises(ValueError):
                    self.engine.fail(c, bad, transient=True)
        self.assertEqual(self.engine.snapshot("run")["account"]["reserved"], 2)

    def test_invalid_lease_and_overflow_rejected(self):
        for lease in (0, -1, True, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                self.engine.claim("run", "w", 1, lease)
        with self.assertRaises(ValueError):
            self.engine.claim("run", "w", 1e308, 1e308)

    def test_backward_clock_rejected(self):
        c = self.engine.claim("run", "w", 5)
        with self.assertRaises(ValueError):
            self.engine.complete(c, c.execute(), 4)
        with self.assertRaises(ValueError):
            self.engine.claim("run", "other", 4)

    def test_bad_payload_cannot_write_nonfinite_result(self):
        c = self.engine.claim("run", "w", 1)
        with self.assertRaises(ValueError):
            self.engine.complete(c, {"value": float("nan")}, 1.1)
        self.assertEqual(self.engine.snapshot("run")["tasks"][0]["state"], "running")

    def test_completion_and_outbox_rollback_together(self):
        c = self.engine.claim("run", "w", 1)
        self.engine.db.execute("""CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox
          BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(Exception):
            self.engine.complete(c, c.execute(), 1.1)
        snapshot = self.engine.snapshot("run")
        self.assertEqual(snapshot["tasks"][0]["state"], "running")
        self.assertEqual(snapshot["account"]["spent"], 0)
        self.assertEqual(snapshot["account"]["reserved"], 2)

    def test_duplicate_run_does_not_partially_add_tasks(self):
        with self.assertRaises(Exception):
            self.engine.create("run", SPEC["tasks"], 100)
        self.assertEqual(len(self.engine.snapshot("run")["tasks"]), 4)

    def test_invalid_graph_and_adapter(self):
        for mutate in ("cycle", "unknown_dependency", "duplicate", "shell"):
            tasks = deepcopy(SPEC["tasks"])
            if mutate == "cycle":
                tasks[0]["deps"] = ["package"]
            elif mutate == "unknown_dependency":
                tasks[0]["deps"] = ["absent"]
            elif mutate == "duplicate":
                tasks[1]["id"] = "normalize"
            else:
                tasks[0]["adapter"] = "shell"
            with self.assertRaises(ValueError):
                self.engine.create(mutate, tasks, 30)

    def test_noninteger_budget_cost_and_attempt_count(self):
        for budget in (True, -1, 2.5):
            with self.assertRaises(ValueError):
                self.engine.create("bad", SPEC["tasks"], budget)
        for field in ("cost", "max_attempts"):
            tasks = deepcopy(SPEC["tasks"])
            tasks[0][field] = True
            with self.assertRaises(ValueError):
                self.engine.create("bad", tasks, 30)

    def test_flag_adapter_rejects_invalid_thresholds(self):
        for threshold in (float("nan"), float("inf"), True, "0.6", None):
            with self.subTest(threshold=threshold):
                with self.assertRaises(ValueError):
                    ADAPTERS["flag"]({"source": "n", "threshold": threshold}, {"n": {"values": [0.2, 0.8]}})


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "receiver.db"
        self.receiver = Receiver(self.path)
        self.event = {"event_id": "run:task", "result": {"score": 2}}

    def tearDown(self):
        self.receiver.close()
        self.tmp.cleanup()

    def test_repeated_delivery_survives_receiver_restart(self):
        self.assertTrue(self.receiver.receive(self.event))
        self.receiver.close()
        self.receiver = Receiver(self.path)
        self.assertFalse(self.receiver.receive(self.event))
        self.assertEqual(self.receiver.count(), 1)

    def test_same_event_id_with_changed_body_is_rejected(self):
        self.receiver.receive(self.event)
        with self.assertRaises(StateError):
            self.receiver.receive({"event_id": "run:task", "result": {"score": 3}})
        self.assertEqual(self.receiver.count(), 1)

    def test_receiver_effect_and_inbox_are_atomic(self):
        self.receiver.db.execute("""CREATE TRIGGER reject_effect BEFORE INSERT ON effects
          BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(Exception):
            self.receiver.receive(self.event)
        self.assertEqual(self.receiver.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
