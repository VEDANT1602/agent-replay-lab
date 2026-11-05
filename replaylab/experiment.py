"""Deterministic process-boundary fault replay, not a throughput benchmark."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import tempfile

from .engine import ADAPTERS, Engine, Receiver, StateError

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = ("no_fault", "after_claim", "after_adapter", "after_commit", "after_receiver_before_ack")


def naive(spec: dict, scenario: str) -> dict:
    completed, effects, trace = {}, [], []
    injected, calls, actual_units = False, 0, 0
    while len(completed) < len(spec["tasks"]):
        task = next(t for t in spec["tasks"] if t["id"] not in completed and all(d in completed for d in t["deps"]))
        trace.append({"event": "claimed", "task_id": task["id"]})
        eligible = task["id"] == "summary" and not injected and scenario != "no_fault"
        if eligible and scenario == "after_claim":
            completed = {}
            injected = True
            trace.append({"event": "process_restarted", "fault": scenario})
            continue
        result = ADAPTERS[task["adapter"]](task["payload"], {d: completed[d] for d in task["deps"]})
        calls += 1
        actual_units += task["cost"]
        trace.append({"event": "adapter_executed", "task_id": task["id"]})
        if eligible and scenario == "after_adapter":
            completed = {}
            injected = True
            trace.append({"event": "process_restarted", "fault": scenario})
            continue
        effects.append(task["id"])
        completed[task["id"]] = result
        trace.append({"event": "effect_applied", "task_id": task["id"]})
        if eligible and scenario in ("after_commit", "after_receiver_before_ack"):
            completed = {}
            injected = True
            trace.append({"event": "process_restarted", "fault": scenario})
    counts = Counter(effects)
    return {"adapter_calls": calls, "executed_units": actual_units,
            "effect_attempts": len(effects), "unique_effects": len(counts),
            "duplicate_effects": sum(max(0, n - 1) for n in counts.values()),
            "completed_tasks": len(completed), "trace": trace}


def durable(spec: dict, scenario: str, directory: Path) -> dict:
    engine_path, receiver_path = directory / "workflow.db", directory / "receiver.db"
    engine, receiver = Engine(engine_path), Receiver(receiver_path)
    engine.create("experiment", spec["tasks"], spec["budget"])
    now, injected, calls, actual_units, deliveries, deduplications = 1.0, False, 0, 0, 0, 0
    trace = []

    def restart(fault):
        nonlocal engine, receiver, now, injected
        engine.close()
        receiver.close()
        engine, receiver = Engine(engine_path), Receiver(receiver_path)
        now += 10
        injected = True
        trace.append({"event": "process_restarted", "fault": fault, "clock": now})

    try:
        while True:
            claim = engine.claim("experiment", "worker", now, lease_seconds=2)
            if claim is None:
                break
            eligible = claim.task_id == "summary" and not injected and scenario != "no_fault"
            if eligible and scenario == "after_claim":
                restart(scenario)
                continue
            result = claim.execute()
            calls += 1
            cost = next(t["cost"] for t in spec["tasks"] if t["id"] == claim.task_id)
            actual_units += cost
            trace.append({"event": "adapter_executed", "task_id": claim.task_id, "attempt": claim.attempt})
            if eligible and scenario == "after_adapter":
                restart(scenario)
                continue
            engine.complete(claim, result, now + 0.1)
            if eligible and scenario == "after_commit":
                restart(scenario)
                continue
            if eligible and scenario == "after_receiver_before_ack":
                event = next(e for e in engine.pending_events("experiment") if e["task_id"] == claim.task_id)
                deliveries += 1
                receiver.receive(event)
                restart(scenario)
                continue
            for event in engine.pending_events("experiment"):
                deliveries += 1
                if not receiver.receive(event):
                    deduplications += 1
                engine.acknowledge(event["event_id"])
            now += 1
        for event in engine.pending_events("experiment"):
            deliveries += 1
            if not receiver.receive(event):
                deduplications += 1
            engine.acknowledge(event["event_id"])
        snapshot = engine.snapshot("experiment")
        effects = receiver.effect_ids()
        return {"adapter_calls": calls, "executed_units": actual_units,
                "charged_units": snapshot["account"]["spent"],
                "reserved_units": snapshot["account"]["reserved"],
                "effect_attempts": deliveries, "unique_effects": len(set(effects)),
                "duplicate_effects": len(effects) - len(set(effects)), "deduplicated_deliveries": deduplications,
                "completed_tasks": sum(t["state"] == "succeeded" for t in snapshot["tasks"]),
                "snapshot": snapshot, "trace": trace, "audit": engine.audit("experiment")}
    finally:
        engine.close()
        receiver.close()


def stale_worker_probe(spec: dict, directory: Path) -> dict:
    engine = Engine(directory / "fencing.db")
    try:
        engine.create("race", spec["tasks"], spec["budget"])
        old = engine.claim("race", "old-worker", 1, 1)
        new = engine.claim("race", "replacement", 2, 1)
        rejected = False
        try:
            engine.complete(old, old.execute(), 2.1)
        except StateError:
            rejected = True
        engine.complete(new, new.execute(), 2.2)
        return {"stale_completion_rejected": rejected, "old_token": old.token,
                "new_token": new.token, "snapshot": engine.snapshot("race"), "audit": engine.audit("race")}
    finally:
        engine.close()


def run_experiment(output: Path) -> dict:
    spec = json.loads((ROOT / "data/workflow.json").read_text())
    results = {"protocol": "five fixed scenarios; one fault at task summary; deterministic logical clock",
               "measurement": "operation counts, not elapsed time or model quality",
               "budget_units": "synthetic declared maximum per attempt; not dollars or tokens",
               "source_sha256": {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in (ROOT / "data/workflow.json", ROOT / "replaylab/engine.py", Path(__file__))},
               "scenarios": []}
    with tempfile.TemporaryDirectory() as tmp:
        for scenario in SCENARIOS:
            directory = Path(tmp) / scenario
            directory.mkdir()
            results["scenarios"].append({"scenario": scenario, "naive": naive(spec, scenario),
                                        "durable": durable(spec, scenario, directory)})
        results["fencing_probe"] = stale_worker_probe(spec, Path(tmp))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8", newline="\n")
    return results
