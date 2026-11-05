import argparse
import json
from pathlib import Path
import time

from .engine import Engine, Receiver
from .experiment import ROOT, run_experiment


def main():
    parser = argparse.ArgumentParser(description="Local recoverable workflow lab; no API calls.")
    sub = parser.add_subparsers(dest="command", required=True)
    experiment = sub.add_parser("experiment", help="run deterministic fault-injection comparisons")
    experiment.add_argument("--output", type=Path, default=ROOT / "results/fault-replay.json")
    run = sub.add_parser("run", help="run/resume the fixture in persistent local state")
    run.add_argument("--state-dir", type=Path, default=ROOT / ".runtime/demo")
    args = parser.parse_args()
    if args.command == "experiment":
        result = run_experiment(args.output)
        print("scenario                              calls naive/durable  duplicate effects naive/durable")
        for r in result["scenarios"]:
            print(f"{r['scenario']:<38}{r['naive']['adapter_calls']}/{r['durable']['adapter_calls']:<18}"
                  f"{r['naive']['duplicate_effects']}/{r['durable']['duplicate_effects']}")
        print(f"Saved {args.output}")
        return
    args.state_dir.mkdir(parents=True, exist_ok=True)
    e, receiver = Engine(args.state_dir / "workflow.db"), Receiver(args.state_dir / "receiver.db")
    try:
        if e.db.execute("SELECT 1 FROM runs WHERE id='demo'").fetchone() is None:
            spec = json.loads((ROOT / "data/workflow.json").read_text())
            e.create("demo", spec["tasks"], spec["budget"], time.time())
        while claim := e.claim("demo", "local-worker", time.time(), lease_seconds=30):
            try:
                result = claim.execute()
            except (ValueError, KeyError, TypeError):
                e.fail(claim, time.time(), transient=False)
            else:
                e.complete(claim, result, time.time())
        for event in e.pending_events("demo"):
            receiver.receive(event)
            e.acknowledge(event["event_id"])
        print(json.dumps({"workflow": e.snapshot("demo"), "receiver_effects": receiver.count()}, indent=2))
    finally:
        e.close()
        receiver.close()


if __name__ == "__main__":
    main()
