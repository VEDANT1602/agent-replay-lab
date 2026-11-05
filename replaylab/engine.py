"""SQLite workflow state; adapters cannot execute arbitrary commands.

Every mutating public operation uses an explicit transaction. A lease token
fences task completion, while an outbox transfers committed results to a
separate receiver with a durable deduplication table.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sqlite3
from typing import Any


class StateError(RuntimeError):
    """A stale or incompatible operation cannot change workflow state."""


def validate_clock(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("clock must be a finite nonnegative number")
    if not math.isfinite(value) or value < 0:
        raise ValueError("clock must be a finite nonnegative number")
    return float(value)


def positive_int(value: int, label: str, *, zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < (0 if zero else 1):
        raise ValueError(f"{label} must be an integer >= {0 if zero else 1}")
    return value


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def normalize(payload: dict, deps: dict) -> dict:
    values = payload["values"]
    if not isinstance(values, list) or not values:
        raise ValueError("nonempty values required")
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in values):
        raise ValueError("values must be finite numbers")
    return {"values": [round(float(v) / 100.0, 6) for v in values]}


def summarize(payload: dict, deps: dict) -> dict:
    values = deps[payload["source"]]["values"]
    return {"count": len(values), "mean": round(sum(values) / len(values), 6)}


def flag(payload: dict, deps: dict) -> dict:
    values = deps[payload["source"]]["values"]
    threshold = payload["threshold"]
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not math.isfinite(threshold):
        raise ValueError("threshold must be a finite number")
    return {"above_threshold": sum(v >= threshold for v in values)}


def combine(payload: dict, deps: dict) -> dict:
    return {"summary": deps[payload["summary"]], "flags": deps[payload["flags"]]}


ADAPTERS = {"normalize": normalize, "summarize": summarize, "flag": flag, "combine": combine}


@dataclass(frozen=True)
class Claim:
    run_id: str
    task_id: str
    worker: str
    token: int
    attempt: int
    adapter: str
    payload: dict
    dependencies: dict
    lease_until: float

    def execute(self) -> dict:
        return ADAPTERS[self.adapter](self.payload, self.dependencies)


class Engine:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self.db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY, budget INTEGER NOT NULL CHECK(budget>=0),
          spent INTEGER NOT NULL DEFAULT 0 CHECK(spent>=0),
          reserved INTEGER NOT NULL DEFAULT 0 CHECK(reserved>=0),
          CHECK(spent+reserved<=budget));
        CREATE TABLE IF NOT EXISTS tasks (
          run_id TEXT REFERENCES runs(id), id TEXT, ordinal INTEGER,
          adapter TEXT, payload TEXT, deps TEXT, cost INTEGER,
          max_attempts INTEGER, attempts INTEGER DEFAULT 0,
          token INTEGER DEFAULT 0, worker TEXT, lease_until REAL,
          state TEXT DEFAULT 'pending', result TEXT,
          PRIMARY KEY(run_id,id));
        CREATE TABLE IF NOT EXISTS outbox (
          event_id TEXT PRIMARY KEY, run_id TEXT REFERENCES runs(id),
          task_id TEXT, body TEXT NOT NULL, delivered INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS audit (
          seq INTEGER PRIMARY KEY, run_id TEXT, task_id TEXT,
          event TEXT, clock REAL, token INTEGER, detail TEXT);
        """)

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        else:
            self.db.execute("COMMIT")

    def _audit(self, run: str, task: str | None, event: str, now: float, token: int = 0, detail: str = ""):
        self.db.execute("INSERT INTO audit(run_id,task_id,event,clock,token,detail) VALUES(?,?,?,?,?,?)",
                        (run, task, event, now, token, detail))

    def create(self, run_id: str, tasks: list[dict], budget: int, now: float = 0) -> None:
        now = validate_clock(now)
        positive_int(budget, "budget", zero=True)
        if not isinstance(run_id, str) or not run_id or ":" in run_id:
            raise ValueError("nonempty run id without ':' required")
        if not isinstance(tasks, list) or not tasks:
            raise ValueError("nonempty task list required")
        ids = [t.get("id") for t in tasks]
        if any(not isinstance(i, str) or not i or ":" in i for i in ids) or len(set(ids)) != len(ids):
            raise ValueError("unique nonempty task ids without ':' required")
        graph = {}
        for t in tasks:
            if t.get("adapter") not in ADAPTERS:
                raise ValueError("adapter is not allowlisted")
            positive_int(t.get("cost"), "cost")
            positive_int(t.get("max_attempts"), "max_attempts")
            deps = t.get("deps", [])
            if not isinstance(deps, list) or any(d not in ids for d in deps) or len(set(deps)) != len(deps):
                raise ValueError("dependencies must be distinct existing task ids")
            graph[t["id"]] = deps
            if not isinstance(t.get("payload"), dict):
                raise ValueError("payload must be a JSON object")
            canonical(t["payload"])
        visited, active = set(), set()

        def visit(node):
            if node in active:
                raise ValueError("dependency cycle")
            if node in visited:
                return
            active.add(node)
            for dep in graph[node]:
                visit(dep)
            active.remove(node)
            visited.add(node)

        for node in graph:
            visit(node)
        with self.transaction():
            self.db.execute("INSERT INTO runs(id,budget) VALUES(?,?)", (run_id, budget))
            for ordinal, t in enumerate(tasks):
                self.db.execute("""INSERT INTO tasks
                  (run_id,id,ordinal,adapter,payload,deps,cost,max_attempts)
                  VALUES(?,?,?,?,?,?,?,?)""", (run_id, t["id"], ordinal, t["adapter"], canonical(t["payload"]),
                  canonical(graph[t["id"]]), t["cost"], t["max_attempts"]))
            self._audit(run_id, None, "created", now)

    def _known_run(self, run: str):
        row = self.db.execute("SELECT * FROM runs WHERE id=?", (run,)).fetchone()
        if row is None:
            raise ValueError("unknown run")
        return row

    def _check_clock(self, run: str, now: float):
        previous = self.db.execute("SELECT MAX(clock) FROM audit WHERE run_id=?", (run,)).fetchone()[0]
        if previous is not None and now < previous:
            raise ValueError("workflow clock cannot move backward")

    def _settle(self, row):
        self.db.execute("UPDATE runs SET reserved=reserved-?,spent=spent+? WHERE id=?",
                        (row["cost"], row["cost"], row["run_id"]))

    def _recover(self, run: str, now: float):
        expired = self.db.execute("SELECT * FROM tasks WHERE run_id=? AND state='running' AND lease_until<=?",
                                  (run, now)).fetchall()
        for row in expired:
            self._settle(row)
            state = "failed" if row["attempts"] >= row["max_attempts"] else "pending"
            self.db.execute("UPDATE tasks SET state=?,worker=NULL,lease_until=NULL WHERE run_id=? AND id=?",
                            (state, run, row["id"]))
            self._audit(run, row["id"], "lease_expired", now, row["token"], state)

    def claim(self, run: str, worker: str, now: float, lease_seconds: float = 5) -> Claim | None:
        now = validate_clock(now)
        lease_seconds = validate_clock(lease_seconds)
        if lease_seconds <= 0 or not math.isfinite(now + lease_seconds):
            raise ValueError("lease duration must be positive with a finite deadline")
        if not isinstance(worker, str) or not worker:
            raise ValueError("worker required")
        with self.transaction():
            self._known_run(run)
            self._check_clock(run, now)
            self._recover(run, now)
            rows = self.db.execute("SELECT * FROM tasks WHERE run_id=? ORDER BY ordinal", (run,)).fetchall()
            by_id = {r["id"]: r for r in rows}
            # Iterate to a fixed point so failed ancestors block all descendants.
            blocked = {r["id"] for r in rows if r["state"] in ("failed", "blocked")}
            while True:
                more = {r["id"] for r in rows if r["state"] == "pending" and any(d in blocked for d in json.loads(r["deps"]))}
                if more <= blocked:
                    break
                blocked |= more
            for row in rows:
                if row["state"] == "pending" and row["id"] in blocked:
                    self.db.execute("UPDATE tasks SET state='blocked' WHERE run_id=? AND id=?", (run, row["id"]))
                    self._audit(run, row["id"], "blocked", now)
            for row in rows:
                if row["state"] != "pending" or row["id"] in blocked:
                    continue
                deps = json.loads(row["deps"])
                if any(by_id[d]["state"] != "succeeded" for d in deps):
                    continue
                account = self._known_run(run)
                if account["spent"] + account["reserved"] + row["cost"] > account["budget"]:
                    continue
                token, attempt = row["token"] + 1, row["attempts"] + 1
                self.db.execute("""UPDATE tasks SET state='running',worker=?,lease_until=?,token=?,attempts=?
                  WHERE run_id=? AND id=?""", (worker, now + lease_seconds, token, attempt, run, row["id"]))
                self.db.execute("UPDATE runs SET reserved=reserved+? WHERE id=?", (row["cost"], run))
                self._audit(run, row["id"], "claimed", now, token, worker)
                return Claim(run, row["id"], worker, token, attempt, row["adapter"], json.loads(row["payload"]),
                             {d: json.loads(by_id[d]["result"]) for d in deps}, now + lease_seconds)
        return None

    def _current(self, claim: Claim, now: float):
        self._check_clock(claim.run_id, now)
        row = self.db.execute("SELECT * FROM tasks WHERE run_id=? AND id=?", (claim.run_id, claim.task_id)).fetchone()
        if row is None or row["token"] != claim.token or row["worker"] != claim.worker:
            raise StateError("stale worker or fencing token")
        if row["state"] == "succeeded":
            return row
        if row["state"] != "running" or now >= row["lease_until"]:
            raise StateError("task is not owned by a live lease")
        return row

    def complete(self, claim: Claim, result: dict, now: float) -> dict:
        now = validate_clock(now)
        if not isinstance(result, dict):
            raise ValueError("result must be a JSON object")
        body = canonical(result)
        with self.transaction():
            row = self._current(claim, now)
            if row["state"] == "succeeded":
                if body != row["result"]:
                    raise StateError("completed result cannot be changed")
                return json.loads(row["result"])
            self._settle(row)
            self.db.execute("UPDATE tasks SET state='succeeded',result=? WHERE run_id=? AND id=?",
                            (body, claim.run_id, claim.task_id))
            event_id = f"{claim.run_id}:{claim.task_id}"
            event = canonical({"event_id": event_id, "run_id": claim.run_id, "task_id": claim.task_id, "result": result})
            self.db.execute("INSERT INTO outbox(event_id,run_id,task_id,body) VALUES(?,?,?,?)",
                            (event_id, claim.run_id, claim.task_id, event))
            self._audit(claim.run_id, claim.task_id, "completed", now, claim.token)
        return result

    def fail(self, claim: Claim, now: float, *, transient: bool) -> None:
        now = validate_clock(now)
        if not isinstance(transient, bool):
            raise ValueError("transient must be a bool")
        with self.transaction():
            row = self._current(claim, now)
            if row["state"] == "succeeded":
                raise StateError("completed task cannot fail")
            self._settle(row)
            state = "pending" if transient and row["attempts"] < row["max_attempts"] else "failed"
            self.db.execute("UPDATE tasks SET state=?,worker=NULL,lease_until=NULL WHERE run_id=? AND id=?",
                            (state, claim.run_id, claim.task_id))
            self._audit(claim.run_id, claim.task_id, "attempt_failed", now, claim.token, state)

    def pending_events(self, run: str) -> list[dict]:
        self._known_run(run)
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT body FROM outbox WHERE run_id=? AND delivered=0 ORDER BY rowid", (run,))]

    def acknowledge(self, event_id: str) -> None:
        with self.transaction():
            self.db.execute("UPDATE outbox SET delivered=1 WHERE event_id=?", (event_id,))

    def snapshot(self, run: str) -> dict:
        # One transaction prevents a mixed account/task view during a worker commit.
        with self.transaction():
            account = dict(self._known_run(run))
            tasks = [dict(r) for r in self.db.execute("SELECT id,state,attempts,token,cost FROM tasks WHERE run_id=? ORDER BY ordinal", (run,))]
            return {"account": account, "tasks": tasks, "pending_outbox": len(self.pending_events(run))}

    def audit(self, run: str) -> list[dict]:
        self._known_run(run)
        return [dict(r) for r in self.db.execute("SELECT seq,task_id,event,clock,token,detail FROM audit WHERE run_id=? ORDER BY seq", (run,))]


class Receiver:
    """Local deduplication and effect share one SQLite transaction.

    This does not make arbitrary email, payment, or API calls exactly once.
    The caller must use a destination's idempotency protocol for those effects.
    """

    def __init__(self, path: str | Path):
        self.db = sqlite3.connect(str(path), timeout=10, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS inbox(event_id TEXT PRIMARY KEY, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS effects(event_id TEXT PRIMARY KEY, result TEXT NOT NULL);
        """)

    def receive(self, event: dict) -> bool:
        body = canonical(event)
        event_id = event["event_id"]
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT body FROM inbox WHERE event_id=?", (event_id,)).fetchone()
            if row is not None:
                if row[0] != body:
                    raise StateError("event id reused with different content")
                self.db.execute("COMMIT")
                return False
            self.db.execute("INSERT INTO inbox VALUES(?,?)", (event_id, body))
            self.db.execute("INSERT INTO effects VALUES(?,?)", (event_id, canonical(event["result"])))
            self.db.execute("COMMIT")
            return True
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM effects").fetchone()[0]

    def effect_ids(self) -> list[str]:
        return [row[0] for row in self.db.execute("SELECT event_id FROM effects ORDER BY rowid")]

    def close(self) -> None:
        self.db.close()
