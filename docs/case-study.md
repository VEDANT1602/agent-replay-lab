# From a retry loop to a recovery contract

## Topic: agent execution

The narrower problem is recovery at the boundary between computation and a durable effect. A loop that retries after an exception answers only part of that problem. It also needs to know who owns the task, what was committed before a crash, which budget remains available, and whether a destination already applied an effect.

## Where the usual approach breaks

I started with the smallest plausible baseline: retain completed tasks in a Python dictionary, execute the next ready task, and emit its result. If the process stops, the dictionary disappears but previously emitted effects remain. A restart can repeat both useful work and effects. More retries do not repair the missing state boundary.

The comparison deliberately exposes that baseline's limitation. It does not stand in for Temporal, Airflow, durable cloud functions, or another mature orchestrator.

## My approach

I separated six decisions: reserve, lease, execute, commit, deliver, reconcile.

The lease grants temporary ownership, and an incrementing token makes older ownership visibly stale. The cost reservation happens in the same claim transaction, so simultaneous workers cannot each spend the final available units. When a worker disappears, I charge its reservation before offering a new attempt. That choice can underuse a budget if work never started, but it avoids assuming an uncertain attempt was free.

The task result and outgoing event form one transaction. The receiver then stores its receipt and the local effect in a separate transaction. If the sender stops before acknowledging delivery, the next delivery finds the receipt and does not apply the local effect twice.

The local receiver is an intentional boundary of the claim. An inbox record cannot atomically protect an unrelated email provider or payment API. Those integrations require a destination-supported idempotency key and a reconciliation policy.

## What the experiment showed

The four-task fixture normalizes synthetic values, calculates a summary, calculates threshold flags, and combines the two branches. Its normal cost is eight declared units. It contains no language model and does not evaluate reasoning quality.

Across the five fixed fault scenarios, the durable workflow completed four tasks and produced four unique receiver effects each time. The simple in-memory baseline produced one duplicate effect after a claim or adapter-boundary crash, and two after a commit or acknowledgement-boundary crash. These are operation counts from the recorded traces, not statistical estimates.

The adapter-before-commit failure still required repeated computation: the durable version made five adapter calls rather than four. Persistence cannot recover a result that was never committed. The after-claim failure used four adapter calls but charged eleven units instead of eight: the lost reservation was conservatively charged even though the injected crash occurred before execution. This is the visible price of the chosen accounting policy.

The stale-worker probe advanced the logical clock beyond a lease, assigned a replacement token, and attempted completion with the older token. That write was rejected. Concurrent-claim tests additionally use two independent database connections to check that only one worker receives the ready root task.

## What I learned

I need to write down the recovery boundary before choosing a retry count. “Safe to retry” can mean redoing pure computation, refusing a stale state write, or deduplicating a destination effect; these are separate guarantees. The tests are most useful where a transaction can roll back or ownership changes, rather than where a happy-path function returns an expected value.

## Next changes for a real workload

A deployment would need provider-aware cost caps, lease renewal, durable cancellation, authentication, observability, bounded payloads, destination-specific idempotency, and load tests on the chosen storage topology. The synthetic accounting constrains admitted attempts under the worker protocol, not arbitrary repeated calls to `Claim.execute()`. I would also test actual process termination and disk faults. This repository orderly closes and reopens database connections at deterministic boundaries within the same Python process; it does not kill a worker process, emulate sudden power loss, or prove filesystem durability.
