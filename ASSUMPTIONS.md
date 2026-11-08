# Assumptions

Where Substrate v1.0.0's API documentation doesn't say what happens, the fake picks something plausible. Each choice here should be checked against a real cluster.

1. **Resume is synchronous by default.** `ResumeActor` returns a RUNNING actor. `World.ResumeDelay` makes it return RESUMING and finish later. The docs don't say whether the call waits.
2. **One actor per worker.** Each worker's `status.capacity.actors` is 1, so a pool of N workers runs N actors.
3. **No free worker means ResourceExhausted**, the error type the resume-failure metric is split by.
4. **CreateActor fails with FailedPrecondition** when the template's golden snapshot failed. A template with golden snapshots disabled creates actors with no snapshot.
5. **DeleteActorTemplate succeeds while actors still use the template.**
6. **DeleteActor on a missing actor is NotFound**, not success.
7. **Revert clears the crash** and drops node-local snapshot locality.
8. **After a crash the worker assignment is cleared** and `assigned_node` is kept.
9. **`metadata.update_time` is the last write to the actor, not when its state began.** Checked in the v1.0.0 source: every store write bumps it, including atelet snapshot reports and tags on a RUNNING or SUSPENDED actor and the UpdateActor RPC. `World.Touch` makes such a write. When an actor arrived on its worker is the `create_time` of its ActorAssignment (`ListWorkerActorAssignments`), which a resume creates when it binds the actor and a release deletes. Seeded actors are taken to have been bound at their `update_time`.
10. **WorkerPool shape**: `spec.replicas`, `spec.sandboxClasses[].name`, `status.readyReplicas`, and a `Ready` condition whose `lastTransitionTime` marks when the pool fell short. Taken from what substrate-console reads, not from the CRD.
11. **Only ready workers exist in ateapi.** A pool with fewer ready replicas than desired has fewer Workers.
12. **Metric names and labels**: `ate.actor.lifecycle.operation.duration` (seconds, with `ate.actor.operation.name`, `ate.template.name`, `ate.template.atespace`, `ate.workerpool.name`, `error.type`) and `ate.actor.crashes`. Underscore names add `_seconds` and `_total`. Checked on kind (Substrate v1.0.0): failed resumes land in the same histogram with `error.type` set and take a few milliseconds, so the fake gives them a 2.5 ms median. Real buckets are coarser than the fake's (0.005 to 30 s), and real series also carry `ate.snapshot.kind` (boot from golden takes seconds, latest snapshot about 0.2 s), which the fake leaves out.
13. **Worker names are `<pool>-<n>`**, and the worker pod has the same name.
14. **A worker pod's log holds every actor that ran there**, one after another, as a container log would. Readers that want one actor's lines pass `sinceTime`, honoured to the second like the real API server.
