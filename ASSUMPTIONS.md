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
9. **The actor's state time is `metadata.update_time`**, since ateapi has no separate field for when a state began.
10. **WorkerPool shape**: `spec.replicas`, `spec.sandboxClasses[].name`, `status.readyReplicas`, and a `Ready` condition whose `lastTransitionTime` marks when the pool fell short. Taken from what substrate-console reads, not from the CRD.
11. **Only ready workers exist in ateapi.** A pool with fewer ready replicas than desired has fewer Workers.
12. **Metric names and labels**: `ate.actor.lifecycle.operation.duration` (seconds, with `ate.actor.operation.name`, `ate.template.name`, `ate.template.atespace`, `ate.workerpool.name`, `error.type`) and `ate.actor.crashes`. Underscore names add `_seconds` and `_total`.
13. **Worker names are `<pool>-<n>`**, and the worker pod has the same name.
14. **Pod logs are per actor.** A worker pod's log shows whatever actor is assigned to it now.

## Checked on a real cluster

Against Substrate v1.0.0 on KinD (2026-11-08):

- **An egress policy must be named `default`.** Any other `metadata.name` is InvalidArgument (`egress_policy.metadata.name: must be default`). The fake also accepts an empty name and stores it as `default`; whether the real API does is unchecked.
- **A WorkerPool creates a Deployment with its own name**, so a pool can't share a name with another Deployment in its namespace. The fake has no Deployments and doesn't enforce this.
