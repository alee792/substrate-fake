# substrate-fake

A fake [Agent Substrate](https://github.com/agent-substrate/substrate) cluster for tests, demos and local development. It fakes the interfaces a real cluster offers, so the code you point at it runs its real client paths:

| Real thing | The fake |
|---|---|
| ateapi (`ateapi.v1.Control`) | gRPC over TLS for `api.ate-system.svc`. Authenticates bearer JWTs and authorizes each call against access policies, as ateapi does. Also in process with `Cluster.Client()`, which skips both. |
| Kubernetes API | The parts Substrate clients read: WorkerPools, the ClusterTrustBundle that roots ateapi's certificate, ServiceAccount token requests, OIDC discovery for the token issuer, worker pod logs. A kubeconfig is written for it. |
| Prometheus | The HTTP query API, evaluated by the real PromQL engine over series generated from the world's state. |
| atenet-router | `Router.Dial(atespace, actor, port)`, reaching whatever a test exposed for a running actor. |

Everything reads and writes one `World`. A resume through ateapi takes a worker that the next `ListWorkers` shows as busy, a crash shows up in `ate.actor.crashes`, and a WorkerPool short of workers shows up in Kubernetes.

It is an unofficial, personal project and is not affiliated with the Substrate maintainers.

## Run it

```sh
go run github.com/alee792/substrate-fake/cmd/substrate-fake --scenario demo
```

It prints a `KUBECONFIG`, the ateapi endpoint and the Prometheus URL. For example, with [substrate-console](https://github.com/alee792/substrate-console):

```sh
substrate-console serve --kubeconfig <printed> --endpoint <printed> --prometheus-url <printed>
```

`--scenario basic` starts an empty cluster with one 16-worker pool. `--metric-names underscore` switches from Prometheus 3's dotted names to the classic `ate_actor_crashes_total` style.

## Use it in tests

```go
c, err := substratefake.Start(ctx, substratefake.Options{NoServers: true})
defer c.Close()
api := c.Client() // ateapi.ControlClient, in process

c.World.GoldenDelay = 10 * time.Second           // golden snapshots take a while
c.World.Inject("DeleteActor", substratefake.Fault{Code: codes.Unavailable, Times: 2})
c.World.OnResume(func(r *ateapi.ObjectRef) { /* start the actor's process */ })
c.Router.Expose("s1", "a1", 8080, lis.Addr().String())

// ... run the code under test ...

alice := c.World.IssueToken("system:serviceaccount:ate-users:alice", substratefake.TokenOptions{})
c.World.Grant("s1", "system:serviceaccount:ate-users:alice", substratefake.RoleViewer)

c.World.CallNames() // ["CreateActor s1/a1", "CreateActorEgressPolicy s1/a1", "ResumeActor s1/a1", ...]
c.World.Crash("s1", "a1", "OOM")
```

Leave out `NoServers` to get the TLS endpoint, the Kubernetes API and Prometheus too. Pass `Clock: substratefake.NewManualClock(t0)` to move time yourself; delayed transitions (golden snapshots, resumes) happen when the clock passes them.

## What it does

- **Actors.** A created actor is SUSPENDED, from its template's golden snapshot. Resume schedules it onto a free ACTIVE worker in a pool matching its selector, or fails with ResourceExhausted. Suspend writes a new snapshot generation and frees the worker. Revert takes a crashed or running actor back to its durable snapshot. Delete frees everything, and deleting again is NotFound.
- **Templates.** Create starts the golden snapshot (ready at once, or after `GoldenDelay`). `FailGolden` makes it fail. Templates can't be updated, as in Substrate.
- **Workers.** `DrainWorker` marks a worker DRAINING and leaves its actor alone, as the API documents.
- **Metrics.** `ate.actor.lifecycle.operation.duration` (resume, with `error.type` on failures) per template, from each template's `Profile`, and `ate.actor.crashes` from real crash events. Series are generated for whatever range a query asks for.
- **Access.** Tokens are JWTs the fake signs: `World.IssueToken(principal, …)`, or a Kubernetes TokenRequest for any ServiceAccount. The issuer is the Kubernetes API's URL, which serves `/.well-known/openid-configuration` and its keys. ate-client is a bootstrap owner, as `ate-setup` makes it by default. `World.Grant` binds owner or viewer globally, or owner, editor or viewer on one atespace, and every RPC is checked against the table in Substrate's [authorization guide](https://github.com/agent-substrate/substrate/blob/v1.0.0/docs/guides/authorization.md), with ateapi's `PermissionDenied` wording. `World.TrustAudience` adds an audience, as adding it to a JWT provider does. The demo scenario grants two more people: `DemoOnCall`, a global viewer, and `DemoTriageDev`, an editor of `triage` only; the CLI writes a token file for each.
- **Faults.** Any ateapi method can be made to fail with a code, slow down, or both, for a number of calls or forever.

Not faked: port-forwarding (pass the endpoint directly), watch streams, the access policy RPCs (use `World.Grant`), groups, pod certificates and the worker-side services. Tags are faked (`CreateTag`, `GetTag`, `ListTags`, `UpdateTag`, `DeleteTag` and `CreateActor` with `source_tag`), with no guest state behind them. Unimplemented methods return Unimplemented.

[ASSUMPTIONS.md](ASSUMPTIONS.md) lists where the API docs were silent and the fake had to pick a behaviour.

## License

Apache-2.0.
