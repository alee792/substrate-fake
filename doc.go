// Package substratefake is a fake Agent Substrate cluster for tests,
// demos and local development.
//
// It fakes the cluster top to bottom, at the same interfaces a real cluster
// offers, so code under test runs its real client paths:
//
//   - ateapi: a gRPC server implementing ateapi.v1.Control, over TLS on a
//     loopback port with bearer-token auth, or in process (Cluster.Client).
//   - Kubernetes: an HTTPS API server with just what Substrate clients read:
//     WorkerPools, the ClusterTrustBundle that roots ateapi's certificate,
//     ServiceAccount token requests, and worker pod logs. Start writes a
//     kubeconfig for it.
//   - Prometheus: the HTTP query API, evaluated by the real PromQL engine
//     over series generated from the world's state and a scenario's
//     traffic profiles.
//   - The atenet router: Router.Dial reaches whatever a test exposed for a
//     running actor's port.
//
// All of it reads and writes one World, which tests can inspect (an ordered
// call log, the stored protos), drive (crash an actor, fail a golden
// snapshot, advance a manual clock), and break (per-method error codes and
// latency).
//
// Behaviour follows Substrate v1.0.0's API documentation. Where the docs are
// silent the fake picks something plausible; ASSUMPTIONS.md lists each
// choice so it can be checked against a real cluster.
package substratefake
