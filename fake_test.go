package substratefake

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func start(t *testing.T, opts Options) *Cluster {
	t.Helper()
	c, err := Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func code(err error) codes.Code { return status.Code(err) }

func ref(space, name string) *ateapi.ObjectRef { return &ateapi.ObjectRef{Atespace: space, Name: name} }

// The write path Canopy drives: atespace, template, actor, egress policy,
// resume, suspend, delete.
func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC))
	c := start(t, Options{NoServers: true, Clock: clock})
	c.World.GoldenDelay = 10 * time.Second
	api := c.Client()
	var resumed []string
	c.World.OnResume(func(r *ateapi.ObjectRef) { resumed = append(resumed, r.Name) })

	if _, err := api.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "s1"}}}); err != nil {
		t.Fatal(err)
	}
	tmpl := TemplateSpec{Atespace: "s1", Name: "t1", Image: "img:1", CPUMillis: 1000, MemoryBytes: gib}.Proto()
	tmpl.Status = nil
	if _, err := api.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl}); err != nil {
		t.Fatal(err)
	}
	got, _ := api.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref("s1", "t1")})
	if got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil {
		t.Fatal("golden snapshot ready before GoldenDelay")
	}
	clock.Advance(11 * time.Second)
	got, _ = api.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref("s1", "t1")})
	if got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag().GetName() != "t1-golden" {
		t.Fatalf("golden not ready after GoldenDelay: %v", got.GetStatus())
	}

	a, err := api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "s1", Name: "a1"}, ActorTemplate: ref("", "t1")}})
	if err != nil {
		t.Fatal(err)
	}
	if a.Status.State != ateapi.ActorState_ACTOR_STATE_SUSPENDED || len(a.Status.Snapshots) != 1 {
		t.Fatalf("new actor: %v", a.Status)
	}
	if _, err := api.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{Actor: ref("s1", "a1"), EgressPolicy: &ateapi.EgressPolicy{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.UpdateActorEgressPolicy(ctx, &ateapi.UpdateActorEgressPolicyRequest{Actor: ref("s1", "a1"),
		EgressPolicy: &ateapi.EgressPolicy{Metadata: &ateapi.ResourceMetadata{Name: "custom"}}}); code(err) != codes.InvalidArgument {
		t.Fatalf("egress policy named other than default: %v", err)
	}
	// Ties follow Substrate's own validation: same pattern string, same port
	// key, any protocols. Defaults apply first (http 80, https 443).
	all := &ateapi.Ports{All: &ateapi.AllPorts{}}
	for _, tc := range []struct {
		name  string
		rules []*ateapi.EgressRule
		tie   bool
	}{
		{"http and tls on every port", []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"*"}, Ports: all}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"*"}, Ports: all}}}, true},
		{"http on every port, tls on 443", []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"*"}, Ports: all}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"*"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}}}, false},
		{"two http rules on the default port", []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}},
			{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}}}, true},
		{"wildcard and specific host on one port", []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"*"}}},
			{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}}}, false},
		{"https and tls on 443", []*ateapi.EgressRule{
			{Https: &ateapi.HttpsRule{Hostnames: []string{"*.example.com"}}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"*.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}}}, true},
	} {
		_, err := api.UpdateActorEgressPolicy(ctx, &ateapi.UpdateActorEgressPolicyRequest{Actor: ref("s1", "a1"), EgressPolicy: &ateapi.EgressPolicy{Rules: tc.rules}})
		if tie := code(err) == codes.InvalidArgument; tie != tc.tie || (!tc.tie && err != nil) {
			t.Errorf("%s: err %v, want tie %v", tc.name, err, tc.tie)
		}
	}
	if p := c.World.Policy("s1", "a1"); p.GetMetadata().GetName() != "default" {
		t.Fatalf("egress policy name = %q", p.GetMetadata().GetName())
	}
	r, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s1", "a1")})
	if err != nil || !r.Resumed || r.Actor.Status.State != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("resume: %v %v", r, err)
	}
	if wa := r.Actor.Status.WorkerAssignment; wa.GetWorkerPool() != "default" || wa.GetSandboxRuntime().GetSandboxClass() != "gvisor" {
		t.Fatalf("assignment: %v", wa)
	}
	if len(resumed) != 1 {
		t.Fatalf("OnResume ran %d times", len(resumed))
	}
	s, err := api.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref("s1", "a1")})
	if err != nil || s.Actor.Status.State != ateapi.ActorState_ACTOR_STATE_SUSPENDED || s.Actor.Status.WorkerAssignment != nil || len(s.Actor.Status.Snapshots) != 2 {
		t.Fatalf("suspend: %v %v", s, err)
	}
	if _, err := api.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref("s1", "a1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref("s1", "a1")}); code(err) != codes.NotFound {
		t.Fatalf("second delete: %v", err)
	}
	if c.World.Policy("s1", "a1") != nil {
		t.Fatal("egress policy outlived its actor")
	}

	want := []string{"CreateAtespace s1", "CreateActorTemplate s1/t1", "GetActorTemplate s1/t1", "GetActorTemplate s1/t1", "CreateActor s1/a1",
		"CreateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "UpdateActorEgressPolicy s1/a1", "ResumeActor s1/a1", "SuspendActor s1/a1", "DeleteActor s1/a1", "DeleteActor s1/a1"}
	if got := c.World.CallNames(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestSchedulingAndCrash(t *testing.T) {
	ctx := context.Background()
	c := start(t, Options{NoServers: true, Scenario: func(w *World) {
		w.AddPool(Pool{Name: "tiny", Replicas: 1, Ready: 1})
		w.AddTemplate(TemplateSpec{Atespace: "s", Name: "t", Image: "i", Selector: map[string]string{"pool": "tiny"}, Created: w.Now()}.Proto())
	}})
	api := c.Client()
	for _, n := range []string{"a", "b"} {
		if _, err := api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "s", Name: n}, ActorTemplate: ref("s", "t")}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s", "a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s", "b")}); code(err) != codes.ResourceExhausted {
		t.Fatalf("resume with no free worker: %v", err)
	}
	if err := c.World.Crash("s", "a", "OOM"); err != nil {
		t.Fatal(err)
	}
	a, _ := api.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref("s", "a")})
	if a.Status.State != ateapi.ActorState_ACTOR_STATE_CRASHED || a.Status.WorkerAssignment != nil || a.Status.AssignedNode == "" {
		t.Fatalf("crashed actor: %v", a.Status)
	}
	if _, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s", "a")}); code(err) != codes.FailedPrecondition {
		t.Fatalf("resume crashed: %v", err)
	}
	// The crash freed the worker.
	if _, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s", "b")}); err != nil {
		t.Fatal(err)
	}
	if r, err := api.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: ref("s", "a")}); err != nil || r.Actor.Status.Crash != nil {
		t.Fatalf("revert: %v %v", r, err)
	}
	// Draining leaves the actor alone but stops new placements.
	if _, err := api.DrainWorker(ctx, &ateapi.DrainWorkerRequest{Worker: &ateapi.ObjectRef{Name: "tiny-1"}}); err != nil {
		t.Fatal(err)
	}
	if a, _ := api.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref("s", "b")}); a.Status.State != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Fatal("drain moved the actor")
	}
}

func TestFaults(t *testing.T) {
	ctx := context.Background()
	c := start(t, Options{NoServers: true})
	api := c.Client()
	c.World.Inject("DeleteActor", Fault{Code: codes.Unavailable, Times: 2})
	c.World.Inject("GetActor", Fault{Delay: 50 * time.Millisecond})
	for i := 0; i < 2; i++ {
		if _, err := api.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref("s", "x")}); code(err) != codes.Unavailable {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if _, err := api.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref("s", "x")}); code(err) != codes.NotFound {
		t.Fatalf("after the fault ran out: %v", err)
	}
	t0 := time.Now()
	_, _ = api.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref("s", "x")})
	if time.Since(t0) < 50*time.Millisecond {
		t.Fatal("delay not applied")
	}
}

func TestRouter(t *testing.T) {
	ctx := context.Background()
	c := start(t, Options{NoServers: true, Scenario: func(w *World) {
		Basic(w)
		w.AddTemplate(TemplateSpec{Atespace: "s", Name: "t", Image: "i", Created: w.Now()}.Proto())
	}})
	api := c.Client()
	_, _ = api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "s", Name: "a"}, ActorTemplate: ref("s", "t")}})
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	go func() {
		conn, err := lis.Accept()
		if err == nil {
			_, _ = conn.Write([]byte("hi"))
			conn.Close()
		}
	}()
	c.Router.Expose("s", "a", 8080, lis.Addr().String())
	if _, err := c.Router.Dial(ctx, "s", "a", 8080); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("dial suspended: %v", err)
	}
	_, _ = api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s", "a")})
	conn, err := c.Router.Dial(ctx, "s", "a", 8080)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(conn)
	if string(b) != "hi" {
		t.Fatalf("read %q", b)
	}
}

// The network path a real client takes: trust bundle and token from the
// Kubernetes API, then ateapi over TLS.
func TestNetwork(t *testing.T) {
	ctx := context.Background()
	c := start(t, Options{Scenario: Demo})
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.CAPEM)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	kget := func(path string) map[string]any {
		req, _ := http.NewRequest("GET", c.KubeURL+path, nil)
		req.Header.Set("Authorization", "Bearer "+c.Token)
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return m
	}
	ctb := kget("/apis/certificates.k8s.io/v1/clustertrustbundles?labelSelector=podcert.ate.dev%2Fcanarying%3Dlive")
	items := ctb["items"].([]any)
	bundle := items[0].(map[string]any)["spec"].(map[string]any)["trustBundle"].(string)
	pools := kget("/apis/ate.dev/v1/workerpools")["items"].([]any)
	if len(pools) != 2 {
		t.Fatalf("%d WorkerPools", len(pools))
	}
	klog := func(query string) string {
		req, _ := http.NewRequest("GET", c.KubeURL+"/api/v1/namespaces/ate-workers/pods/gvisor-pool-a-1/log?"+query, nil)
		req.Header.Set("Authorization", "Bearer "+c.Token)
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	// The pod log keeps the previous actor's lines; sinceTime drops them.
	if full := klog("timestamps=true"); !strings.Contains(full, "212 alerts") || !strings.Contains(full, "waiting for input") {
		t.Fatalf("full pod log: %q", full)
	}
	since := c.World.Actor("triage", "triage-7f3a").Metadata.UpdateTime.AsTime().Format(time.RFC3339)
	if logs := klog("timestamps=true&sinceTime=" + url.QueryEscape(since)); strings.Contains(logs, "212 alerts") || !strings.Contains(logs, "waiting for input") {
		t.Fatalf("pod log since %s: %q", since, logs)
	}
	if logs := klog("tailLines=2"); strings.Count(logs, "\n") != 2 {
		t.Fatalf("tailLines=2: %q", logs)
	}

	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(bundle))
	dial := func(token string) ateapi.ControlClient {
		conn, err := grpc.NewClient(c.APIEndpoint,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: APIServerName})),
			grpc.WithPerRPCCredentials(bearer(token)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return ateapi.NewControlClient(conn)
	}
	if _, err := dial("wrong").ListAtespaces(ctx, &ateapi.ListAtespacesRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	api := dial(c.Token)
	n := 0
	var tok string
	for {
		r, err := api.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: "triage", Options: &ateapi.ListOptions{PageSize: 50, PageToken: tok}})
		if err != nil {
			t.Fatal(err)
		}
		n += len(r.Actors)
		if tok = r.NextPageToken; tok == "" {
			break
		}
	}
	if n != 186 {
		t.Fatalf("triage has %d actors", n)
	}
}

type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return true }

func promQuery(t *testing.T, base, path string, v url.Values) []any {
	t.Helper()
	resp, err := http.Get(base + path + "?" + v.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r struct {
		Status, Error string
		Data          struct{ Result []any }
	}
	_ = json.NewDecoder(resp.Body).Decode(&r)
	if r.Status != "success" {
		t.Fatalf("%s: %s", v.Get("query"), r.Error)
	}
	return r.Data.Result
}

func TestPrometheus(t *testing.T) {
	for _, names := range []MetricNames{DottedNames, UnderscoreNames} {
		c := start(t, Options{Scenario: Demo, Names: names})
		n := nameSets[names]
		sel := func(metric string, m ...string) string {
			if names == DottedNames {
				return `{"` + metric + `", ` + strings.Join(m, ", ") + "}"
			}
			return metric + "{" + strings.Join(m, ", ") + "}"
		}
		q := func(s string) string {
			if names == DottedNames {
				return `"` + s + `"`
			}
			return s
		}
		p95 := `1000 * histogram_quantile(0.95, sum by (le, ` + q(n.tmplName) + `) (rate(` + sel(n.bucket, q(n.op)+`="resume"`) + `[5m])))`
		res := promQuery(t, c.PrometheusURL, "/api/v1/query", url.Values{"query": {p95}})
		got := map[string]float64{}
		for _, r := range res {
			m := r.(map[string]any)
			v, _ := strconv.ParseFloat(m["value"].([]any)[1].(string), 64)
			got[m["metric"].(map[string]any)[n.tmplName].(string)] = v
		}
		// histogram_quantile interpolates within buckets, so allow 10%.
		for tmpl, want := range map[string]float64{"agent-devbox-v12": 395, "mcp-enrich-v3": 250, "codex-review": 610, "agent-devbox-v11": 890} {
			if g := got[tmpl]; g < want*0.85 || g > want*1.15 {
				t.Errorf("%v: p95 %s = %.0f ms, want about %.0f", names, tmpl, g, want)
			}
		}
		crashes := `sum(increase(` + sel(n.crashes, q(n.tmplSpace)+`="triage"`) + `[60m]))`
		res = promQuery(t, c.PrometheusURL, "/api/v1/query", url.Values{"query": {crashes}})
		v, _ := strconv.ParseFloat(res[0].(map[string]any)["value"].([]any)[1].(string), 64)
		if v < 2.5 || v > 4 {
			t.Errorf("%v: crashes in the last hour = %.2f, want 3", names, v)
		}
		now := time.Now()
		res = promQuery(t, c.PrometheusURL, "/api/v1/query_range", url.Values{"query": {p95}, "start": {strconv.FormatInt(now.Add(-6*time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(now.Unix(), 10)}, "step": {"360"}})
		if len(res) != 4 {
			t.Errorf("%v: %d series in range query, want 4", names, len(res))
		}
	}
}

// Failed resumes share the latency histogram with successful ones, as on a
// real cluster, so a p95 that doesn't filter error.type is dragged down by
// fast rejections when a pool is full.
func TestFailedResumesAreFast(t *testing.T) {
	c := start(t, Options{Names: UnderscoreNames, Scenario: func(w *World) {
		Basic(w)
		w.AddTemplate(TemplateSpec{Atespace: "a", Name: "full", Image: "img", Selector: map[string]string{"pool": "default"}, Created: time.Now().Add(-2 * time.Hour)}.Proto())
		w.SetProfile("a", "full", Profile{
			ResumesPerSecond:   func(time.Time) float64 { return 1 },
			ResumeP95:          func(time.Time) time.Duration { return 400 * time.Millisecond },
			ResumeFailureRatio: func(time.Time) float64 { return 0.8 },
		})
	}})
	p95 := func(filter string) float64 {
		t.Helper()
		q := `1000 * histogram_quantile(0.95, sum by (le) (rate(ate_actor_lifecycle_operation_duration_seconds_bucket{ate_actor_operation_name="resume"` + filter + `}[5m])))`
		res := promQuery(t, c.PrometheusURL, "/api/v1/query", url.Values{"query": {q}})
		if len(res) != 1 {
			t.Fatalf("%s: %d series, want 1", q, len(res))
		}
		v, _ := strconv.ParseFloat(res[0].(map[string]any)["value"].([]any)[1].(string), 64)
		return v
	}
	if all := p95(""); all < 100 || all > 360 {
		// With 80% failing in ~2.5 ms, p95 is the 75th percentile of the
		// successful resumes: well under 400 ms but far from the failures.
		t.Errorf("unfiltered p95 = %.0f ms, want between 100 and 360", all)
	}
	if ok := p95(`, error_type=""`); ok < 340 || ok > 460 {
		t.Errorf("successful p95 = %.0f ms, want about 400", ok)
	}
	fails := `sum(rate(ate_actor_lifecycle_operation_duration_seconds_bucket{ate_actor_operation_name="resume", error_type="ResourceExhausted", le="0.01"}[5m])) / sum(rate(ate_actor_lifecycle_operation_duration_seconds_count{ate_actor_operation_name="resume", error_type="ResourceExhausted"}[5m]))`
	res := promQuery(t, c.PrometheusURL, "/api/v1/query", url.Values{"query": {fails}})
	if v, _ := strconv.ParseFloat(res[0].(map[string]any)["value"].([]any)[1].(string), 64); v < 0.9 {
		t.Errorf("share of failed resumes under 10 ms = %.2f, want nearly all", v)
	}
}

// update_time moves on any write, so the assignment says when an actor
// arrived on its worker.
func TestAssignmentMarksArrival(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC))
	c := start(t, Options{NoServers: true, Clock: clock})
	api := c.Client()
	c.World.AddTemplate(TemplateSpec{Atespace: "s1", Name: "t1", Image: "img:1"}.Proto())
	if _, err := api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "s1", Name: "a1"}, ActorTemplate: ref("", "t1")}}); err != nil {
		t.Fatal(err)
	}
	r, err := api.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref("s1", "a1")})
	if err != nil {
		t.Fatal(err)
	}
	resumedAt := clock.Now()
	worker := r.Actor.Status.WorkerAssignment.GetWorker()

	clock.Advance(5 * time.Minute)
	c.World.Touch("s1", "a1") // say, a snapshot report
	a, _ := api.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref("s1", "a1")})
	if !a.Metadata.UpdateTime.AsTime().Equal(clock.Now()) {
		t.Fatalf("update_time %v after a write at %v", a.Metadata.UpdateTime.AsTime(), clock.Now())
	}
	l, err := api.ListWorkerActorAssignments(ctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: worker})
	if err != nil || len(l.ActorAssignments) != 1 {
		t.Fatalf("assignments: %v %v", l, err)
	}
	if as := l.ActorAssignments[0]; as.ActorUid != a.Metadata.Uid || !as.Metadata.CreateTime.AsTime().Equal(resumedAt) {
		t.Fatalf("assignment %v, want uid %s created %v", as, a.Metadata.Uid, resumedAt)
	}

	if _, err := api.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref("s1", "a1")}); err != nil {
		t.Fatal(err)
	}
	if l, _ := api.ListWorkerActorAssignments(ctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: worker}); len(l.GetActorAssignments()) != 0 {
		t.Fatalf("assignment outlived suspend: %v", l)
	}
}
