package substratefake

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
)

// Basic is an empty cluster with one pool of 16 gVisor workers, so actors
// can be created and resumed without further setup. Callers create
// atespaces and templates themselves.
func Basic(w *World) {
	w.Name = "fake"
	w.AddPool(Pool{Name: "default", Labels: map[string]string{"pool": "default"}, SandboxClass: "gvisor", Replicas: 16, Ready: 16})
}

// TemplateSpec is a shorthand for seeding templates.
type TemplateSpec struct {
	Atespace, Name, Image string
	Command               []string
	CPUMillis             int64
	MemoryBytes           int64
	Selector              map[string]string
	Fidelity              ateapi.SnapshotFidelity
	Created               time.Time
	// GoldenError fails the golden snapshot; GoldenOff disables it.
	GoldenError string
	GoldenOff   bool
}

// Proto builds the ActorTemplate, with a ready golden snapshot unless
// GoldenError or GoldenOff say otherwise.
func (s TemplateSpec) Proto() *ateapi.ActorTemplate {
	mode := ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED
	if s.GoldenOff {
		mode = ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED
	}
	t := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: s.Atespace, Name: s.Name, CreateTime: ts(s.Created)},
		Containers: []*ateapi.Container{{
			Name: "agent", Image: s.Image, Command: s.Command,
			Env:         []*ateapi.EnvVar{{Name: "LOG_LEVEL", Value: "info"}, {Name: "MODEL_ENDPOINT", Value: "http://gateway.ate-system.svc"}},
			WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Path: "/readyz", Port: 8080}},
		}},
		Resources:      &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: s.CPUMillis, MemoryBytes: s.MemoryBytes}},
		WorkerSelector: &ateapi.Selector{MatchLabels: s.Selector},
		SnapshotConfig: &ateapi.SnapshotConfig{PreferredFidelity: s.Fidelity, GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: mode}},
		Status:         &ateapi.ActorTemplateStatus{},
	}
	switch {
	case s.GoldenOff:
	case s.GoldenError != "":
		t.Status.GoldenSnapshotStatus = &ateapi.GoldenSnapshotStatus{ErrorMessage: s.GoldenError}
	default:
		t.Status.GoldenSnapshotStatus = &ateapi.GoldenSnapshotStatus{
			GoldenTag: &ateapi.ObjectRef{Atespace: s.Atespace, Name: s.Name + "-golden"}, SnapshotTime: ts(s.Created.Add(30 * time.Second)),
		}
	}
	return t
}

const gib = int64(1) << 30

// Demo is a 412-actor cluster with a few real problems in it:
//
//   - three OOM crashes from agent-devbox-v11 on one node, while v12 (twice
//     the memory) has none;
//   - ioc-enrich-02 stuck resuming for 9 minutes;
//   - gvisor-pool-a 39 of 40 workers busy, with resumes starting to fail
//     with ResourceExhausted;
//   - mcp-enrich-v4's golden snapshot failed on an image pull.
//
// New templates take 25 seconds to get a golden snapshot, and resumes take
// half a second.
// Demo principals: an on-call engineer who can see the whole cluster, and a
// developer who can change the triage atespace and see nothing else.
const (
	DemoOnCall    = "system:serviceaccount:ate-users:oncall"
	DemoTriageDev = "system:serviceaccount:ate-users:triage-dev"
)

func Demo(w *World) {
	w.Name = "demo-cluster"
	// Two people besides the shared ate-client owner.
	w.Grant("", DemoOnCall, RoleViewer)
	w.Grant("triage", DemoTriageDev, RoleEditor)
	w.GoldenDelay = 25 * time.Second
	w.ResumeDelay = 500 * time.Millisecond
	now := w.Now()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	r := rand.New(rand.NewPCG(7, 7))
	hex := func() string { return fmt.Sprintf("%04x", r.IntN(0xffff)) }

	gvisor := map[string]string{"pool": "gvisor-pool-a"}
	microvm := map[string]string{"pool": "microvm-pool-b"}
	w.AddPool(Pool{Name: "gvisor-pool-a", Labels: gvisor, SandboxClass: "gvisor", Replicas: 40, Ready: 40})
	w.AddPool(Pool{Name: "microvm-pool-b", Labels: microvm, SandboxClass: "microvm", Replicas: 8, Ready: 8})

	mem := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	for _, s := range []TemplateSpec{
		{Atespace: "triage", Name: "agent-devbox-v11", Image: "ghcr.io/example/agent-devbox:11", Command: []string{"node", "server.js"}, CPUMillis: 2000, MemoryBytes: 2 * gib, Selector: gvisor, Fidelity: mem, Created: ago(9 * 24 * time.Hour)},
		{Atespace: "triage", Name: "agent-devbox-v12", Image: "ghcr.io/example/agent-devbox:12", Command: []string{"node", "server.js"}, CPUMillis: 2000, MemoryBytes: 4 * gib, Selector: gvisor, Fidelity: mem, Created: ago(3 * 24 * time.Hour)},
		{Atespace: "ioc-enrich", Name: "mcp-enrich-v3", Image: "ghcr.io/example/mcp-enrich:3", Command: []string{"python", "-m", "enrich.server"}, CPUMillis: 500, MemoryBytes: gib, Selector: gvisor, Fidelity: mem, Created: ago(12 * 24 * time.Hour)},
		{Atespace: "ioc-enrich", Name: "mcp-enrich-v4", Image: "ghcr.io/example/mcp-enrich:4", Command: []string{"python", "-m", "enrich.server"}, CPUMillis: 500, MemoryBytes: gib, Selector: gvisor, Fidelity: mem, Created: ago(40 * time.Minute),
			GoldenError: "ImagePullFailed: ghcr.io/example/mcp-enrich:4: manifest unknown"},
		{Atespace: "code-review", Name: "codex-review", Image: "ghcr.io/example/codex-review:5", Command: []string{"codex", "app-server"}, CPUMillis: 4000, MemoryBytes: 4 * gib, Selector: microvm, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, Created: ago(20 * 24 * time.Hour)},
	} {
		w.AddTemplate(s.Proto())
	}

	actor := func(space, name, tmpl string, state ateapi.ActorState, since time.Time) *ateapi.Actor {
		created := ago(time.Duration(2+r.IntN(400)) * time.Hour)
		if created.After(since) {
			created = since.Add(-time.Hour)
		}
		return &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: space, Name: name, CreateTime: ts(created), UpdateTime: ts(since)},
			ActorTemplate: &ateapi.ObjectRef{Atespace: space, Name: tmpl},
			Status:        &ateapi.ActorStatus{State: state},
		}
	}
	durable := func(gen int, fid ateapi.SnapshotFidelity) *ateapi.Snapshot {
		return &ateapi.Snapshot{Generation: int32(gen), Fidelity: fid,
			DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}
	}
	suspended := func(space, name, tmpl string, idle time.Duration) {
		a := actor(space, name, tmpl, ateapi.ActorState_ACTOR_STATE_SUSPENDED, ago(idle))
		a.Status.Snapshots = []*ateapi.Snapshot{durable(3+r.IntN(20), mem)}
		w.AddActor(a)
	}
	next := map[string]int{}
	running := func(space, name, tmpl, pool string, since time.Duration) {
		next[pool]++
		a := actor(space, name, tmpl, ateapi.ActorState_ACTOR_STATE_RUNNING, ago(since))
		a.Status.WorkerAssignment = &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: fmt.Sprintf("%s-%d", pool, next[pool])}}
		a.Status.Snapshots = []*ateapi.Snapshot{durable(2+r.IntN(20), mem)}
		w.AddActor(a)
	}

	// triage: three OOM crashes on one node, all agent-devbox-v11.
	for _, c := range []struct {
		name string
		d    time.Duration
	}{{"triage-0d55", 6 * time.Minute}, {"triage-77b2", 31 * time.Minute}, {"triage-a1f9", 48 * time.Minute}} {
		a := actor("triage", c.name, "agent-devbox-v11", ateapi.ActorState_ACTOR_STATE_CRASHED, ago(c.d))
		a.Status.AssignedNode = "kind-worker3"
		a.Status.Crash = &ateapi.ActorCrash{CrashTime: ts(ago(c.d)),
			Message: `container "agent" exceeded memory limit 2Gi (working set 2.04Gi) and was killed by the sandbox on kind-worker3`}
		a.Status.Snapshots = []*ateapi.Snapshot{durable(14, mem)}
		w.AddActor(a)
	}
	running("triage", "triage-7f3a", "agent-devbox-v12", "gvisor-pool-a", 12*time.Minute)
	// The worker's log starts with whatever ran there before triage-7f3a.
	_ = w.AppendLog("triage", "triage-7f3a",
		LogLine{ago(55 * time.Minute), "session resumed from generation 3"},
		LogLine{ago(54 * time.Minute), "triage: classified 212 alerts, 3 escalated"},
		LogLine{ago(41 * time.Minute), "idle 10m, ready to suspend"},
	)
	_ = w.AppendLog("triage", "triage-7f3a",
		LogLine{ago(12 * time.Minute), "session resumed from generation 9"},
		LogLine{ago(11 * time.Minute), "bash: npm install (412 packages)"},
		LogLine{ago(10 * time.Minute), "node --max-old-space-size=3072 build.js"},
		LogLine{ago(9 * time.Minute), "[build] bundling 1,904 modules"},
		LogLine{ago(2 * time.Minute), "[build] done in 7m12s"},
		LogLine{ago(20 * time.Second), "waiting for input"},
	)
	running("triage", "triage-c41e", "agent-devbox-v12", "gvisor-pool-a", 3*time.Minute)
	for i := 0; i < 30; i++ {
		running("triage", "triage-"+hex(), "agent-devbox-v12", "gvisor-pool-a", time.Duration(r.IntN(1800))*time.Second)
	}
	suspended("triage", "triage-91c0", "agent-devbox-v12", 2*time.Minute)
	suspended("triage", "triage-2a90", "agent-devbox-v12", 41*time.Minute)
	suspended("triage", "triage-5e18", "agent-devbox-v12", 3*time.Hour)
	for i := 0; i < 148; i++ {
		tmpl := "agent-devbox-v12"
		if i%5 == 0 {
			tmpl = "agent-devbox-v11"
		}
		suspended("triage", "triage-"+hex(), tmpl, time.Duration(10+r.IntN(60*24*9))*time.Minute)
	}

	// ioc-enrich: quiet, except one actor stuck resuming. It holds a worker
	// and never finishes.
	for i := 0; i < 6; i++ {
		running("ioc-enrich", fmt.Sprintf("ioc-enrich-%02d", 10+i), "mcp-enrich-v3", "gvisor-pool-a", time.Duration(r.IntN(3600))*time.Second)
	}
	stuck := actor("ioc-enrich", "ioc-enrich-02", "mcp-enrich-v3", ateapi.ActorState_ACTOR_STATE_RESUMING, ago(9*time.Minute))
	next["gvisor-pool-a"]++
	stuck.Status.WorkerAssignment = &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: fmt.Sprintf("gvisor-pool-a-%d", next["gvisor-pool-a"])}}
	stuck.Status.Snapshots = []*ateapi.Snapshot{durable(31, mem)}
	w.AddActor(stuck)
	for i := 0; i < 143; i++ {
		suspended("ioc-enrich", fmt.Sprintf("ioc-enrich-%02d", 20+i), "mcp-enrich-v3", time.Duration(5+r.IntN(60*24*3))*time.Minute)
	}

	// code-review: runs on microVMs.
	for i := 0; i < 2; i++ {
		running("code-review", fmt.Sprintf("codex-review-%d", i+1), "codex-review", "microvm-pool-b", 20*time.Minute)
	}
	for i := 0; i < 74; i++ {
		suspended("code-review", fmt.Sprintf("codex-review-%d", i+3), "codex-review", time.Duration(30+r.IntN(60*24*14))*time.Minute)
	}
	// 38 running gvisor actors plus the stuck one hold 39 of gvisor-pool-a's
	// 40 workers.

	// Traffic. Times are relative to now, so the story holds for a few hours
	// after start.
	wobble := func(t time.Time, amp, period float64) float64 {
		return amp * math.Sin(float64(t.Unix())/period)
	}
	// ramp goes 0 → 1 over the six hours before now.
	ramp := func(t time.Time) float64 { return math.Min(math.Max(1-now.Sub(t).Hours()/6, 0), 1) }
	full := func(t time.Time) float64 {
		if now.Sub(t) < 20*time.Minute {
			return 0.041
		}
		return 0
	}
	ms := func(v float64) time.Duration { return time.Duration(v * float64(time.Millisecond)) }
	w.SetProfile("triage", "agent-devbox-v11", Profile{
		ResumesPerSecond:   func(t time.Time) float64 { return 0.06 + wobble(t, 0.01, 900) },
		ResumeP95:          func(t time.Time) time.Duration { return ms(440 + 450*math.Pow(ramp(t), 2.2) + wobble(t, 12, 700)) },
		ResumeFailureRatio: full,
	})
	w.SetProfile("triage", "agent-devbox-v12", Profile{
		ResumesPerSecond:   func(t time.Time) float64 { return 0.3 + wobble(t, 0.05, 1100) },
		ResumeP95:          func(t time.Time) time.Duration { return ms(395 + wobble(t, 10, 800)) },
		ResumeFailureRatio: full,
	})
	w.SetProfile("ioc-enrich", "mcp-enrich-v3", Profile{
		ResumesPerSecond:   func(t time.Time) float64 { return 0.2 + wobble(t, 0.03, 1300) },
		ResumeP95:          func(t time.Time) time.Duration { return ms(250 + wobble(t, 8, 600)) },
		ResumeFailureRatio: full,
	})
	w.SetProfile("ioc-enrich", "mcp-enrich-v4", Profile{})
	w.SetProfile("code-review", "codex-review", Profile{
		ResumesPerSecond: func(t time.Time) float64 { return 0.02 },
		ResumeP95:        func(t time.Time) time.Duration { return ms(610 + wobble(t, 20, 1500)) },
	})
}
