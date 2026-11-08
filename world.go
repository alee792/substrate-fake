package substratefake

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Clock tells the world what time it is. Tests that need to move time pass
// a ManualClock; everything else uses the wall clock.
type Clock interface{ Now() time.Time }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// ManualClock is a Clock that only moves when told to.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a clock stopped at t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now implements Clock.
func (c *ManualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves the clock forward.
func (c *ManualClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Pool is a WorkerPool: the Kubernetes object that sets a pool's size,
// labels and sandbox class. Its ready workers appear in ateapi as Workers.
type Pool struct {
	Namespace    string
	Name         string
	Labels       map[string]string
	SandboxClass string // gvisor or microvm
	Replicas     int    // spec.replicas
	Ready        int    // status.readyReplicas; this many Workers exist
	// NotReadySince is when the pool fell short of Replicas, if it has.
	NotReadySince time.Time
	// Nodes spreads workers across this many nodes (kind-worker1..N).
	Nodes int
}

// LogLine is one line of an actor's output, as its worker pod logs it.
type LogLine struct {
	Time time.Time
	Text string
}

// Profile is a template's traffic over time, which the fake Prometheus turns
// into ate.actor.lifecycle.operation.duration series. Any field may be nil.
type Profile struct {
	// ResumesPerSecond is how often actors of the template resume.
	ResumesPerSecond func(t time.Time) float64
	// ResumeP95 is the 95th percentile resume latency.
	ResumeP95 func(t time.Time) time.Duration
	// ResumeFailureRatio is the share of resumes that fail with
	// ResourceExhausted, 0 to 1.
	ResumeFailureRatio func(t time.Time) float64
}

// Call is one ateapi call the world served, in order.
type Call struct {
	Method string // e.g. "ResumeActor"
	Target string // "atespace/name", or "" when the call has none
	Time   time.Time
	Code   codes.Code
}

func (c Call) String() string {
	if c.Target == "" {
		return c.Method
	}
	return c.Method + " " + c.Target
}

// Fault makes an ateapi method fail or slow down.
type Fault struct {
	Code    codes.Code    // codes.OK only delays
	Message string        // default: "injected fault"
	Delay   time.Duration // before answering (or failing)
	Times   int           // how many calls it affects; 0 means every call
}

// CrashEvent records a crash for the ate.actor.crashes counter.
type CrashEvent struct {
	Time               time.Time
	Atespace, Template string
	Pool               string
}

type pending struct {
	at time.Time
	f  func()
}

// World is the fake cluster's state. Every fake interface reads and writes
// it, so a write through ateapi shows up in Kubernetes and Prometheus too.
// Its methods are safe for concurrent use.
type World struct {
	mu    sync.Mutex
	clock Clock
	rng   *rand.Rand

	// Name is the cluster name the kubeconfig gives its context.
	Name string

	atespaces map[string]*ateapi.Atespace
	templates map[string]*ateapi.ActorTemplate // "atespace/name"
	actors    map[string]*ateapi.Actor
	policies  map[string]*ateapi.EgressPolicy // by actor
	workers   map[string]*ateapi.Worker
	pools     []*Pool
	logs      map[string][]LogLine // by worker pod, "namespace/pod"
	bound     map[string]time.Time // when each assigned actor was bound, by actor UID
	crashes   []CrashEvent
	profiles  map[string]Profile // by template
	goldenErr map[string]string  // by template, applied at creation
	pending   []pending
	calls     []Call
	faults    map[string][]*Fault
	onResume  []func(*ateapi.ObjectRef)

	// GoldenDelay is how long a new template's golden snapshot takes. Zero
	// makes it ready at once.
	GoldenDelay time.Duration
	// ResumeDelay is how long an actor stays RESUMING. Zero makes
	// ResumeActor return a RUNNING actor.
	ResumeDelay time.Duration
	// DefaultProfile applies to templates without their own.
	DefaultProfile Profile
}

// NewWorld returns an empty world: no atespaces, templates, actors or pools.
// Scenarios fill it in.
func NewWorld(clock Clock) *World {
	if clock == nil {
		clock = wallClock{}
	}
	return &World{
		clock: clock, rng: rand.New(rand.NewPCG(1, 2)), Name: "fake",
		atespaces: map[string]*ateapi.Atespace{}, templates: map[string]*ateapi.ActorTemplate{},
		actors: map[string]*ateapi.Actor{}, policies: map[string]*ateapi.EgressPolicy{},
		workers: map[string]*ateapi.Worker{}, logs: map[string][]LogLine{}, bound: map[string]time.Time{},
		profiles: map[string]Profile{}, goldenErr: map[string]string{}, faults: map[string][]*Fault{},
		DefaultProfile: Profile{
			ResumesPerSecond: func(time.Time) float64 { return 0.02 },
			ResumeP95:        func(time.Time) time.Duration { return 400 * time.Millisecond },
		},
	}
}

// Now is the world's current time.
func (w *World) Now() time.Time { return w.clock.Now() }

func key(atespace, name string) string  { return atespace + "/" + name }
func refKey(r *ateapi.ObjectRef) string { return key(r.GetAtespace(), r.GetName()) }
func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func (w *World) uid() string {
	return fmt.Sprintf("%08x-%04x-4%03x-a%03x-%012x", w.rng.Uint32(), w.rng.Uint32()&0xffff, w.rng.Uint32()&0xfff, w.rng.Uint32()&0xfff, w.rng.Uint64()&0xffffffffffff)
}

// meta fills in uid, version and timestamps for a new record.
func (w *World) meta(m *ateapi.ResourceMetadata, now time.Time) *ateapi.ResourceMetadata {
	if m == nil {
		m = &ateapi.ResourceMetadata{}
	}
	if m.Uid == "" {
		m.Uid = w.uid()
	}
	if m.Version == 0 {
		m.Version = 1
	}
	if m.CreateTime == nil {
		m.CreateTime = ts(now)
	}
	if m.UpdateTime == nil {
		m.UpdateTime = m.CreateTime
	}
	return m
}

// Touch writes to an actor without changing its state, as ateapi does when
// atelet reports a snapshot, a tag is taken, or UpdateActor is called. It
// moves metadata.update_time, which is why update_time doesn't say when the
// state began.
func (w *World) Touch(atespace, name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	if a := w.actors[key(atespace, name)]; a != nil {
		touch(a.Metadata, w.clock.Now())
	}
}

func touch(m *ateapi.ResourceMetadata, now time.Time) {
	m.Version++
	m.UpdateTime = ts(now)
}

// tick applies transitions that are due. Callers hold w.mu.
func (w *World) tick() {
	now := w.clock.Now()
	var later []pending
	for _, p := range w.pending {
		if p.at.After(now) {
			later = append(later, p)
			continue
		}
		p.f()
	}
	w.pending = later
}

func (w *World) after(d time.Duration, f func()) {
	w.pending = append(w.pending, pending{at: w.clock.Now().Add(d), f: f})
}

// ---------- seeding ----------

// AddAtespace creates an atespace if it doesn't exist.
func (w *World) AddAtespace(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.addAtespace(name, w.clock.Now())
}

func (w *World) addAtespace(name string, now time.Time) {
	if _, ok := w.atespaces[name]; !ok {
		w.atespaces[name] = &ateapi.Atespace{Metadata: w.meta(&ateapi.ResourceMetadata{Name: name}, now)}
	}
}

// AddPool adds a WorkerPool and registers its ready workers.
func (w *World) AddPool(p Pool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p.Namespace == "" {
		p.Namespace = "ate-workers"
	}
	if p.Labels == nil {
		p.Labels = map[string]string{"pool": p.Name}
	}
	if p.Nodes == 0 {
		p.Nodes = 4
	}
	if p.SandboxClass == "" {
		p.SandboxClass = "gvisor"
	}
	pp := p
	w.pools = append(w.pools, &pp)
	now := w.clock.Now()
	for i := 1; i <= p.Ready; i++ {
		name := fmt.Sprintf("%s-%d", p.Name, i)
		w.workers[name] = &ateapi.Worker{
			Metadata:        w.meta(&ateapi.ResourceMetadata{Name: name}, now.Add(-time.Duration(48+i)*time.Hour)),
			WorkerNamespace: p.Namespace, WorkerPool: p.Name, WorkerPod: name, WorkerPodUid: w.uid(),
			NodeName: fmt.Sprintf("kind-worker%d", (i-1)%p.Nodes+1), Labels: maps.Clone(p.Labels),
			Status: &ateapi.WorkerStatus{
				State:          ateapi.WorkerState_WORKER_STATE_ACTIVE,
				Capacity:       &ateapi.WorkerResources{Actors: 1, CpuMillicores: 4000, MemoryBytes: 8 << 30},
				Allocated:      &ateapi.WorkerResources{},
				SandboxClasses: []*ateapi.WorkerSandboxClass{{SandboxClass: p.SandboxClass}},
			},
		}
	}
}

// AddTemplate stores a template as given, status included, so scenarios can
// seed ready, building or failed golden snapshots. It creates the atespace.
func (w *World) AddTemplate(t *ateapi.ActorTemplate) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock.Now()
	t = proto.Clone(t).(*ateapi.ActorTemplate)
	t.Metadata = w.meta(t.Metadata, now)
	w.addAtespace(t.Metadata.Atespace, now)
	w.templates[key(t.Metadata.Atespace, t.Metadata.Name)] = t
}

// AddActor stores an actor as given. If its status assigns a worker, the
// worker counts it; if it holds a crash, the crash counts toward
// ate.actor.crashes.
func (w *World) AddActor(a *ateapi.Actor) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock.Now()
	a = proto.Clone(a).(*ateapi.Actor)
	a.Metadata = w.meta(a.Metadata, now)
	if a.Status == nil {
		a.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
	}
	w.addAtespace(a.Metadata.Atespace, now)
	if wa := a.Status.WorkerAssignment; wa != nil {
		if wk := w.workers[wa.GetWorker().GetName()]; wk != nil {
			w.fillAssignment(wa, wk)
			wk.Status.Allocated.Actors++
			w.bound[a.Metadata.Uid] = a.Metadata.UpdateTime.AsTime()
		}
	}
	if c := a.Status.Crash; c != nil {
		w.crashes = append(w.crashes, CrashEvent{Time: c.CrashTime.AsTime(), Atespace: a.Metadata.Atespace, Template: a.ActorTemplate.GetName(), Pool: w.poolOf(a)})
	}
	w.actors[key(a.Metadata.Atespace, a.Metadata.Name)] = a
}

// AddCrashEvent records a past crash for the crash counter without an actor
// in CRASHED state, e.g. one that has since been reverted.
func (w *World) AddCrashEvent(e CrashEvent) {
	w.mu.Lock()
	w.crashes = append(w.crashes, e)
	w.mu.Unlock()
}

// SetProfile sets a template's traffic profile.
func (w *World) SetProfile(atespace, template string, p Profile) {
	w.mu.Lock()
	w.profiles[key(atespace, template)] = p
	w.mu.Unlock()
}

// AppendLog writes lines to the log of the worker pod an actor is on, as
// its output would be. The lines stay in that pod's log after the actor
// leaves, as in a real worker, so the next actor's readers see them unless
// they read from when it arrived.
func (w *World) AppendLog(atespace, actor string, lines ...LogLine) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	a := w.actors[key(atespace, actor)]
	if a == nil {
		return fmt.Errorf("actor %s/%s not found", atespace, actor)
	}
	wa := a.Status.GetWorkerAssignment()
	if wa == nil {
		return fmt.Errorf("actor %s/%s has no worker to log on", atespace, actor)
	}
	k := key(wa.WorkerNamespace, wa.WorkerPod)
	w.logs[k] = append(w.logs[k], lines...)
	sort.SliceStable(w.logs[k], func(i, j int) bool { return w.logs[k][i].Time.Before(w.logs[k][j].Time) })
	return nil
}

// ---------- driving ----------

// Crash moves an actor to CRASHED, as a worker's ReportActorCrashed would:
// the crash is recorded, the worker is released, and the node is kept.
func (w *World) Crash(atespace, actor, message string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a := w.actors[key(atespace, actor)]
	if a == nil {
		return fmt.Errorf("actor %s/%s not found", atespace, actor)
	}
	now := w.clock.Now()
	pool := w.poolOf(a)
	w.release(a)
	a.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	a.Status.Crash = &ateapi.ActorCrash{Message: message, CrashTime: ts(now)}
	touch(a.Metadata, now)
	w.crashes = append(w.crashes, CrashEvent{Time: now, Atespace: atespace, Template: a.ActorTemplate.GetName(), Pool: pool})
	return nil
}

// FailGolden makes a template's golden snapshot fail with message. Called
// before the template exists, it applies when the template is created.
func (w *World) FailGolden(atespace, template, message string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := key(atespace, template)
	t := w.templates[k]
	if t == nil {
		w.goldenErr[k] = message
		return
	}
	t.Status = &ateapi.ActorTemplateStatus{GoldenSnapshotStatus: &ateapi.GoldenSnapshotStatus{ErrorMessage: message}}
	touch(t.Metadata, w.clock.Now())
}

// SetWorkerState changes a worker's state, e.g. to UNAVAILABLE.
func (w *World) SetWorkerState(name string, s ateapi.WorkerState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	wk := w.workers[name]
	if wk == nil {
		return fmt.Errorf("worker %s not found", name)
	}
	wk.Status.State = s
	touch(wk.Metadata, w.clock.Now())
	return nil
}

// Inject adds a fault to an ateapi method, by its short name ("ResumeActor").
// Faults on a method apply in the order added.
func (w *World) Inject(method string, f Fault) {
	w.mu.Lock()
	ff := f
	w.faults[method] = append(w.faults[method], &ff)
	w.mu.Unlock()
}

// ClearFaults removes every injected fault.
func (w *World) ClearFaults() {
	w.mu.Lock()
	w.faults = map[string][]*Fault{}
	w.mu.Unlock()
}

// fault returns the fault to apply to a call, using it up.
func (w *World) fault(method string) *Fault {
	w.mu.Lock()
	defer w.mu.Unlock()
	fs := w.faults[method]
	for len(fs) > 0 {
		f := fs[0]
		if f.Times == 0 {
			return f
		}
		f.Times--
		if f.Times == 0 {
			fs = fs[1:]
			w.faults[method] = fs
		}
		return f
	}
	return nil
}

// OnResume registers a hook that runs, outside the world's lock, each time
// ResumeActor starts an actor. Tests start the actor's process here.
func (w *World) OnResume(f func(ref *ateapi.ObjectRef)) {
	w.mu.Lock()
	w.onResume = append(w.onResume, f)
	w.mu.Unlock()
}

func (w *World) record(c Call) {
	w.mu.Lock()
	w.calls = append(w.calls, c)
	w.mu.Unlock()
}

// Calls returns every ateapi call so far, in order.
func (w *World) Calls() []Call {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.calls)
}

// CallNames returns Calls as strings like "ResumeActor triage/a1".
func (w *World) CallNames() []string {
	var out []string
	for _, c := range w.Calls() {
		out = append(out, c.String())
	}
	return out
}

// ---------- inspection ----------

// Actor returns a copy of an actor, or nil.
func (w *World) Actor(atespace, name string) *ateapi.Actor {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	if a := w.actors[key(atespace, name)]; a != nil {
		return proto.Clone(a).(*ateapi.Actor)
	}
	return nil
}

// Template returns a copy of a template, or nil.
func (w *World) Template(atespace, name string) *ateapi.ActorTemplate {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	if t := w.templates[key(atespace, name)]; t != nil {
		return proto.Clone(t).(*ateapi.ActorTemplate)
	}
	return nil
}

// Policy returns a copy of an actor's egress policy, or nil.
func (w *World) Policy(atespace, actor string) *ateapi.EgressPolicy {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p := w.policies[key(atespace, actor)]; p != nil {
		return proto.Clone(p).(*ateapi.EgressPolicy)
	}
	return nil
}

// Pools returns copies of the WorkerPools.
func (w *World) Pools() []Pool {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []Pool
	for _, p := range w.pools {
		c := *p
		c.Labels = maps.Clone(p.Labels)
		out = append(out, c)
	}
	return out
}

// ---------- scheduling ----------

func matches(sel, labels map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func (w *World) pool(name string) *Pool {
	for _, p := range w.pools {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// schedule picks a free ACTIVE worker whose pool matches the selector.
func (w *World) schedule(sel map[string]string) *ateapi.Worker {
	names := slices.Sorted(maps.Keys(w.workers))
	sort.SliceStable(names, func(i, j int) bool { return workerIndex(names[i]) < workerIndex(names[j]) })
	for _, n := range names {
		wk := w.workers[n]
		p := w.pool(wk.WorkerPool)
		if p == nil || !matches(sel, p.Labels) || wk.Status.State != ateapi.WorkerState_WORKER_STATE_ACTIVE {
			continue
		}
		if wk.Status.Allocated.Actors < max(wk.Status.Capacity.Actors, 1) {
			return wk
		}
	}
	return nil
}

// workerIndex orders "pool-2" before "pool-10".
func workerIndex(name string) int {
	i := strings.LastIndexByte(name, '-')
	var n int
	fmt.Sscanf(name[i+1:], "%d", &n)
	return n
}

func (w *World) fillAssignment(wa *ateapi.WorkerAssignment, wk *ateapi.Worker) {
	wa.Worker = &ateapi.ObjectRef{Name: wk.Metadata.Name}
	wa.WorkerNamespace, wa.WorkerPool, wa.WorkerPod, wa.WorkerPodUid = wk.WorkerNamespace, wk.WorkerPool, wk.WorkerPod, wk.WorkerPodUid
	wa.NodeName = wk.NodeName
	if p := w.pool(wk.WorkerPool); p != nil {
		wa.SandboxRuntime = &ateapi.ActorSandboxRuntime{SandboxClass: p.SandboxClass}
	}
}

func (w *World) assign(a *ateapi.Actor, wk *ateapi.Worker) {
	wa := &ateapi.WorkerAssignment{}
	w.fillAssignment(wa, wk)
	a.Status.WorkerAssignment = wa
	a.Status.AssignedNode = wk.NodeName
	wk.Status.Allocated.Actors++
	w.bound[a.Metadata.Uid] = w.clock.Now()
}

// release frees an actor's worker. The assigned node stays, as ateapi keeps
// it after a crash.
func (w *World) release(a *ateapi.Actor) {
	wa := a.Status.GetWorkerAssignment()
	if wa == nil {
		return
	}
	if wk := w.workers[wa.GetWorker().GetName()]; wk != nil && wk.Status.Allocated.Actors > 0 {
		wk.Status.Allocated.Actors--
	}
	a.Status.WorkerAssignment = nil
	delete(w.bound, a.Metadata.Uid)
}

// poolOf is the pool an actor runs in, or would be scheduled to.
func (w *World) poolOf(a *ateapi.Actor) string {
	if p := a.Status.GetWorkerAssignment().GetWorkerPool(); p != "" {
		return p
	}
	sel := w.selectorFor(a)
	for _, p := range w.pools {
		if matches(sel, p.Labels) {
			return p.Name
		}
	}
	return ""
}

func (w *World) selectorFor(a *ateapi.Actor) map[string]string {
	if s := a.GetWorkerSelector().GetMatchLabels(); len(s) > 0 {
		return s
	}
	if t := w.templates[key(a.Metadata.Atespace, a.ActorTemplate.GetName())]; t != nil {
		return t.GetWorkerSelector().GetMatchLabels()
	}
	return nil
}

// podLogs returns a worker pod's whole log: every actor that has run there.
func (w *World) podLogs(namespace, pod string) ([]LogLine, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick() // a due transition may move an actor onto this pod
	for _, wk := range w.workers {
		if wk.WorkerNamespace == namespace && wk.WorkerPod == pod {
			return slices.Clone(w.logs[key(namespace, pod)]), true
		}
	}
	return nil, false
}
