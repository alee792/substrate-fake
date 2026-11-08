package substratefake

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// controlServer implements ateapi.v1.Control over a World. Methods that
// Substrate clients rarely call return Unimplemented.
type controlServer struct {
	ateapi.UnimplementedControlServer
	w *World
}

// interceptor records each call and applies injected faults. With auth on,
// it authenticates and authorizes the call as ateapi does (see authz.go);
// token is the raw Options.Token, accepted as ClientPrincipal.
func (w *World) interceptor(auth bool, token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		method := info.FullMethod[strings.LastIndexByte(info.FullMethod, '/')+1:]
		call := Call{Method: method, Target: target(req), Time: w.Now()}
		resp, err := func() (any, error) {
			if auth {
				principal, err := w.access.authenticate(ctx, w.Now(), token)
				if err != nil {
					return nil, err
				}
				call.Principal = principal
				if err := w.access.authorize(principal, info.FullMethod, req); err != nil {
					return nil, err
				}
			}
			if f := w.fault(method); f != nil {
				if f.Delay > 0 {
					select {
					case <-time.After(f.Delay):
					case <-ctx.Done():
						return nil, status.FromContextError(ctx.Err()).Err()
					}
				}
				if f.Code != codes.OK {
					msg := f.Message
					if msg == "" {
						msg = "injected fault"
					}
					return nil, status.Error(f.Code, msg)
				}
			}
			return h(ctx, req)
		}()
		call.Code = status.Code(err)
		w.record(call)
		return resp, err
	}
}

// target finds the object a request is about: an ObjectRef field, or a
// resource's metadata, or the atespace a list is scoped to.
func target(req any) string {
	m, ok := req.(proto.Message)
	if !ok {
		return ""
	}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.Kind() != protoreflect.MessageKind || f.IsList() || f.IsMap() || !r.Has(f) {
			continue
		}
		sub := r.Get(f).Message()
		switch v := sub.Interface().(type) {
		case *ateapi.ObjectRef:
			return refString(v.GetAtespace(), v.GetName())
		}
		if mf := sub.Descriptor().Fields().ByName("metadata"); mf != nil && sub.Has(mf) {
			if md, ok := sub.Get(mf).Message().Interface().(*ateapi.ResourceMetadata); ok {
				return refString(md.GetAtespace(), md.GetName())
			}
		}
	}
	if f := fields.ByName("atespace"); f != nil && f.Kind() == protoreflect.StringKind {
		return r.Get(f).String()
	}
	return ""
}

func refString(atespace, name string) string {
	if atespace == "" {
		return name
	}
	return atespace + "/" + name
}

func notFound(kind, k string) error { return status.Errorf(codes.NotFound, "%s %q not found", kind, k) }

// page slices a sorted list by ListOptions. Tokens are offsets.
func page[T any](items []T, o *ateapi.ListOptions) ([]T, string, error) {
	start := 0
	if tok := o.GetPageToken(); tok != "" {
		n, err := strconv.Atoi(tok)
		if err != nil || n < 0 || n > len(items) {
			return nil, "", status.Error(codes.InvalidArgument, "bad page token")
		}
		start = n
	}
	size := int(o.GetPageSize())
	if size <= 0 || size > 1000 {
		size = 1000
	}
	end := min(start+size, len(items))
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

func clones[T proto.Message](m map[string]T, keep func(T) bool) []T {
	var out []T
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if keep == nil || keep(m[k]) {
			out = append(out, proto.Clone(m[k]).(T))
		}
	}
	return out
}

func clone[T proto.Message](v T) T { return proto.Clone(v).(T) }

// ---------- atespaces ----------

func (s *controlServer) CreateAtespace(_ context.Context, in *ateapi.CreateAtespaceRequest) (*ateapi.Atespace, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	name := in.GetAtespace().GetMetadata().GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace name is required")
	}
	if _, ok := w.atespaces[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "atespace %q already exists", name)
	}
	w.addAtespace(name, w.clock.Now())
	return clone(w.atespaces[name]), nil
}

func (s *controlServer) GetAtespace(_ context.Context, in *ateapi.GetAtespaceRequest) (*ateapi.Atespace, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	a := w.atespaces[in.GetAtespace().GetName()]
	if a == nil {
		return nil, notFound("atespace", in.GetAtespace().GetName())
	}
	return clone(a), nil
}

func (s *controlServer) ListAtespaces(_ context.Context, in *ateapi.ListAtespacesRequest) (*ateapi.ListAtespacesResponse, error) {
	w := s.w
	w.mu.Lock()
	all := clones(w.atespaces, nil)
	w.mu.Unlock()
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListAtespacesResponse{Atespaces: items, NextPageToken: next}, nil
}

func (s *controlServer) DeleteAtespace(_ context.Context, in *ateapi.DeleteAtespaceRequest) (*ateapi.Atespace, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	name := in.GetAtespace().GetName()
	a := w.atespaces[name]
	if a == nil {
		return nil, notFound("atespace", name)
	}
	for k := range w.actors {
		if strings.HasPrefix(k, name+"/") {
			return nil, status.Errorf(codes.FailedPrecondition, "atespace %q still has actors", name)
		}
	}
	for k := range w.tags {
		if strings.HasPrefix(k, name+"/") {
			return nil, status.Errorf(codes.FailedPrecondition, "atespace %q still has tags", name)
		}
	}
	delete(w.atespaces, name)
	return a, nil
}

// ---------- templates ----------

func (s *controlServer) CreateActorTemplate(_ context.Context, in *ateapi.CreateActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	t := clone(in.GetActorTemplate())
	md := t.GetMetadata()
	if md.GetAtespace() == "" || md.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor template needs an atespace and a name")
	}
	if _, ok := w.atespaces[md.Atespace]; !ok {
		return nil, notFound("atespace", md.Atespace)
	}
	if len(t.Containers) == 0 || t.Containers[0].GetImage() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor template needs a container with an image")
	}
	k := key(md.Atespace, md.Name)
	if _, ok := w.templates[k]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor template %q already exists", k)
	}
	now := w.clock.Now()
	md.Uid, md.Version, md.CreateTime, md.UpdateTime = "", 0, nil, nil
	t.Metadata = w.meta(md, now)
	t.Status = &ateapi.ActorTemplateStatus{}
	if t.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() != ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		finish := func() {
			if msg, ok := w.goldenErr[k]; ok {
				t.Status.GoldenSnapshotStatus = &ateapi.GoldenSnapshotStatus{ErrorMessage: msg}
			} else {
				t.Status.GoldenSnapshotStatus = &ateapi.GoldenSnapshotStatus{
					GoldenTag: &ateapi.ObjectRef{Atespace: md.Atespace, Name: md.Name + "-golden"}, SnapshotTime: ts(w.clock.Now()),
				}
			}
			touch(t.Metadata, w.clock.Now())
		}
		if w.GoldenDelay > 0 {
			w.after(w.GoldenDelay, finish)
		} else {
			finish()
		}
	}
	w.templates[k] = t
	return clone(t), nil
}

func (s *controlServer) GetActorTemplate(_ context.Context, in *ateapi.GetActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	t := w.templates[refKey(in.GetActorTemplate())]
	if t == nil {
		return nil, notFound("actor template", refKey(in.GetActorTemplate()))
	}
	return clone(t), nil
}

func (s *controlServer) ListActorTemplates(_ context.Context, in *ateapi.ListActorTemplatesRequest) (*ateapi.ListActorTemplatesResponse, error) {
	w := s.w
	w.mu.Lock()
	w.tick()
	all := clones(w.templates, func(t *ateapi.ActorTemplate) bool { return t.Metadata.Atespace == in.GetAtespace() })
	w.mu.Unlock()
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListActorTemplatesResponse{ActorTemplates: items, NextPageToken: next}, nil
}

// DeleteActorTemplate deletes a template even while actors use it; see
// ASSUMPTIONS.md.
func (s *controlServer) DeleteActorTemplate(_ context.Context, in *ateapi.DeleteActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	k := refKey(in.GetActorTemplate())
	t := w.templates[k]
	if t == nil {
		return nil, notFound("actor template", k)
	}
	delete(w.templates, k)
	return t, nil
}

// ---------- actors ----------

func (s *controlServer) CreateActor(_ context.Context, in *ateapi.CreateActorRequest) (*ateapi.Actor, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a := clone(in.GetActor())
	md := a.GetMetadata()
	if md.GetAtespace() == "" || md.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor needs an atespace and a name")
	}
	k := key(md.Atespace, md.Name)
	if _, ok := w.actors[k]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor %q already exists", k)
	}
	tk := key(md.Atespace, a.GetActorTemplate().GetName())
	t := w.templates[tk]
	if t == nil {
		return nil, notFound("actor template", tk)
	}
	if msg := t.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); msg != "" {
		return nil, status.Errorf(codes.FailedPrecondition, "actor template %q has no golden snapshot: %s", tk, msg)
	}
	var seed *ateapi.Snapshot
	if st := a.GetSourceTag(); st != nil {
		if st.GetAtespace() == "" || st.GetName() == "" {
			return nil, status.Error(codes.InvalidArgument, "source_tag needs an atespace and a name")
		}
		tg := w.tags[refKey(st)]
		if tg == nil {
			return nil, notFound("tag", refKey(st))
		}
		if tg.Scope != ateapi.TagScope_TAG_SCOPE_PUBLISHED && st.GetAtespace() != md.Atespace {
			return nil, status.Errorf(codes.FailedPrecondition, "tag %q is not published to atespace %q", refKey(st), md.Atespace)
		}
		if tg.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
			return nil, status.Errorf(codes.FailedPrecondition, "tag %q is not complete", refKey(st))
		}
		if tg.GetStatus().GetActorTemplateUid() != t.GetMetadata().GetUid() {
			return nil, status.Errorf(codes.FailedPrecondition, "tag %q was taken under another actor template", refKey(st))
		}
		seed = clone(tg.Status.Snapshot)
	}
	now := w.clock.Now()
	md.Uid, md.Version, md.CreateTime, md.UpdateTime = "", 0, nil, nil
	a.Metadata = w.meta(md, now)
	a.ActorTemplate.Atespace = md.Atespace
	a.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
	if seed != nil {
		seed.Generation, seed.Locality = 0, ""
		a.Status.Snapshots = []*ateapi.Snapshot{seed}
	} else if tag := t.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag(); tag != nil {
		a.Status.Snapshots = []*ateapi.Snapshot{{
			Uuid: w.uid(), Generation: 0, Fidelity: t.GetSnapshotConfig().GetPreferredFidelity(), ActorTemplateUid: t.Metadata.Uid,
			DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
		}}
	}
	w.actors[k] = a
	return clone(a), nil
}

func (s *controlServer) GetActor(_ context.Context, in *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a := w.actors[refKey(in.GetActor())]
	if a == nil {
		return nil, notFound("actor", refKey(in.GetActor()))
	}
	return clone(a), nil
}

func (s *controlServer) ListActors(_ context.Context, in *ateapi.ListActorsRequest) (*ateapi.ListActorsResponse, error) {
	w := s.w
	w.mu.Lock()
	w.tick()
	all := clones(w.actors, func(a *ateapi.Actor) bool { return a.Metadata.Atespace == in.GetAtespace() })
	w.mu.Unlock()
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListActorsResponse{Actors: items, NextPageToken: next}, nil
}

func (s *controlServer) actor(in *ateapi.ObjectRef) (*ateapi.Actor, error) {
	a := s.w.actors[refKey(in)]
	if a == nil {
		return nil, notFound("actor", refKey(in))
	}
	return a, nil
}

func stateName(s ateapi.ActorState) string {
	return strings.TrimPrefix(s.String(), "ACTOR_STATE_")
}

// ResumeActor schedules a SUSPENDED actor onto a free worker in a pool that
// matches its selector. With no free worker it fails with
// ResourceExhausted, the error type resume failures are counted under.
func (s *controlServer) ResumeActor(_ context.Context, in *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	w := s.w
	w.mu.Lock()
	w.tick()
	a, err := s.actor(in.GetActor())
	if err != nil {
		w.mu.Unlock()
		return nil, err
	}
	switch a.Status.State {
	case ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_RESUMING:
		out := clone(a)
		w.mu.Unlock()
		return &ateapi.ResumeActorResponse{Actor: out, Resumed: false}, nil
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
	default:
		w.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is %s; only a SUSPENDED actor can resume", refKey(in.GetActor()), stateName(a.Status.State))
	}
	wk := w.schedule(w.selectorFor(a))
	if wk == nil {
		w.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted, "no free worker matches actor %s", refKey(in.GetActor()))
	}
	now := w.clock.Now()
	w.assign(a, wk)
	if w.ResumeDelay > 0 {
		a.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING
		w.after(w.ResumeDelay, func() {
			if a.Status.State == ateapi.ActorState_ACTOR_STATE_RESUMING {
				a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
				touch(a.Metadata, w.clock.Now())
			}
		})
	} else {
		a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	}
	a.Status.LastAssignedGeneration = latestGeneration(a)
	touch(a.Metadata, now)
	out := clone(a)
	hooks := slices.Clone(w.onResume)
	w.mu.Unlock()
	for _, h := range hooks {
		h(clone(in.GetActor()))
	}
	return &ateapi.ResumeActorResponse{Actor: out, Resumed: true}, nil
}

func latestGeneration(a *ateapi.Actor) int32 {
	var g int32
	for _, s := range a.Status.Snapshots {
		g = max(g, s.Generation)
	}
	return g
}

// SuspendActor checkpoints a RUNNING actor to a new snapshot generation and
// frees its worker. A SUSPENDED actor comes back as it is.
func (s *controlServer) SuspendActor(_ context.Context, in *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a, err := s.actor(in.GetActor())
	if err != nil {
		return nil, err
	}
	switch a.Status.State {
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
		return &ateapi.SuspendActorResponse{Actor: clone(a)}, nil
	case ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_RESUMING:
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is %s; only a RUNNING actor can suspend", refKey(in.GetActor()), stateName(a.Status.State))
	}
	fid := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	if t := w.templates[key(a.Metadata.Atespace, a.ActorTemplate.GetName())]; t != nil && t.GetSnapshotConfig().GetPreferredFidelity() != 0 {
		fid = t.GetSnapshotConfig().GetPreferredFidelity()
	}
	snap := &ateapi.Snapshot{
		Uuid: w.uid(), Generation: latestGeneration(a) + 1, Fidelity: fid, Locality: a.Status.AssignedNode,
		DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
		SandboxRuntime:  a.Status.GetWorkerAssignment().GetSandboxRuntime(),
	}
	a.Status.Snapshots = append(a.Status.Snapshots, snap)
	w.release(a)
	a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	touch(a.Metadata, w.clock.Now())
	return &ateapi.SuspendActorResponse{Actor: clone(a)}, nil
}

// RevertActor returns a crashed or running actor, or a suspended one with a
// node-local snapshot, to SUSPENDED at its durable snapshot.
func (s *controlServer) RevertActor(_ context.Context, in *ateapi.RevertActorRequest) (*ateapi.RevertActorResponse, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a, err := s.actor(in.GetActor())
	if err != nil {
		return nil, err
	}
	hasLocal := false
	for _, sn := range a.Status.Snapshots {
		hasLocal = hasLocal || sn.Locality != ""
	}
	switch a.Status.State {
	case ateapi.ActorState_ACTOR_STATE_CRASHED, ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_RESUMING:
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
		if !hasLocal {
			return nil, status.Errorf(codes.FailedPrecondition, "actor %s is already at its durable snapshot", refKey(in.GetActor()))
		}
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is %s and can't be reverted", refKey(in.GetActor()), stateName(a.Status.State))
	}
	for _, sn := range a.Status.Snapshots {
		sn.Locality = ""
	}
	w.release(a)
	a.Status.Crash = nil
	a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	touch(a.Metadata, w.clock.Now())
	return &ateapi.RevertActorResponse{Actor: clone(a)}, nil
}

// DeleteActor removes an actor, releasing its worker and egress policy. A
// second delete is NotFound.
func (s *controlServer) DeleteActor(_ context.Context, in *ateapi.DeleteActorRequest) (*ateapi.Actor, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	a, err := s.actor(in.GetActor())
	if err != nil {
		return nil, err
	}
	if v := in.GetOptions().GetVersion(); v != 0 && v != a.Metadata.Version {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is at version %d, not %d", refKey(in.GetActor()), a.Metadata.Version, v)
	}
	if u := in.GetOptions().GetUid(); u != "" && u != a.Metadata.Uid {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s has uid %s, not %s", refKey(in.GetActor()), a.Metadata.Uid, u)
	}
	w.release(a)
	k := refKey(in.GetActor())
	delete(w.actors, k)
	delete(w.policies, k)
	a.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
	a.Metadata.DeleteTime = ts(w.clock.Now())
	return a, nil
}

// ---------- tags ----------

// CreateTag copies the durable snapshot a SUSPENDED actor holds into a new
// tag, COMPLETED at once (the real copy takes as long as an upload).
func (s *controlServer) CreateTag(_ context.Context, in *ateapi.CreateTagRequest) (*ateapi.Tag, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tick()
	tg := clone(in.GetTag())
	md := tg.GetMetadata()
	src := tg.GetSourceActor()
	if md.GetAtespace() == "" || md.GetName() == "" || src.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "tag needs an atespace, a name and a source_actor")
	}
	if src.GetAtespace() != md.GetAtespace() {
		return nil, status.Error(codes.InvalidArgument, "tag atespace must match source_actor's")
	}
	switch tg.GetScope() {
	case ateapi.TagScope_TAG_SCOPE_ATESPACE, ateapi.TagScope_TAG_SCOPE_PUBLISHED:
	default:
		return nil, status.Error(codes.InvalidArgument, "tag scope is required")
	}
	k := key(md.Atespace, md.Name)
	if _, ok := w.tags[k]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "tag %q already exists", k)
	}
	a, err := s.actor(src)
	if err != nil {
		return nil, err
	}
	if a.Status.State != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is %s; only a SUSPENDED actor can be tagged", refKey(src), stateName(a.Status.State))
	}
	var latest *ateapi.Snapshot
	for _, sn := range a.Status.Snapshots {
		if latest == nil || sn.Generation > latest.Generation {
			latest = sn
		}
	}
	if latest == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s holds no snapshot", refKey(src))
	}
	t := w.templates[key(a.Metadata.Atespace, a.ActorTemplate.GetName())]
	if t == nil {
		return nil, notFound("actor template", key(a.Metadata.Atespace, a.ActorTemplate.GetName()))
	}
	snap := clone(latest)
	snap.Uuid, snap.Owner, snap.Locality = w.uid(), ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ""
	snap.DurableSnapshot = &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}
	md.Uid, md.Version, md.CreateTime, md.UpdateTime = "", 0, nil, nil
	tg.Metadata = w.meta(md, w.clock.Now())
	tg.Status = &ateapi.TagStatus{Snapshot: snap, ActorTemplateUid: t.Metadata.Uid}
	w.tags[k] = tg
	return clone(tg), nil
}

func (s *controlServer) GetTag(_ context.Context, in *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	tg := w.tags[refKey(in.GetTag())]
	if tg == nil {
		return nil, notFound("tag", refKey(in.GetTag()))
	}
	return clone(tg), nil
}

func (s *controlServer) ListTags(_ context.Context, in *ateapi.ListTagsRequest) (*ateapi.ListTagsResponse, error) {
	w := s.w
	w.mu.Lock()
	all := clones(w.tags, func(t *ateapi.Tag) bool { return in.GetAtespace() == "" || t.Metadata.Atespace == in.GetAtespace() })
	w.mu.Unlock()
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListTagsResponse{Tags: items, NextPageToken: next}, nil
}

// UpdateTag changes only the scope (publish or unpublish).
func (s *controlServer) UpdateTag(_ context.Context, in *ateapi.UpdateTagRequest) (*ateapi.Tag, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	md := in.GetTag().GetMetadata()
	tg := w.tags[key(md.GetAtespace(), md.GetName())]
	if tg == nil {
		return nil, notFound("tag", key(md.GetAtespace(), md.GetName()))
	}
	if md.GetVersion() != tg.Metadata.Version || md.GetUid() != tg.Metadata.Uid {
		return nil, status.Errorf(codes.FailedPrecondition, "tag %s: version and uid preconditions do not match", key(md.GetAtespace(), md.GetName()))
	}
	switch sc := in.GetTag().GetScope(); sc {
	case ateapi.TagScope_TAG_SCOPE_ATESPACE, ateapi.TagScope_TAG_SCOPE_PUBLISHED:
		tg.Scope = sc
	default:
		return nil, status.Error(codes.InvalidArgument, "tag scope is required")
	}
	touch(tg.Metadata, w.clock.Now())
	return clone(tg), nil
}

// DeleteTag removes a tag even while actors created from it have not been
// suspended yet, as Substrate does (#1554). Such an actor keeps running in
// the fake; on a real cluster it could not be restored again.
func (s *controlServer) DeleteTag(_ context.Context, in *ateapi.DeleteTagRequest) (*ateapi.Tag, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	k := refKey(in.GetTag())
	tg := w.tags[k]
	if tg == nil {
		return nil, notFound("tag", k)
	}
	delete(w.tags, k)
	tg.Metadata.DeleteTime = ts(w.clock.Now())
	return tg, nil
}

// Tag returns a copy of a tag, or nil.
func (w *World) Tag(atespace, name string) *ateapi.Tag {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tg := w.tags[key(atespace, name)]; tg != nil {
		return clone(tg)
	}
	return nil
}

// ---------- egress policies ----------

func (s *controlServer) GetActorEgressPolicy(_ context.Context, in *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	p := w.policies[refKey(in.GetActor())]
	if p == nil {
		return nil, notFound("egress policy for actor", refKey(in.GetActor()))
	}
	return clone(p), nil
}

func (s *controlServer) CreateActorEgressPolicy(_ context.Context, in *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := s.actor(in.GetActor()); err != nil {
		return nil, err
	}
	k := refKey(in.GetActor())
	if _, ok := w.policies[k]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor %s already has an egress policy", k)
	}
	p := clone(in.GetEgressPolicy())
	p.Metadata = w.meta(&ateapi.ResourceMetadata{Atespace: in.GetActor().GetAtespace(), Name: in.GetActor().GetName()}, w.clock.Now())
	w.policies[k] = p
	return clone(p), nil
}

func (s *controlServer) UpdateActorEgressPolicy(_ context.Context, in *ateapi.UpdateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	k := refKey(in.GetActor())
	old := w.policies[k]
	if old == nil {
		return nil, notFound("egress policy for actor", k)
	}
	p := clone(in.GetEgressPolicy())
	p.Metadata = old.Metadata
	touch(p.Metadata, w.clock.Now())
	w.policies[k] = p
	return clone(p), nil
}

func (s *controlServer) DeleteActorEgressPolicy(_ context.Context, in *ateapi.DeleteActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	k := refKey(in.GetActor())
	p := w.policies[k]
	if p == nil {
		return nil, notFound("egress policy for actor", k)
	}
	delete(w.policies, k)
	return p, nil
}

// ---------- workers ----------

func (s *controlServer) ListWorkers(_ context.Context, in *ateapi.ListWorkersRequest) (*ateapi.ListWorkersResponse, error) {
	w := s.w
	w.mu.Lock()
	all := clones(w.workers, nil)
	w.mu.Unlock()
	slices.SortStableFunc(all, func(a, b *ateapi.Worker) int {
		if c := strings.Compare(a.WorkerPool, b.WorkerPool); c != 0 {
			return c
		}
		return workerIndex(a.Metadata.Name) - workerIndex(b.Metadata.Name)
	})
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListWorkersResponse{Workers: items, NextPageToken: next}, nil
}

func (s *controlServer) GetWorker(_ context.Context, in *ateapi.GetWorkerRequest) (*ateapi.Worker, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	wk := w.workers[in.GetWorker().GetName()]
	if wk == nil {
		return nil, notFound("worker", in.GetWorker().GetName())
	}
	return clone(wk), nil
}

// ListWorkerActorAssignments lists the actors bound to a worker. Each
// assignment is created when a resume binds the actor and removed when it is
// released, so its create_time is when the actor arrived on the worker.
func (s *controlServer) ListWorkerActorAssignments(_ context.Context, in *ateapi.ListWorkerActorAssignmentsRequest) (*ateapi.ListWorkerActorAssignmentsResponse, error) {
	w := s.w
	w.mu.Lock()
	w.tick()
	name := in.GetWorker().GetName()
	if w.workers[name] == nil {
		w.mu.Unlock()
		return nil, notFound("worker", name)
	}
	var all []*ateapi.ActorAssignment
	for _, a := range w.actors {
		if a.Status.GetWorkerAssignment().GetWorker().GetName() != name {
			continue
		}
		at := ts(w.bound[a.Metadata.Uid])
		all = append(all, &ateapi.ActorAssignment{
			Metadata: &ateapi.ResourceMetadata{Name: a.Metadata.Uid, Version: 1, CreateTime: at, UpdateTime: at},
			Actor:    &ateapi.ObjectRef{Atespace: a.Metadata.Atespace, Name: a.Metadata.Name},
			ActorUid: a.Metadata.Uid,
		})
	}
	w.mu.Unlock()
	slices.SortFunc(all, func(a, b *ateapi.ActorAssignment) int { return strings.Compare(a.ActorUid, b.ActorUid) })
	items, next, err := page(all, in.GetOptions())
	if err != nil {
		return nil, err
	}
	return &ateapi.ListWorkerActorAssignmentsResponse{ActorAssignments: items, NextPageToken: next}, nil
}

// DrainWorker marks a worker DRAINING so nothing new lands on it. Actors on
// it are left alone, as the API documents.
func (s *controlServer) DrainWorker(_ context.Context, in *ateapi.DrainWorkerRequest) (*ateapi.DrainWorkerResponse, error) {
	w := s.w
	w.mu.Lock()
	defer w.mu.Unlock()
	wk := w.workers[in.GetWorker().GetName()]
	if wk == nil {
		return nil, notFound("worker", in.GetWorker().GetName())
	}
	if wk.Status.State != ateapi.WorkerState_WORKER_STATE_DRAINING {
		wk.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
		touch(wk.Metadata, w.clock.Now())
	}
	return &ateapi.DrainWorkerResponse{Worker: clone(wk)}, nil
}
