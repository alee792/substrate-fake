package substratefake

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ClientPrincipal is the shared ate-client ServiceAccount that kubectl-ate
// mints tokens for. ate-setup makes it a bootstrap owner by default, and so
// does the fake: Cluster.Token authenticates as it.
const ClientPrincipal = "system:serviceaccount:" + SystemNamespace + ":ate-client"

// DefaultIssuer is the token issuer when the fake serves no Kubernetes API.
// With one, the issuer is KubeURL, which serves OIDC discovery as a
// Kubernetes API server does.
const DefaultIssuer = "https://kubernetes.default.svc.cluster.local"

// Role is a role an access policy grants. Global policies grant owner and
// viewer; atespace policies also grant editor.
type Role int

const (
	RoleNone Role = iota
	RoleViewer
	RoleEditor
	RoleOwner
)

func (r Role) String() string {
	return [...]string{"none", "viewer", "editor", "owner"}[r]
}

// access is ateapi's authentication and authorization, as
// docs/guides/authentication.md and authorization.md describe them for
// Substrate v1.0.0. Tokens are JWTs the fake signs itself; ateapi accepts
// those whose issuer, audience, signature and expiry check out.
type access struct {
	mu        sync.Mutex
	key       *ecdsa.PrivateKey
	signer    jose.Signer
	issuer    string
	audiences []string
	owners    []string                   // bootstrap owners
	grants    map[string]map[string]Role // scope ("" is global) → principal → role
}

func newAccess() *access {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "substrate-fake"))
	if err != nil {
		panic(err)
	}
	return &access{key: key, signer: signer, issuer: DefaultIssuer, audiences: []string{APIServerName},
		owners: []string{ClientPrincipal}, grants: map[string]map[string]Role{}}
}

// TokenOptions shape a token from IssueToken.
type TokenOptions struct {
	// TTL defaults to one hour, measured on the world's clock.
	TTL time.Duration
	// Audience defaults to APIServerName, the audience kubectl-ate asks for.
	Audience string
}

// IssueToken signs a token whose sub is principal, as a Kubernetes
// ServiceAccount token or an identity provider's ID token would carry it.
// ateapi accepts it if its audience is one TrustAudience added.
func (w *World) IssueToken(principal string, opts TokenOptions) string {
	if opts.TTL == 0 {
		opts.TTL = time.Hour
	}
	if opts.Audience == "" {
		opts.Audience = APIServerName
	}
	a := w.access
	a.mu.Lock()
	iss := a.issuer
	a.mu.Unlock()
	now := w.Now()
	tok, err := jwt.Signed(a.signer).Claims(jwt.Claims{
		Issuer: iss, Subject: principal, Audience: jwt.Audience{opts.Audience},
		IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), Expiry: jwt.NewNumericDate(now.Add(opts.TTL)),
	}).Serialize()
	if err != nil {
		panic(err)
	}
	return tok
}

// TrustAudience makes ateapi accept tokens for aud as well as
// APIServerName, as adding it to a JWT provider's audiences does.
func (w *World) TrustAudience(aud string) {
	w.access.mu.Lock()
	defer w.access.mu.Unlock()
	if !slices.Contains(w.access.audiences, aud) {
		w.access.audiences = append(w.access.audiences, aud)
	}
}

// Issuer is the iss of the tokens the fake signs.
func (w *World) Issuer() string {
	w.access.mu.Lock()
	defer w.access.mu.Unlock()
	return w.access.issuer
}

// JWKS is the public key set that verifies the fake's tokens.
func (w *World) JWKS() jose.JSONWebKeySet {
	return jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &w.access.key.PublicKey, KeyID: "substrate-fake", Algorithm: string(jose.ES256), Use: "sig"}}}
}

// SetBootstrapOwners replaces the principals that hold global owner
// whatever the policies say. The default is ClientPrincipal.
func (w *World) SetBootstrapOwners(principals ...string) {
	w.access.mu.Lock()
	w.access.owners = slices.Clone(principals)
	w.access.mu.Unlock()
}

// Grant binds principal to role in an access policy: the global one when
// atespace is empty, else that atespace's. RoleNone removes the binding.
func (w *World) Grant(atespace, principal string, role Role) {
	a := w.access
	a.mu.Lock()
	defer a.mu.Unlock()
	if atespace == "" && role == RoleEditor {
		panic("substrate-fake: global policies grant owner or viewer, not editor")
	}
	if a.grants[atespace] == nil {
		a.grants[atespace] = map[string]Role{}
	}
	if role == RoleNone {
		delete(a.grants[atespace], principal)
		return
	}
	a.grants[atespace][principal] = role
}

// role is what principal holds on the scope: its own binding there, or the
// global one, which applies in every atespace.
func (a *access) role(principal, atespace string) Role {
	a.mu.Lock()
	defer a.mu.Unlock()
	if slices.Contains(a.owners, principal) {
		return RoleOwner
	}
	r := a.grants[""][principal]
	if atespace != "" {
		r = max(r, a.grants[atespace][principal])
	}
	return r
}

// authenticate turns the request's bearer token into a principal. raw is
// a token that isn't a JWT, which the fake takes as ClientPrincipal (the
// token Options.Token set).
func (a *access) authenticate(ctx context.Context, now time.Time, raw string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")
	if len(v) == 0 {
		return "", status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
	}
	tok, ok := strings.CutPrefix(v[0], "Bearer ")
	if !ok || tok == "" {
		return "", status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
	}
	if raw != "" && tok == raw {
		return ClientPrincipal, nil
	}
	parsed, err := jwt.ParseSigned(tok, []jose.SignatureAlgorithm{jose.ES256})
	if err != nil {
		return "", status.Error(codes.Unauthenticated, "unauthenticated: invalid bearer token")
	}
	var c jwt.Claims
	if err := parsed.Claims(&a.key.PublicKey, &c); err != nil {
		return "", status.Error(codes.Unauthenticated, "unauthenticated: invalid bearer token")
	}
	a.mu.Lock()
	exp := jwt.Expected{Issuer: a.issuer, AnyAudience: jwt.Audience(a.audiences), Time: now}
	a.mu.Unlock()
	// ateapi allows five minutes of clock skew.
	if err := c.ValidateWithLeeway(exp, 5*time.Minute); err != nil || c.Subject == "" {
		return "", status.Error(codes.Unauthenticated, "unauthenticated: invalid bearer token")
	}
	return c.Subject, nil
}

// check is one permission a call needs: role on a scope ("" is global).
type check struct {
	relation string
	atespace string
	global   bool
	need     Role
}

// checks lists what a request needs, after registry.go in Substrate v1.0.0.
// A method with no rule is denied, as ateapi denies it.
func checks(req any) ([]check, bool) {
	on := func(rel, ns string, need Role) check { return check{relation: rel, atespace: ns, need: need} }
	global := func(rel string, need Role) check { return check{relation: rel, global: true, need: need} }
	list := func(rel, ns string) check {
		if ns == "" {
			return global(rel, RoleViewer)
		}
		return on(rel, ns, RoleViewer)
	}
	switch r := req.(type) {
	case *ateapi.CreateAtespaceRequest:
		return []check{global("can_create_atespace", RoleOwner)}, true
	case *ateapi.ListAtespacesRequest:
		return []check{global("can_list_atespaces", RoleViewer)}, true
	case *ateapi.GetAtespaceRequest:
		return []check{on("can_get", r.GetAtespace().GetName(), RoleViewer)}, true
	case *ateapi.DeleteAtespaceRequest:
		return []check{on("can_delete", r.GetAtespace().GetName(), RoleOwner)}, true
	case *ateapi.CreateActorTemplateRequest:
		return []check{on("can_create_actor_template", r.GetActorTemplate().GetMetadata().GetAtespace(), RoleEditor)}, true
	case *ateapi.GetActorTemplateRequest:
		return []check{on("can_get", r.GetActorTemplate().GetAtespace(), RoleViewer)}, true
	case *ateapi.DeleteActorTemplateRequest:
		return []check{on("can_delete", r.GetActorTemplate().GetAtespace(), RoleEditor)}, true
	case *ateapi.ListActorTemplatesRequest:
		return []check{list("can_list_actor_templates", r.GetAtespace())}, true
	case *ateapi.CreateActorRequest:
		return []check{
			on("can_create_actor", r.GetActor().GetMetadata().GetAtespace(), RoleEditor),
			on("can_use_template", r.GetActor().GetActorTemplate().GetAtespace(), RoleViewer),
		}, true
	case *ateapi.GetActorRequest:
		return []check{on("can_get", r.GetActor().GetAtespace(), RoleViewer)}, true
	case *ateapi.DeleteActorRequest:
		return []check{on("can_delete", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.ListActorsRequest:
		return []check{list("can_list_actors", r.GetAtespace())}, true
	case *ateapi.SuspendActorRequest:
		return []check{on("can_suspend", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.ResumeActorRequest:
		return []check{on("can_resume", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.RevertActorRequest:
		return []check{on("can_revert", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.GetActorEgressPolicyRequest:
		return []check{on("can_get_egress_policy", r.GetActor().GetAtespace(), RoleViewer)}, true
	case *ateapi.CreateActorEgressPolicyRequest:
		return []check{on("can_create_egress_policy", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.UpdateActorEgressPolicyRequest:
		return []check{on("can_update_egress_policy", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.DeleteActorEgressPolicyRequest:
		return []check{on("can_delete_egress_policy", r.GetActor().GetAtespace(), RoleEditor)}, true
	case *ateapi.ListWorkersRequest:
		return []check{global("can_list_workers", RoleViewer)}, true
	case *ateapi.GetWorkerRequest:
		return []check{global("can_get", RoleViewer)}, true
	case *ateapi.ListWorkerActorAssignmentsRequest:
		return []check{global("can_list_actor_assignments", RoleViewer)}, true
	case *ateapi.DrainWorkerRequest:
		return []check{global("can_drain", RoleOwner)}, true
	}
	return nil, false
}

// authorize returns PermissionDenied, worded as ateapi words it, unless
// principal passes every check the request needs.
func (a *access) authorize(principal, fullMethod string, req any) error {
	cs, ok := checks(req)
	if !ok {
		return status.Errorf(codes.PermissionDenied, "permission denied: no authorization rule for %s", fullMethod)
	}
	// Like ateapi, a missing resource name is InvalidArgument before any
	// check runs.
	for _, c := range cs {
		if !c.global && c.atespace == "" {
			return status.Error(codes.InvalidArgument, "atespace is required")
		}
	}
	for _, c := range cs {
		scope := c.atespace
		if c.global {
			scope = ""
		}
		if a.role(principal, scope) < c.need {
			obj := "global:root"
			if !c.global {
				obj = "atespace:" + c.atespace
			}
			return status.Errorf(codes.PermissionDenied, "permission denied: principal %q lacks %q on %q",
				"user:"+strings.ReplaceAll(principal, ":", "%3A"), c.relation, obj)
		}
	}
	return nil
}

// discovery is the OIDC discovery document the Kubernetes API serves at
// /.well-known/openid-configuration.
func (w *World) discovery() []byte {
	iss := w.Issuer()
	b, _ := json.Marshal(map[string]any{
		"issuer": iss, "jwks_uri": iss + "/openid/v1/jwks",
		"response_types_supported": []string{"id_token"}, "subject_types_supported": []string{"public"},
		"id_token_signing_alg_values_supported": []string{"ES256"},
	})
	return b
}

func (w *World) setIssuer(iss string) {
	w.access.mu.Lock()
	w.access.issuer = iss
	w.access.mu.Unlock()
}
