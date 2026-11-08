package substratefake

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func dialAs(t *testing.T, c *Cluster, token string) ateapi.ControlClient {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(c.CAPEM)
	conn, err := grpc.NewClient(c.APIEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: APIServerName})),
		grpc.WithPerRPCCredentials(bearer(token)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return ateapi.NewControlClient(conn)
}

func TestAccess(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC))
	c := start(t, Options{Scenario: Demo, Clock: clock})
	const (
		viewer = "system:serviceaccount:ate-users:val"
		editor = "system:serviceaccount:ate-users:eve"
		nobody = "system:serviceaccount:ate-users:nobody"
	)
	c.World.Grant("", viewer, RoleViewer)
	c.World.Grant("triage", editor, RoleEditor)

	triageActor := &ateapi.ObjectRef{Atespace: "triage", Name: "triage-7f3a"}
	type call struct {
		name string
		do   func(ateapi.ControlClient) error
	}
	calls := []call{
		{"ListAtespaces", func(api ateapi.ControlClient) error {
			_, err := api.ListAtespaces(ctx, &ateapi.ListAtespacesRequest{})
			return err
		}},
		{"ListWorkers", func(api ateapi.ControlClient) error {
			_, err := api.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
			return err
		}},
		{"ListActors triage", func(api ateapi.ControlClient) error {
			_, err := api.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: "triage"})
			return err
		}},
		{"ListActors ioc-enrich", func(api ateapi.ControlClient) error {
			_, err := api.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: "ioc-enrich"})
			return err
		}},
		{"GetAtespace triage", func(api ateapi.ControlClient) error {
			_, err := api.GetAtespace(ctx, &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "triage"}})
			return err
		}},
		{"SuspendActor triage", func(api ateapi.ControlClient) error {
			_, err := api.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: triageActor})
			return err
		}},
		{"ListWorkerActorAssignments", func(api ateapi.ControlClient) error {
			_, err := api.ListWorkerActorAssignments(ctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: "gvisor-pool-a-1"}})
			return err
		}},
		{"DrainWorker", func(api ateapi.ControlClient) error {
			_, err := api.DrainWorker(ctx, &ateapi.DrainWorkerRequest{Worker: &ateapi.ObjectRef{Name: "nope"}})
			return err
		}},
	}
	// Owner passes every check; DrainWorker then fails on the missing worker.
	want := map[string][]codes.Code{
		ClientPrincipal: {codes.OK, codes.OK, codes.OK, codes.OK, codes.OK, codes.OK, codes.OK, codes.NotFound},
		viewer:          {codes.OK, codes.OK, codes.OK, codes.OK, codes.OK, codes.PermissionDenied, codes.OK, codes.PermissionDenied},
		editor:          {codes.PermissionDenied, codes.PermissionDenied, codes.OK, codes.PermissionDenied, codes.OK, codes.OK, codes.PermissionDenied, codes.PermissionDenied},
		nobody:          {codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied, codes.PermissionDenied},
	}
	for principal, codesWant := range want {
		api := dialAs(t, c, c.World.IssueToken(principal, TokenOptions{}))
		for i, cl := range calls {
			if got := code(cl.do(api)); got != codesWant[i] {
				t.Errorf("%s: %s = %v, want %v", principal, cl.name, got, codesWant[i])
			}
		}
	}

	// Denials name the principal and permission, as ateapi's do.
	_, err := dialAs(t, c, c.World.IssueToken(editor, TokenOptions{})).ListWorkers(ctx, &ateapi.ListWorkersRequest{})
	if !strings.Contains(err.Error(), `lacks "can_list_workers" on "global:root"`) || !strings.Contains(err.Error(), "ate-users%3Aeve") {
		t.Errorf("denial: %v", err)
	}
	if calls := c.World.Calls(); calls[len(calls)-1].Principal != editor {
		t.Errorf("call log principal %q", calls[len(calls)-1].Principal)
	}

	// Expired tokens, other audiences and foreign signatures are rejected.
	short := c.World.IssueToken(viewer, TokenOptions{TTL: time.Minute})
	clock.Advance(10 * time.Minute)
	if err := calls[0].do(dialAs(t, c, short)); code(err) != codes.Unauthenticated {
		t.Errorf("expired token: %v", err)
	}
	other := c.World.IssueToken(viewer, TokenOptions{Audience: "https://console.example.com/mcp"})
	if err := calls[0].do(dialAs(t, c, other)); code(err) != codes.Unauthenticated {
		t.Errorf("untrusted audience: %v", err)
	}
	c.World.TrustAudience("https://console.example.com/mcp")
	if err := calls[0].do(dialAs(t, c, other)); err != nil {
		t.Errorf("trusted audience: %v", err)
	}
	if err := calls[0].do(dialAs(t, c, "not-a-jwt")); code(err) != codes.Unauthenticated {
		t.Errorf("garbage token: %v", err)
	}
}

// The Kubernetes API publishes the issuer's keys, so a verifier can check
// the fake's tokens the way it checks a real cluster's.
func TestTokenDiscovery(t *testing.T) {
	c := start(t, Options{Scenario: Basic})
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.CAPEM)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	get := func(path string, v any) {
		resp, err := hc.Get(c.KubeURL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %s", path, resp.Status)
		}
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatal(err)
		}
	}
	var disc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	get("/.well-known/openid-configuration", &disc)
	if disc.Issuer != c.KubeURL {
		t.Fatalf("issuer %q, want %q", disc.Issuer, c.KubeURL)
	}
	var keys jose.JSONWebKeySet
	get(strings.TrimPrefix(disc.JWKSURI, c.KubeURL), &keys)

	// A TokenRequest mints a token for that ServiceAccount.
	req, _ := http.NewRequest("POST", c.KubeURL+"/api/v1/namespaces/ate-users/serviceaccounts/alice/token",
		strings.NewReader(`{"spec":{"audiences":["api.ate-system.svc"],"expirationSeconds":600}}`))
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var tr struct{ Status struct{ Token string } }
	_ = json.NewDecoder(resp.Body).Decode(&tr)
	parsed, err := jwt.ParseSigned(tr.Status.Token, []jose.SignatureAlgorithm{jose.ES256})
	if err != nil {
		t.Fatal(err)
	}
	var claims jwt.Claims
	if err := parsed.Claims(keys.Keys[0].Key, &claims); err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "system:serviceaccount:ate-users:alice" || claims.Issuer != c.KubeURL {
		t.Fatalf("claims %+v", claims)
	}
}

// Every Control method the fake serves has an access rule, as ateapi
// requires of every RPC it serves.
func TestEveryServedMethodHasARule(t *testing.T) {
	c := start(t, Options{NoServers: true})
	conn, err := grpc.NewClient("passthrough:///substrate-fake",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return c.buf.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	served := 0
	svc := ateapi.File_api_ateapi_v1_ateapi_proto.Services().ByName("Control")
	for i := 0; i < svc.Methods().Len(); i++ {
		m := svc.Methods().Get(i)
		if m.IsStreamingClient() || m.IsStreamingServer() {
			continue
		}
		req := dynamicpb.NewMessage(m.Input())
		out := dynamicpb.NewMessage(m.Output())
		err := conn.Invoke(context.Background(), "/"+string(svc.FullName())+"/"+string(m.Name()), req, out)
		if code(err) == codes.Unimplemented {
			continue
		}
		served++
		typed, err2 := protoregistry.GlobalTypes.FindMessageByName(m.Input().FullName())
		if err2 != nil {
			t.Fatal(err2)
		}
		if _, ok := checks(typed.New().Interface()); !ok {
			t.Errorf("%s is served but has no access rule", m.Name())
		}
	}
	if served < 20 {
		t.Fatalf("only %d methods looked served; the probe is broken", served)
	}
}
