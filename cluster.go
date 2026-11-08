package substratefake

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Options configures Start.
type Options struct {
	// Scenario fills the world. Default: Basic.
	Scenario func(*World)
	// Clock drives time. Default: the wall clock.
	Clock Clock
	// Names picks the Prometheus metric spelling. Default: DottedNames.
	Names MetricNames
	// Dir is where Start writes the kubeconfig. Default: a temporary
	// directory that Close removes.
	Dir string
	// Addr is the host to listen on. Default: 127.0.0.1.
	Addr string
	// Token is the kubeconfig's bearer token for the Kubernetes API, which
	// ateapi also accepts as ClientPrincipal. Default: a signed
	// ServiceAccount token for ate-client, valid for a year. Use
	// World.IssueToken for other principals.
	Token string
	// NoServers skips the network listeners; only Client works. Useful in
	// unit tests that need nothing but ateapi.
	NoServers bool
}

// Cluster is a running fake.
type Cluster struct {
	World  *World
	Router *Router

	// APIEndpoint is ateapi's host:port. It serves TLS for APIServerName,
	// signed by CAPEM, and wants Token as a bearer token.
	APIEndpoint string
	// KubeURL is the fake Kubernetes API, and Kubeconfig a file pointing at
	// it with context Context.
	KubeURL, Kubeconfig, Context string
	// PrometheusURL is the fake Prometheus HTTP API.
	PrometheusURL string
	Token         string
	CAPEM         []byte

	grpcServer *grpc.Server
	bufServer  *grpc.Server
	buf        *bufconn.Listener
	https      []*http.Server
	tmpDir     string
	closeOnce  sync.Once
}

// Start builds a world from the scenario and serves it.
func Start(ctx context.Context, opts Options) (*Cluster, error) {
	w := NewWorld(opts.Clock)
	if opts.Scenario == nil {
		opts.Scenario = Basic
	}
	opts.Scenario(w)
	c := &Cluster{World: w, Router: &Router{w: w, routes: map[string]string{}}, Token: opts.Token}

	// In-process ateapi, no TLS or auth.
	c.buf = bufconn.Listen(1 << 20)
	c.bufServer = grpc.NewServer(grpc.UnaryInterceptor(w.interceptor(false, "")))
	ateapi.RegisterControlServer(c.bufServer, &controlServer{w: w})
	go func() { _ = c.bufServer.Serve(c.buf) }()
	if opts.NoServers {
		if c.Token == "" {
			c.Token = c.clientToken()
		}
		return c, nil
	}

	if err := c.serve(ctx, opts); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cluster) serve(_ context.Context, opts Options) error {
	host := opts.Addr
	if host == "" {
		host = "127.0.0.1"
	}
	ca, err := newAuthority()
	if err != nil {
		return err
	}
	c.CAPEM = ca.PEM

	// ateapi over TLS, as a cluster's api Service serves it.
	apiCert, err := ca.serve(APIServerName)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return err
	}
	c.APIEndpoint = lis.Addr().String()

	// The Kubernetes API is the token issuer, so its URL comes first.
	klis, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return err
	}
	c.KubeURL = "https://" + klis.Addr().String()
	c.World.setIssuer(c.KubeURL)
	raw := c.Token
	if c.Token == "" {
		c.Token = c.clientToken()
	}

	c.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{apiCert}, MinVersion: tls.VersionTLS13})),
		grpc.UnaryInterceptor(c.World.interceptor(true, raw)),
	)
	ateapi.RegisterControlServer(c.grpcServer, &controlServer{w: c.World})
	go func() { _ = c.grpcServer.Serve(lis) }()

	// Kubernetes API over TLS.
	kubeCert, err := ca.serve("kubernetes.default.svc")
	if err != nil {
		return err
	}
	kube := &kubeAPI{w: c.World, token: c.Token, caPEM: ca.PEM, started: c.World.Now(), tokenTTL: time.Hour}
	ks := &http.Server{Handler: kube.handler(), ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{kubeCert}, MinVersion: tls.VersionTLS12}}
	go func() { _ = ks.ServeTLS(klis, "", "") }()

	// Prometheus over plain HTTP, as kind's usually is.
	plis, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return err
	}
	c.PrometheusURL = "http://" + plis.Addr().String()
	ps := &http.Server{Handler: newPromAPI(c.World, opts.Names).handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = ps.Serve(plis) }()
	c.https = []*http.Server{ks, ps}

	dir := opts.Dir
	if dir == "" {
		if dir, err = os.MkdirTemp("", "substrate-fake-"); err != nil {
			return err
		}
		c.tmpDir = dir
	}
	c.Context = c.World.Name
	c.Kubeconfig = filepath.Join(dir, "kubeconfig")
	return os.WriteFile(c.Kubeconfig, []byte(c.kubeconfig()), 0o600)
}

func (c *Cluster) kubeconfig() string {
	return fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: %[1]s
  cluster:
    server: %[2]s
    certificate-authority-data: %[3]s
users:
- name: %[1]s
  user:
    token: %[4]s
contexts:
- name: %[1]s
  context:
    cluster: %[1]s
    user: %[1]s
current-context: %[1]s
`, c.Context, c.KubeURL, base64.StdEncoding.EncodeToString(c.CAPEM), c.Token)
}

// clientToken is the kubeconfig's token: ate-client's, for a year.
func (c *Cluster) clientToken() string {
	return c.World.IssueToken(ClientPrincipal, TokenOptions{TTL: 365 * 24 * time.Hour})
}

// Client returns an ateapi client connected in process. It skips TLS and
// auth, and shares the world, call log and faults with the network server.
func (c *Cluster) Client() ateapi.ControlClient {
	conn, err := grpc.NewClient("passthrough:///substrate-fake",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return c.buf.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err) // NewClient only fails on bad options
	}
	return ateapi.NewControlClient(conn)
}

// Close stops every server and removes the temporary kubeconfig.
func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		if c.grpcServer != nil {
			c.grpcServer.Stop()
		}
		c.bufServer.Stop()
		for _, s := range c.https {
			_ = s.Close()
		}
		if c.tmpDir != "" {
			_ = os.RemoveAll(c.tmpDir)
		}
	})
}

// Router stands in for atenet-router: it connects to a port on a running
// actor. Tests say where each actor's port really is with Expose.
type Router struct {
	w      *World
	mu     sync.Mutex
	routes map[string]string
}

func routeKey(atespace, actor string, port int) string {
	return fmt.Sprintf("%s/%s:%d", atespace, actor, port)
}

// Expose sends connections for an actor's port to addr.
func (r *Router) Expose(atespace, actor string, port int, addr string) {
	r.mu.Lock()
	r.routes[routeKey(atespace, actor, port)] = addr
	r.mu.Unlock()
}

// ErrNotRunning is returned by Dial when the actor isn't RUNNING.
var ErrNotRunning = errors.New("actor is not running")

// Dial connects to an actor's port. Like the router, it refuses actors that
// aren't RUNNING; resuming them is the caller's job.
func (r *Router) Dial(ctx context.Context, atespace, actor string, port int) (net.Conn, error) {
	a := r.w.Actor(atespace, actor)
	if a == nil {
		return nil, status.Errorf(codes.NotFound, "actor %s/%s not found", atespace, actor)
	}
	if a.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		return nil, fmt.Errorf("%s/%s is %s: %w", atespace, actor, stateName(a.GetStatus().GetState()), ErrNotRunning)
	}
	r.mu.Lock()
	addr, ok := r.routes[routeKey(atespace, actor, port)]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("nothing exposed for %s; call Router.Expose first", routeKey(atespace, actor, port))
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
