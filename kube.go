package substratefake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Labels and names Substrate clients look for, as kubectl-ate does.
const (
	SystemNamespace      = "ate-system"
	APIServiceName       = "api"
	ClientServiceAccount = "ate-client"
	ServiceDNSSigner     = "servicedns.podcert.ate.dev/identity"
	liveBundleLabel      = "podcert.ate.dev/canarying"
)

// APIServerName is the DNS name ateapi's certificate is issued for.
const APIServerName = APIServiceName + "." + SystemNamespace + ".svc"

// kubeAPI is the slice of the Kubernetes API that Substrate clients use.
// Anything else answers 404 with a Status, as an API server does for a
// resource it doesn't serve.
type kubeAPI struct {
	w        *World
	token    string
	caPEM    []byte
	started  time.Time
	tokenTTL time.Duration
}

func (k *kubeAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /version", func(rw http.ResponseWriter, _ *http.Request) {
		writeJSON(rw, 200, map[string]any{"major": "1", "minor": "35", "gitVersion": "v1.35.0-substrate-fake", "platform": "linux/amd64"})
	})
	for _, v := range []string{"v1", "v1beta1"} {
		mux.HandleFunc("GET /apis/certificates.k8s.io/"+v+"/clustertrustbundles", k.trustBundles(v))
	}
	mux.HandleFunc("POST /api/v1/namespaces/{ns}/serviceaccounts/{sa}/token", k.tokenRequest)
	mux.HandleFunc("GET /apis/ate.dev/v1/workerpools", k.workerPools)
	mux.HandleFunc("GET /apis/ate.dev/v1/namespaces/{ns}/workerpools", k.workerPools)
	mux.HandleFunc("GET /apis/ate.dev/v1/namespaces/{ns}/workerpools/{name}", k.workerPool)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods/{pod}/log", k.podLog)
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		writeStatus(rw, http.StatusNotFound, "NotFound", fmt.Sprintf("the server could not find the requested resource (%s %s); substrate-fake serves only what Substrate clients read", r.Method, r.URL.Path))
	})
	return k.auth(mux)
}

func (k *kubeAPI) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if k.token != "" && r.Header.Get("Authorization") != "Bearer "+k.token {
			writeStatus(rw, http.StatusUnauthorized, "Unauthorized", "Unauthorized")
			return
		}
		next.ServeHTTP(rw, r)
	})
}

func writeJSON(rw http.ResponseWriter, code int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(code)
	_ = json.NewEncoder(rw).Encode(v)
}

func writeStatus(rw http.ResponseWriter, code int, reason, msg string) {
	writeJSON(rw, code, map[string]any{
		"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{},
		"status": "Failure", "message": msg, "reason": reason, "code": code,
	})
}

// selectorMatches handles equality selectors ("a=b,c=d"), which is all
// Substrate clients send.
func selectorMatches(sel string, labels map[string]string) bool {
	for _, term := range strings.Split(sel, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		k, v, ok := strings.Cut(term, "=")
		if !ok {
			if _, has := labels[term]; !has {
				return false
			}
			continue
		}
		if labels[strings.TrimSuffix(k, "=")] != strings.TrimPrefix(v, "=") {
			return false
		}
	}
	return true
}

func (k *kubeAPI) trustBundles(version string) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		labels := map[string]string{liveBundleLabel: "live"}
		var items []any
		if selectorMatches(r.URL.Query().Get("labelSelector"), labels) {
			items = append(items, map[string]any{
				"metadata": map[string]any{"name": "servicedns.podcert.ate.dev:identity:live", "labels": labels, "creationTimestamp": k.started.UTC().Format(time.RFC3339)},
				"spec":     map[string]any{"signerName": ServiceDNSSigner, "trustBundle": string(k.caPEM)},
			})
		}
		writeJSON(rw, 200, map[string]any{
			"kind": "ClusterTrustBundleList", "apiVersion": "certificates.k8s.io/" + version,
			"metadata": map[string]any{}, "items": orEmpty(items),
		})
	}
}

func orEmpty(v []any) []any {
	if v == nil {
		return []any{}
	}
	return v
}

// tokenRequest mints the fake's one bearer token for the ate-client
// ServiceAccount, the way kubectl-ate gets an ateapi token.
func (k *kubeAPI) tokenRequest(rw http.ResponseWriter, r *http.Request) {
	ns, sa := r.PathValue("ns"), r.PathValue("sa")
	if ns != SystemNamespace || sa != ClientServiceAccount {
		writeStatus(rw, http.StatusNotFound, "NotFound", fmt.Sprintf(`serviceaccounts "%s" not found`, sa))
		return
	}
	var req struct {
		Spec map[string]any `json:"spec"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	writeJSON(rw, http.StatusCreated, map[string]any{
		"kind": "TokenRequest", "apiVersion": "authentication.k8s.io/v1",
		"metadata": map[string]any{"name": sa, "namespace": ns}, "spec": req.Spec,
		"status": map[string]any{"token": k.token, "expirationTimestamp": k.w.Now().Add(k.tokenTTL).UTC().Format(time.RFC3339)},
	})
}

func (k *kubeAPI) poolObject(p Pool) map[string]any {
	cond := map[string]any{"type": "Ready", "status": "True", "reason": "WorkersReady", "lastTransitionTime": k.started.Add(-72 * time.Hour).UTC().Format(time.RFC3339)}
	if p.Ready < p.Replicas {
		since := p.NotReadySince
		if since.IsZero() {
			since = k.started
		}
		cond = map[string]any{"type": "Ready", "status": "False", "reason": "WorkersNotReady",
			"message": fmt.Sprintf("%d of %d workers ready", p.Ready, p.Replicas), "lastTransitionTime": since.UTC().Format(time.RFC3339)}
	}
	return map[string]any{
		"apiVersion": "ate.dev/v1", "kind": "WorkerPool",
		"metadata": map[string]any{
			"name": p.Name, "namespace": p.Namespace, "labels": p.Labels, "uid": "pool-" + p.Name,
			"creationTimestamp": k.started.Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339),
		},
		"spec":   map[string]any{"replicas": p.Replicas, "sandboxClasses": []any{map[string]any{"name": p.SandboxClass}}},
		"status": map[string]any{"replicas": p.Ready, "readyReplicas": p.Ready, "conditions": []any{cond}},
	}
}

func (k *kubeAPI) workerPools(rw http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	sel := r.URL.Query().Get("labelSelector")
	var items []any
	for _, p := range k.w.Pools() {
		if (ns == "" || p.Namespace == ns) && selectorMatches(sel, p.Labels) {
			items = append(items, k.poolObject(p))
		}
	}
	writeJSON(rw, 200, map[string]any{"apiVersion": "ate.dev/v1", "kind": "WorkerPoolList", "metadata": map[string]any{"resourceVersion": "1"}, "items": orEmpty(items)})
}

func (k *kubeAPI) workerPool(rw http.ResponseWriter, r *http.Request) {
	for _, p := range k.w.Pools() {
		if p.Namespace == r.PathValue("ns") && p.Name == r.PathValue("name") {
			writeJSON(rw, 200, k.poolObject(p))
			return
		}
	}
	writeStatus(rw, http.StatusNotFound, "NotFound", fmt.Sprintf(`workerpools.ate.dev "%s" not found`, r.PathValue("name")))
}

func (k *kubeAPI) podLog(rw http.ResponseWriter, r *http.Request) {
	lines, ok := k.w.podLogs(r.PathValue("ns"), r.PathValue("pod"))
	if !ok {
		writeStatus(rw, http.StatusNotFound, "NotFound", fmt.Sprintf(`pods "%s" not found`, r.PathValue("pod")))
		return
	}
	q := r.URL.Query()
	if n, err := strconv.Atoi(q.Get("tailLines")); err == nil && n >= 0 && n < len(lines) {
		lines = lines[len(lines)-n:]
	}
	stamps := q.Get("timestamps") == "true"
	rw.Header().Set("Content-Type", "text/plain")
	for _, l := range lines {
		if stamps {
			fmt.Fprintf(rw, "%s %s\n", l.Time.UTC().Format(time.RFC3339Nano), l.Text)
		} else {
			fmt.Fprintln(rw, l.Text)
		}
	}
}
