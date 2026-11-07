package substratefake

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

// MetricNames picks how Substrate's OpenTelemetry instruments are named in
// Prometheus. Prometheus 3 with UTF-8 names keeps the dots
// (ate.actor.crashes); a classic translation adds units and underscores
// (ate_actor_crashes_total).
type MetricNames int

const (
	DottedNames MetricNames = iota
	UnderscoreNames
)

type names struct {
	bucket, count, sum, crashes          string
	op, tmplName, tmplSpace, pool, errTy string
}

var nameSets = map[MetricNames]names{
	DottedNames: {
		bucket: "ate.actor.lifecycle.operation.duration_bucket", count: "ate.actor.lifecycle.operation.duration_count",
		sum: "ate.actor.lifecycle.operation.duration_sum", crashes: "ate.actor.crashes",
		op: "ate.actor.operation.name", tmplName: "ate.template.name", tmplSpace: "ate.template.atespace",
		pool: "ate.workerpool.name", errTy: "error.type",
	},
	UnderscoreNames: {
		bucket: "ate_actor_lifecycle_operation_duration_seconds_bucket", count: "ate_actor_lifecycle_operation_duration_seconds_count",
		sum: "ate_actor_lifecycle_operation_duration_seconds_sum", crashes: "ate_actor_crashes_total",
		op: "ate_actor_operation_name", tmplName: "ate_template_name", tmplSpace: "ate_template_atespace",
		pool: "ate_workerpool_name", errTy: "error_type",
	},
}

// Histogram bucket bounds in seconds, fine enough that histogram_quantile
// lands near a profile's p95.
var buckets = []float64{0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.25, 1.5, 2, 3, 5, 10, math.Inf(1)}

// scrapeInterval is the spacing of generated samples.
const scrapeInterval = 30 * time.Second

// Latency is lognormal; sigma sets its spread.
const sigma = 0.3

// promDB generates series from the world on demand, so any time range can be
// queried without storing samples.
type promDB struct {
	w     *World
	names names
}

// seriesDef is one series and how to compute its value at a time.
type seriesDef struct {
	lset labels.Labels
	// value returns the counter's increment over (t-dt, t].
	inc   func(t time.Time, dt time.Duration) float64
	since time.Time // no samples before this
}

func normCDF(x float64) float64 { return 0.5 * math.Erfc(-x/math.Sqrt2) }

// series lists every series the world has right now.
func (db *promDB) series() []seriesDef {
	w, n := db.w, db.names
	w.mu.Lock()
	type tmpl struct {
		space, name, pool string
		created           time.Time
		prof              Profile
	}
	var ts []tmpl
	for k, t := range w.templates {
		pool := ""
		for _, p := range w.pools {
			if matches(t.GetWorkerSelector().GetMatchLabels(), p.Labels) {
				pool = p.Name
				break
			}
		}
		prof, ok := w.profiles[k]
		if !ok {
			prof = w.DefaultProfile
		}
		ts = append(ts, tmpl{t.Metadata.Atespace, t.Metadata.Name, pool, t.Metadata.CreateTime.AsTime(), prof})
	}
	crashes := append([]CrashEvent(nil), w.crashes...)
	w.mu.Unlock()
	sort.Slice(ts, func(i, j int) bool { return ts[i].space+"/"+ts[i].name < ts[j].space+"/"+ts[j].name })

	var out []seriesDef
	for _, t := range ts {
		prof := t.prof
		if prof.ResumesPerSecond == nil {
			continue // never resumed, so no series, as in a real Prometheus
		}
		rate := func(at time.Time) float64 {
			if prof.ResumesPerSecond == nil {
				return 0
			}
			return math.Max(prof.ResumesPerSecond(at), 0)
		}
		fail := func(at time.Time) float64 {
			if prof.ResumeFailureRatio == nil {
				return 0
			}
			return math.Min(math.Max(prof.ResumeFailureRatio(at), 0), 1)
		}
		median := func(at time.Time) float64 {
			p95 := 0.4
			if prof.ResumeP95 != nil {
				p95 = prof.ResumeP95(at).Seconds()
			}
			return p95 / math.Exp(1.645*sigma)
		}
		base := []string{n.op, "resume", n.tmplName, t.name, n.tmplSpace, t.space}
		if t.pool != "" {
			base = append(base, n.pool, t.pool)
		}
		for _, failed := range []bool{false, true} {
			share := func(at time.Time) float64 {
				if failed {
					return fail(at)
				}
				return 1 - fail(at)
			}
			lbl := append([]string{}, base...)
			if failed {
				lbl = append(lbl, n.errTy, "ResourceExhausted")
			}
			count := func(at time.Time, dt time.Duration) float64 { return rate(at) * share(at) * dt.Seconds() }
			for _, le := range buckets {
				le := le
				out = append(out, seriesDef{
					lset: labels.FromStrings(append(append([]string{labels.MetricName, n.bucket}, lbl...), "le", formatLE(le))...),
					inc: func(at time.Time, dt time.Duration) float64 {
						return count(at, dt) * normCDF((math.Log(le)-math.Log(median(at)))/sigma)
					},
					since: t.created,
				})
			}
			out = append(out,
				seriesDef{lset: labels.FromStrings(append([]string{labels.MetricName, n.count}, lbl...)...), inc: count, since: t.created},
				seriesDef{lset: labels.FromStrings(append([]string{labels.MetricName, n.sum}, lbl...)...), since: t.created,
					inc: func(at time.Time, dt time.Duration) float64 {
						return count(at, dt) * median(at) * math.Exp(sigma*sigma/2)
					}},
			)
		}
	}

	// Crashes: one counter per atespace, template and pool that has crashed.
	type ck struct{ space, tmpl, pool string }
	groups := map[ck][]time.Time{}
	for _, c := range crashes {
		k := ck{c.Atespace, c.Template, c.Pool}
		groups[k] = append(groups[k], c.Time)
	}
	for k, times := range groups {
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
		lbl := []string{labels.MetricName, n.crashes, n.tmplName, k.tmpl, n.tmplSpace, k.space}
		if k.pool != "" {
			lbl = append(lbl, n.pool, k.pool)
		}
		out = append(out, seriesDef{
			lset: labels.FromStrings(lbl...),
			inc: func(at time.Time, dt time.Duration) float64 {
				lo := sort.Search(len(times), func(i int) bool { return times[i].After(at.Add(-dt)) })
				hi := sort.Search(len(times), func(i int) bool { return times[i].After(at) })
				return float64(hi - lo)
			},
			since: times[0].Add(-24 * time.Hour),
		})
	}
	return out
}

func formatLE(le float64) string {
	if math.IsInf(le, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(le, 'f', -1, 64)
}

// Querier implements storage.Queryable.
func (db *promDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return &querier{db: db, mint: mint, maxt: maxt}, nil
}

type querier struct {
	db         *promDB
	mint, maxt int64
}

func (q *querier) Select(_ context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	mint, maxt := q.mint, q.maxt
	if hints != nil {
		mint, maxt = hints.Start, hints.End
	}
	now := q.db.w.Now()
	if t := now.UnixMilli(); maxt > t {
		maxt = t
	}
	step := scrapeInterval.Milliseconds()
	var out []storage.Series
	for _, s := range q.db.series() {
		if !matchAll(ms, s.lset) {
			continue
		}
		// Counters start at zero at the first aligned sample of the range;
		// rate and increase only look at differences.
		start := (mint/step)*step + step
		if since := s.since.UnixMilli(); start < since {
			start = (since/step)*step + step
		}
		if start > maxt {
			continue
		}
		samples := []chunks.Sample{fsample{t: start, f: 0}}
		var cum float64
		for t := start + step; t <= maxt; t += step {
			cum += s.inc(time.UnixMilli(t), scrapeInterval)
			samples = append(samples, fsample{t: t, f: cum})
		}
		out = append(out, storage.NewListSeries(s.lset, samples))
	}
	if sorted {
		sort.Slice(out, func(i, j int) bool { return labels.Compare(out[i].Labels(), out[j].Labels()) < 0 })
	}
	return &seriesSet{series: out, i: -1}
}

func matchAll(ms []*labels.Matcher, l labels.Labels) bool {
	for _, m := range ms {
		if !m.Matches(l.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (q *querier) LabelValues(_ context.Context, name string, _ *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	seen := map[string]bool{}
	for _, s := range q.db.series() {
		if v := s.lset.Get(name); v != "" && matchAll(ms, s.lset) {
			seen[v] = true
		}
	}
	return sortedKeys(seen), nil, nil
}

func (q *querier) LabelNames(_ context.Context, _ *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	seen := map[string]bool{}
	for _, s := range q.db.series() {
		if matchAll(ms, s.lset) {
			s.lset.Range(func(l labels.Label) { seen[l.Name] = true })
		}
	}
	return sortedKeys(seen), nil, nil
}

func (q *querier) Close() error { return nil }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type seriesSet struct {
	series []storage.Series
	i      int
}

func (s *seriesSet) Next() bool                        { s.i++; return s.i < len(s.series) }
func (s *seriesSet) At() storage.Series                { return s.series[s.i] }
func (s *seriesSet) Err() error                        { return nil }
func (s *seriesSet) Warnings() annotations.Annotations { return nil }

type fsample struct {
	t int64
	f float64
}

func (s fsample) T() int64                      { return s.t }
func (s fsample) ST() int64                     { return 0 }
func (s fsample) F() float64                    { return s.f }
func (s fsample) H() *histogram.Histogram       { return nil }
func (s fsample) FH() *histogram.FloatHistogram { return nil }
func (s fsample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s fsample) Copy() chunks.Sample           { return s }

// ---------- HTTP API ----------

type promAPI struct {
	db     *promDB
	engine *promql.Engine
}

func newPromAPI(w *World, n MetricNames) *promAPI {
	return &promAPI{
		db:     &promDB{w: w, names: nameSets[n]},
		engine: promql.NewEngine(promql.EngineOpts{MaxSamples: 50_000_000, Timeout: 30 * time.Second, LookbackDelta: 5 * time.Minute}),
	}
}

func (p *promAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", p.query)
	mux.HandleFunc("/api/v1/query_range", p.queryRange)
	mux.HandleFunc("GET /api/v1/labels", func(rw http.ResponseWriter, r *http.Request) {
		v, _, _ := (&querier{db: p.db}).LabelNames(r.Context(), nil)
		promOK(rw, v)
	})
	mux.HandleFunc("GET /api/v1/label/{name}/values", func(rw http.ResponseWriter, r *http.Request) {
		v, _, _ := (&querier{db: p.db}).LabelValues(r.Context(), r.PathValue("name"), nil)
		promOK(rw, v)
	})
	mux.HandleFunc("GET /api/v1/status/buildinfo", func(rw http.ResponseWriter, _ *http.Request) {
		promOK(rw, map[string]string{"version": "3.5.0-substrate-fake"})
	})
	mux.HandleFunc("GET /-/ready", func(rw http.ResponseWriter, _ *http.Request) { fmt.Fprintln(rw, "Prometheus Server is Ready.") })
	mux.HandleFunc("GET /-/healthy", func(rw http.ResponseWriter, _ *http.Request) { fmt.Fprintln(rw, "Prometheus Server is Healthy.") })
	return mux
}

func promOK(rw http.ResponseWriter, data any) {
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(map[string]any{"status": "success", "data": data})
}

func promErr(rw http.ResponseWriter, code int, typ string, err error) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(code)
	_ = json.NewEncoder(rw).Encode(map[string]any{"status": "error", "errorType": typ, "error": err.Error()})
}

// parseTime accepts Unix seconds (with a fraction) or RFC 3339, as
// Prometheus does.
func parseTime(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func parseStep(s string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return time.ParseDuration(s)
}

func (p *promAPI) query(rw http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	at, err := parseTime(r.Form.Get("time"), p.db.w.Now())
	if err != nil {
		promErr(rw, 400, "bad_data", err)
		return
	}
	q, err := p.engine.NewInstantQuery(r.Context(), p.db, nil, r.Form.Get("query"), at)
	if err != nil {
		promErr(rw, 400, "bad_data", err)
		return
	}
	p.run(rw, r, q)
}

func (p *promAPI) queryRange(rw http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	start, err1 := parseTime(r.Form.Get("start"), time.Time{})
	end, err2 := parseTime(r.Form.Get("end"), time.Time{})
	step, err3 := parseStep(r.Form.Get("step"))
	for _, err := range []error{err1, err2, err3} {
		if err != nil {
			promErr(rw, 400, "bad_data", err)
			return
		}
	}
	if step <= 0 || end.Before(start) {
		promErr(rw, 400, "bad_data", fmt.Errorf("need start <= end and a positive step"))
		return
	}
	if end.Sub(start)/step > 11000 {
		promErr(rw, 400, "bad_data", fmt.Errorf("exceeded maximum resolution of 11,000 points per timeseries"))
		return
	}
	q, err := p.engine.NewRangeQuery(r.Context(), p.db, nil, r.Form.Get("query"), start, end, step)
	if err != nil {
		promErr(rw, 400, "bad_data", err)
		return
	}
	p.run(rw, r, q)
}

func (p *promAPI) run(rw http.ResponseWriter, r *http.Request, q promql.Query) {
	defer q.Close()
	res := q.Exec(r.Context())
	if res.Err != nil {
		promErr(rw, 422, "execution", res.Err)
		return
	}
	promOK(rw, encodeValue(res.Value))
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
func sec(t int64) float64  { return float64(t) / 1000 }

func metricMap(l labels.Labels) map[string]string {
	m := map[string]string{}
	l.Range(func(x labels.Label) { m[x.Name] = x.Value })
	return m
}

func encodeValue(v parser.Value) map[string]any {
	switch v := v.(type) {
	case promql.Matrix:
		res := []any{}
		for _, s := range v {
			vals := [][2]any{}
			for _, p := range s.Floats {
				vals = append(vals, [2]any{sec(p.T), num(p.F)})
			}
			res = append(res, map[string]any{"metric": metricMap(s.Metric), "values": vals})
		}
		return map[string]any{"resultType": "matrix", "result": res}
	case promql.Vector:
		res := []any{}
		for _, s := range v {
			res = append(res, map[string]any{"metric": metricMap(s.Metric), "value": [2]any{sec(s.T), num(s.F)}})
		}
		return map[string]any{"resultType": "vector", "result": res}
	case promql.Scalar:
		return map[string]any{"resultType": "scalar", "result": [2]any{sec(v.T), num(v.V)}}
	case promql.String:
		return map[string]any{"resultType": "string", "result": [2]any{sec(v.T), v.V}}
	}
	return map[string]any{"resultType": "vector", "result": []any{}}
}
