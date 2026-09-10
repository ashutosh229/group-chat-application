package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Backend State & Performance Tracking

type BackendNode struct {
	URL    *url.URL
	RawURL string
	Proxy  *httputil.ReverseProxy

	// Concurrency & load
	ActiveConns  atomic.Int64 // In-flight HTTP + WS
	HTTPRequests atomic.Int64
	WSConns      atomic.Int64
	ActiveWS     atomic.Int64
	Messages     atomic.Int64
	Errors       atomic.Int64

	// Health check state
	mu          sync.RWMutex
	Healthy     bool
	Failures    int
	Successes   int
	LastChecked time.Time
	LastStatus  string

	// Latency & EWMA
	ewmaAlpha   float64
	ewmaLatency float64 // in ms
}

func newBackendNode(rawURL string) (*BackendNode, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	node := &BackendNode{
		URL:         u,
		RawURL:      strings.TrimRight(rawURL, "/"),
		Healthy:     true,
		ewmaAlpha:   0.3,  // weight for newest latency
		ewmaLatency: 10.0, // baseline initial latency estimate (10ms)
		LastStatus:  "Initialized",
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = u.Host
	}
	node.Proxy = proxy
	return node, nil
}

func (b *BackendNode) UpdateLatency(latencyMS float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ewmaLatency == 0 {
		b.ewmaLatency = latencyMS
	} else {
		b.ewmaLatency = (b.ewmaAlpha * latencyMS) + ((1.0 - b.ewmaAlpha) * b.ewmaLatency)
	}
}

func (b *BackendNode) GetEWMALatency() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.ewmaLatency
}

func (b *BackendNode) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Healthy
}

func (b *BackendNode) SetHealth(healthy bool, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Healthy = healthy
	b.LastChecked = time.Now()
	b.LastStatus = reason
}

// Calculate dynamic performance load score (lower is better)
func (b *BackendNode) Score(latencyThreshold float64) float64 {
	b.mu.RLock()
	ewma := b.ewmaLatency
	healthy := b.Healthy
	b.mu.RUnlock()

	if !healthy {
		return math.MaxFloat64
	}

	active := float64(b.ActiveConns.Load())
	score := (active + 1.0) * ewma

	// If EWMA latency exceeds threshold, apply penalty multiplier
	if latencyThreshold > 0 && ewma > latencyThreshold {
		penaltyRatio := ewma / latencyThreshold
		score *= (1.0 + penaltyRatio*2.0)
	}

	return score
}

// Metrics Collector

const maxSamples = 50_000

type LatStats struct {
	Count int     `json:"count"`
	Avg   float64 `json:"avg_ms"`
	Min   float64 `json:"min_ms"`
	Max   float64 `json:"max_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
}

func pctStats(samples []float64) LatStats {
	if len(samples) == 0 {
		return LatStats{}
	}
	cp := make([]float64, len(samples))
	copy(cp, samples)
	sort.Float64s(cp)
	n := len(cp)

	sum := 0.0
	for _, v := range cp {
		sum += v
	}

	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return round2(cp[idx])
	}

	return LatStats{
		Count: n,
		Avg:   round2(sum / float64(n)),
		Min:   round2(cp[0]),
		Max:   round2(cp[n-1]),
		P50:   pct(0.50),
		P90:   pct(0.90),
		P95:   pct(0.95),
		P99:   pct(0.99),
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

type BackendSnap struct {
	URL          string  `json:"url"`
	Healthy      bool    `json:"healthy"`
	Status       string  `json:"status"`
	ActiveConns  int64   `json:"active_conns"`
	EWMALatency  float64 `json:"ewma_latency_ms"`
	Score        float64 `json:"performance_score"`
	HTTPRequests int64   `json:"http_requests"`
	WSConns      int64   `json:"ws_connections"`
	ActiveWS     int64   `json:"active_ws"`
	Messages     int64   `json:"messages"`
	Errors       int64   `json:"errors"`
}

type Snapshot struct {
	Algorithm     string                 `json:"algorithm"`
	ThresholdMS   float64                `json:"threshold_ms"`
	UptimeSec     float64                `json:"uptime_s"`
	ThroughputRPS float64                `json:"throughput_rps"`
	TotalHTTP     int64                  `json:"total_http_requests"`
	TotalWS       int64                  `json:"total_ws_connections"`
	ActiveWS      int64                  `json:"active_ws_connections"`
	TotalMessages int64                  `json:"total_messages_proxied"`
	TotalErrors   int64                  `json:"total_errors"`
	PerBackend    map[string]BackendSnap `json:"per_backend"`
	HTTPLatency   LatStats               `json:"http_latency"`
	WSHandshake   LatStats               `json:"ws_handshake_latency"`
	MessageRelay  LatStats               `json:"message_relay_latency"`
}

// Load Balancer Core
type LoadBalancer struct {
	backends    []*BackendNode
	algorithm   string
	thresholdMS float64
	rrCounter   atomic.Uint64

	// Global counters
	startTime     time.Time
	totalHTTP     atomic.Int64
	totalWS       atomic.Int64
	activeWS      atomic.Int64
	totalMessages atomic.Int64
	totalErrors   atomic.Int64

	// Latency samples
	latMu   sync.Mutex
	httpLat []float64
	wsLat   []float64
	msgLat  []float64

	upgrader websocket.Upgrader
}

func newLB(rawBackends []string, algorithm string, thresholdMS float64) (*LoadBalancer, error) {
	nodes := make([]*BackendNode, 0, len(rawBackends))
	for _, raw := range rawBackends {
		node, err := newBackendNode(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid backend url %s: %w", raw, err)
		}
		nodes = append(nodes, node)
	}

	lb := &LoadBalancer{
		backends:    nodes,
		algorithm:   algorithm,
		thresholdMS: thresholdMS,
		startTime:   time.Now(),
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  4 * 1024 * 1024,
			WriteBufferSize: 4 * 1024 * 1024,
		},
	}

	return lb, nil
}

func (lb *LoadBalancer) appendLat(slice *[]float64, v float64) {
	*slice = append(*slice, v)
	if len(*slice) > maxSamples {
		*slice = (*slice)[len(*slice)-maxSamples:]
	}
}

func (lb *LoadBalancer) RecordHTTP(node *BackendNode, ms float64) {
	lb.totalHTTP.Add(1)
	node.HTTPRequests.Add(1)
	node.UpdateLatency(ms)

	lb.latMu.Lock()
	lb.appendLat(&lb.httpLat, ms)
	lb.latMu.Unlock()
}

func (lb *LoadBalancer) RecordWSOpen(node *BackendNode, ms float64) {
	lb.totalWS.Add(1)
	lb.activeWS.Add(1)
	node.WSConns.Add(1)
	node.ActiveWS.Add(1)
	node.UpdateLatency(ms)

	lb.latMu.Lock()
	lb.appendLat(&lb.wsLat, ms)
	lb.latMu.Unlock()
}

func (lb *LoadBalancer) RecordWSClose(node *BackendNode) {
	lb.activeWS.Add(-1)
	node.ActiveWS.Add(-1)
}

func (lb *LoadBalancer) RecordMessage(node *BackendNode, ms float64) {
	lb.totalMessages.Add(1)
	node.Messages.Add(1)

	lb.latMu.Lock()
	lb.appendLat(&lb.msgLat, ms)
	lb.latMu.Unlock()
}

func (lb *LoadBalancer) RecordError(node *BackendNode) {
	lb.totalErrors.Add(1)
	if node != nil {
		node.Errors.Add(1)
	}
}

// Dynamic Backend Selection Engine
func (lb *LoadBalancer) SelectBackend() (*BackendNode, error) {
	var healthyNodes []*BackendNode
	for _, b := range lb.backends {
		if b.IsHealthy() {
			healthyNodes = append(healthyNodes, b)
		}
	}

	// Fallback to all nodes if all health checks failed (prevent total outage)
	if len(healthyNodes) == 0 {
		healthyNodes = lb.backends
	}

	if len(healthyNodes) == 1 {
		return healthyNodes[0], nil
	}

	switch lb.algorithm {
	case "round_robin":
		idx := lb.rrCounter.Add(1) - 1
		return healthyNodes[idx%uint64(len(healthyNodes))], nil

	case "least_conn":
		var best *BackendNode
		minConns := int64(math.MaxInt64)
		for _, b := range healthyNodes {
			c := b.ActiveConns.Load()
			if c < minConns {
				minConns = c
				best = b
			}
		}
		return best, nil

	case "dynamic", "performance", "weighted":
		fallthrough
	default:
		// 1. Separate candidates: below threshold vs above threshold
		var underThreshold []*BackendNode
		var bestUnder *BackendNode
		minScoreUnder := math.MaxFloat64

		var bestOverall *BackendNode
		minScoreOverall := math.MaxFloat64

		for _, b := range healthyNodes {
			ewma := b.GetEWMALatency()
			score := b.Score(lb.thresholdMS)

			if score < minScoreOverall {
				minScoreOverall = score
				bestOverall = b
			}

			if lb.thresholdMS <= 0 || ewma <= lb.thresholdMS {
				underThreshold = append(underThreshold, b)
				if score < minScoreUnder {
					minScoreUnder = score
					bestUnder = b
				}
			}
		}

		// If we have backends running under the performance threshold, choose the best among them
		if len(underThreshold) > 0 && bestUnder != nil {
			return bestUnder, nil
		}

		// Otherwise, all backends are above threshold — route to lowest loaded overall
		if bestOverall != nil {
			return bestOverall, nil
		}

		return healthyNodes[0], nil
	}
}

// Active Health Checker Loop

func (lb *LoadBalancer) StartHealthChecker(interval time.Duration) {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			for _, node := range lb.backends {
				go func(b *BackendNode) {
					checkURL := b.RawURL + "/feed?limit=1"
					t0 := time.Now()
					resp, err := client.Get(checkURL)
					latency := float64(time.Since(t0).Microseconds()) / 1000.0

					b.mu.Lock()
					defer b.mu.Unlock()

					if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 400 {
						_ = resp.Body.Close()
						b.Successes++
						b.Failures = 0
						if !b.Healthy && b.Successes >= 2 {
							b.Healthy = true
							b.LastStatus = fmt.Sprintf("Recovered (200 OK, %.1fms)", latency)
							log.Printf("[HEALTH] Backend %s RECOVERED -> HEALTHY", b.RawURL)
						} else if b.Healthy {
							b.LastStatus = fmt.Sprintf("Healthy (%.1fms)", latency)
						}
						// Blend health check latency into EWMA
						b.ewmaLatency = (b.ewmaAlpha * latency) + ((1.0 - b.ewmaAlpha) * b.ewmaLatency)
					} else {
						if resp != nil {
							_ = resp.Body.Close()
						}
						b.Failures++
						b.Successes = 0
						if b.Healthy && b.Failures >= 2 {
							b.Healthy = false
							errStr := "Timeout / Connection Failed"
							if resp != nil {
								errStr = fmt.Sprintf("HTTP %d", resp.StatusCode)
							} else if err != nil {
								errStr = err.Error()
							}
							b.LastStatus = fmt.Sprintf("Unhealthy (%s)", errStr)
							log.Printf("[HEALTH] Backend %s UNHEALTHY (%s)", b.RawURL, errStr)
						}
					}
					b.LastChecked = time.Now()
				}(node)
			}
		}
	}()
}

// HTTP & WebSocket Reverse Proxy Handlers

func (lb *LoadBalancer) handleWS(w http.ResponseWriter, r *http.Request) {
	backend, err := lb.SelectBackend()
	if err != nil {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}

	backend.ActiveConns.Add(1)
	defer backend.ActiveConns.Add(-1)

	wsURL := strings.Replace(backend.RawURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = wsURL + r.URL.RequestURI()

	clientConn, err := lb.upgrader.Upgrade(w, r, nil)
	if err != nil {
		lb.RecordError(backend)
		return
	}
	defer clientConn.Close()

	t0 := time.Now()
	dialer := websocket.Dialer{
		ReadBufferSize:  4 * 1024 * 1024,
		WriteBufferSize: 4 * 1024 * 1024,
	}
	backendConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		lb.RecordError(backend)
		backend.SetHealth(false, "WebSocket Dial Failure")
		clientConn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(1011, "backend unavailable"))
		return
	}
	defer backendConn.Close()

	handshakeMS := float64(time.Since(t0).Microseconds()) / 1000.0
	lb.RecordWSOpen(backend, handshakeMS)
	defer lb.RecordWSClose(backend)

	var once sync.Once
	done := make(chan struct{})
	closeOnce := func() { once.Do(func() { close(done) }) }

	// Client -> Backend
	go func() {
		defer closeOnce()
		for {
			mt, msg, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			t := time.Now()
			if err := backendConn.WriteMessage(mt, msg); err != nil {
				return
			}
			lb.RecordMessage(backend, float64(time.Since(t).Microseconds())/1000.0)
		}
	}()

	// Backend -> Client
	go func() {
		defer closeOnce()
		for {
			mt, msg, err := backendConn.ReadMessage()
			if err != nil {
				return
			}
			t := time.Now()
			if err := clientConn.WriteMessage(mt, msg); err != nil {
				return
			}
			lb.RecordMessage(backend, float64(time.Since(t).Microseconds())/1000.0)
		}
	}()

	<-done
}

func (lb *LoadBalancer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	backend, err := lb.SelectBackend()
	if err != nil {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}

	backend.ActiveConns.Add(1)
	defer backend.ActiveConns.Add(-1)
	t0 := time.Now()
	// Custom error handler for failover / failure tracking
	backend.Proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, proxyErr error) {
		lb.RecordError(backend)
		backend.SetHealth(false, fmt.Sprintf("Proxy Error: %v", proxyErr))
		http.Error(w, fmt.Sprintf("502 Bad Gateway: %s unavailable", backend.RawURL), http.StatusBadGateway)
	}

	backend.Proxy.ServeHTTP(w, r)
	elapsedMS := float64(time.Since(t0).Microseconds()) / 1000.0
	lb.RecordHTTP(backend, elapsedMS)
}

// HTTP Router & Metrics Snapshot

func (lb *LoadBalancer) Snapshot() Snapshot {
	uptime := time.Since(lb.startTime).Seconds()
	totalReqs := lb.totalHTTP.Load() + lb.totalWS.Load()
	rps := 0.0
	if uptime > 0 {
		rps = round2(float64(totalReqs) / uptime)
	}

	pb := make(map[string]BackendSnap, len(lb.backends))
	for _, b := range lb.backends {
		pb[b.RawURL] = BackendSnap{
			URL:          b.RawURL,
			Healthy:      b.IsHealthy(),
			Status:       b.LastStatus,
			ActiveConns:  b.ActiveConns.Load(),
			EWMALatency:  round2(b.GetEWMALatency()),
			Score:        round2(b.Score(lb.thresholdMS)),
			HTTPRequests: b.HTTPRequests.Load(),
			WSConns:      b.WSConns.Load(),
			ActiveWS:     b.ActiveWS.Load(),
			Messages:     b.Messages.Load(),
			Errors:       b.Errors.Load(),
		}
	}

	lb.latMu.Lock()
	httpLat := pctStats(lb.httpLat)
	wsLat := pctStats(lb.wsLat)
	msgLat := pctStats(lb.msgLat)
	lb.latMu.Unlock()

	return Snapshot{
		Algorithm:     lb.algorithm,
		ThresholdMS:   lb.thresholdMS,
		UptimeSec:     round2(uptime),
		ThroughputRPS: rps,
		TotalHTTP:     lb.totalHTTP.Load(),
		TotalWS:       lb.totalWS.Load(),
		ActiveWS:      lb.activeWS.Load(),
		TotalMessages: lb.totalMessages.Load(),
		TotalErrors:   lb.totalErrors.Load(),
		PerBackend:    pb,
		HTTPLatency:   httpLat,
		WSHandshake:   wsLat,
		MessageRelay:  msgLat,
	}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS on load balancer
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	switch r.URL.Path {
	case "/lb/metrics":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, metricsHTML)
		return
	case "/lb/metrics.json":
		w.Header().Set("Content-Type", "application/json")
		snap := lb.Snapshot()
		json.NewEncoder(w).Encode(snap)
		return
	}

	// WebSocket upgrade check
	if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		lb.handleWS(w, r)
		return
	}

	// Forward /message, /feed, and all HTTP requests
	lb.handleHTTP(w, r)
}

// Main
func main() {
	backendsFlag := flag.String("backends",
		"http://10.1.75.79:3201,http://10.1.75.79:3202,http://10.1.75.79:3203",
		"Comma-separated backend URLs")
	port := flag.Int("port", 4000, "Port to listen on")
	host := flag.String("host", "0.0.0.0", "Host to bind")
	algo := flag.String("algorithm", "dynamic", "Load balancing algorithm: dynamic, least_conn, round_robin")
	threshold := flag.Float64("threshold", 150.0, "Performance latency threshold in milliseconds (for dynamic balancing)")
	checkInterval := flag.Duration("check-interval", 10*time.Second, "Active health check probe interval")
	flag.Parse()

	parts := strings.Split(*backendsFlag, ",")
	rawBackends := make([]string, 0, len(parts))
	for _, b := range parts {
		b = strings.TrimSpace(strings.TrimRight(b, "/"))
		if b != "" {
			rawBackends = append(rawBackends, b)
		}
	}
	if len(rawBackends) == 0 {
		log.Fatal("At least one backend URL is required (--backends)")
	}

	lb, err := newLB(rawBackends, *algo, *threshold)
	if err != nil {
		log.Fatalf("Failed to initialize load balancer: %v", err)
	}

	// Start active health check probes
	lb.StartHealthChecker(*checkInterval)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println("  🚀 PERFORMANCE-BASED DYNAMIC GO LOAD BALANCER")

	log.Printf("  Algorithm      : %s", *algo)
	log.Printf("  Threshold      : %.1f ms", *threshold)
	log.Printf("  Health Probes  : every %v (/feed?limit=1)", *checkInterval)
	log.Printf("  Backends (%d)  :", len(rawBackends))
	for _, b := range rawBackends {
		log.Printf("    • %s", b)
	}
	log.Printf("  Listening on   : http://%s", addr)
	log.Printf("  Required Routes: POST /message, GET /feed, /ws")
	log.Printf("  Dashboard      : http://%s/lb/metrics", addr)
	log.Println("================================================================")

	srv := &http.Server{
		Addr:         addr,
		Handler:      lb,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

const metricsHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Dynamic Load Balancer — Live Metrics</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Inter', system-ui, sans-serif;
    background: #0b0f19; color: #e2e8f0;
    min-height: 100vh; padding: 2rem;
  }
  .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; flex-wrap: wrap; gap: 1rem; }
  h1 {
    font-size: 1.6rem; font-weight: 700;
    background: linear-gradient(135deg, #38bdf8, #818cf8);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .badges { display: flex; gap: .5rem; align-items: center; }
  .badge { display:inline-block; border-radius:6px; font-size:.75rem; font-weight:700; padding:.2rem .6rem; }
  .badge-algo { background:#38bdf820; color:#38bdf8; border:1px solid #38bdf840; }
  .badge-thresh { background:#f59e0b20; color:#fbbf24; border:1px solid #fbbf2440; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card {
    background: #151d30; border-radius: 12px; padding: 1.2rem;
    border: 1px solid #23314e; transition: border-color .2s;
  }
  .card:hover { border-color: #38bdf8; }
  .card .label { font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; }
  .card .value { font-size: 1.8rem; font-weight: 700; color: #f1f5f9; margin-top: .3rem; }
  .card .unit { font-size: .8rem; color: #64748b; font-weight: 400; }
  table { width: 100%; border-collapse: collapse; background: #151d30; border-radius: 12px; overflow: hidden; border: 1px solid #23314e; }
  th { background: #0b0f19; font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; padding: .8rem 1rem; text-align: left; }
  td { padding: .7rem 1rem; border-top: 1px solid #1f2b45; font-variant-numeric: tabular-nums; }
  tr:nth-child(even) td { background: rgba(255,255,255,.015); }
  .section-title { font-size: 1.1rem; font-weight: 600; margin: 1.8rem 0 .8rem; color: #cbd5e1; }
  .pill { display: inline-block; padding: .2rem .6rem; border-radius: 9999px; font-size: .7rem; font-weight: 600; }
  .pill-ok  { background: #065f4625; color: #34d399; border: 1px solid #34d39950; }
  .pill-err { background: #7f1d1d25; color: #f87171; border: 1px solid #f8717150; }
  .latency-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; }
  #status { font-size: .8rem; color: #64748b; }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>⚖️ Performance-Based Dynamic Load Balancer</h1>
    <div id="status">Connecting to metrics engine…</div>
  </div>
  <div class="badges">
    <span class="badge badge-algo" id="algo-badge">Algorithm: Dynamic</span>
    <span class="badge badge-thresh" id="thresh-badge">Threshold: 150 ms</span>
  </div>
</div>

<div class="grid" id="summary-cards"></div>

<h2 class="section-title">Backend Performance & Health</h2>
<table id="backend-table">
  <thead>
    <tr>
      <th>Backend URL</th>
      <th>Health</th>
      <th>EWMA Latency</th>
      <th>Active Conns</th>
      <th>Score</th>
      <th>HTTP Reqs</th>
      <th>WS Conns</th>
      <th>Messages</th>
      <th>Errors</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<h2 class="section-title">Latency Distribution (Response Time)</h2>
<div class="latency-grid" id="latency-section"></div>

<script>
function card(label, value, unit) {
  return '<div class="card"><div class="label">'+label+'</div><div class="value">'+value+' <span class="unit">'+(unit||'')+'</span></div></div>';
}
function latCard(title, d) {
  if (!d || d.count === 0) return '<div class="card"><div class="label">'+title+'</div><div class="value">—</div></div>';
  return '<div class="card"><div class="label">'+title+' ('+d.count+' samples)</div>'
    +'<table style="margin-top:.6rem;font-size:.82rem;background:transparent;border:none;">'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Avg Latency</td><td style="border:none;padding:.2rem .5rem">'+d.avg_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P50 (Median)</td><td style="border:none;padding:.2rem .5rem">'+d.p50_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P90</td><td style="border:none;padding:.2rem .5rem">'+d.p90_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P95</td><td style="border:none;padding:.2rem .5rem">'+d.p95_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P99</td><td style="border:none;padding:.2rem .5rem">'+d.p99_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Min / Max</td><td style="border:none;padding:.2rem .5rem">'+d.min_ms+' / '+d.max_ms+' ms</td></tr>'
    +'</table></div>';
}
async function refresh() {
  try {
    const r = await fetch('/lb/metrics.json');
    const m = await r.json();
    document.getElementById('status').textContent = 'Live · ' + new Date().toLocaleTimeString();
    document.getElementById('algo-badge').textContent = 'Algorithm: ' + (m.algorithm || 'Dynamic');
    document.getElementById('thresh-badge').textContent = 'Threshold: ' + m.threshold_ms + ' ms';
    document.getElementById('summary-cards').innerHTML = [
      card('Uptime', m.uptime_s, 's'),
      card('Throughput', m.throughput_rps, 'req/s'),
      card('HTTP Requests', m.total_http_requests),
      card('WS Connections', m.total_ws_connections, 'total'),
      card('Active WS', m.active_ws_connections, 'active'),
      card('Messages Proxied', m.total_messages_proxied),
      card('Total Errors', m.total_errors),
    ].join('');
    const tbody = document.querySelector('#backend-table tbody');
    tbody.innerHTML = Object.entries(m.per_backend).map(([b,d]) => {
      const health = d.healthy
        ? '<span class="pill pill-ok">Healthy</span>'
        : '<span class="pill pill-err">Unhealthy: '+(d.status||'Down')+'</span>';
      return '<tr>'
        +'<td><strong>'+b+'</strong></td>'
        +'<td>'+health+'</td>'
        +'<td>'+d.ewma_latency_ms+' ms</td>'
        +'<td>'+d.active_conns+'</td>'
        +'<td>'+d.performance_score+'</td>'
        +'<td>'+d.http_requests+'</td>'
        +'<td>'+d.ws_connections+'</td>'
        +'<td>'+d.messages+'</td>'
        +'<td>'+d.errors+'</td>'
        +'</tr>';
    }).join('');
    document.getElementById('latency-section').innerHTML = [
      latCard('HTTP Request Latency (/message, /feed)', m.http_latency),
      latCard('WebSocket Handshake Latency', m.ws_handshake_latency),
      latCard('Message Relay Latency', m.message_relay_latency),
    ].join('');
  } catch(e) {
    document.getElementById('status').textContent = 'Metrics Error: '+e.message;
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>`
