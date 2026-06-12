package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// MetricsCollector tracks proxy performance
type MetricsCollector struct {
	totalRequests      int64
	totalErrors        int64
	totalCached        int64
	avgResponseTime    int64
	mu                 sync.RWMutex
	requestsByEndpoint map[string]int64
	errorsByEndpoint   map[string]int64
}

func (m *MetricsCollector) RecordRequest(endpoint string, responseTime int64) {
	atomic.AddInt64(&m.totalRequests, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestsByEndpoint[endpoint]++
	m.avgResponseTime = (m.avgResponseTime + responseTime) / 2
}

func (m *MetricsCollector) RecordError(endpoint string) {
	atomic.AddInt64(&m.totalErrors, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorsByEndpoint[endpoint]++
}

func (m *MetricsCollector) RecordCacheHit() {
	atomic.AddInt64(&m.totalCached, 1)
}

func (m *MetricsCollector) GetMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"total_requests":       atomic.LoadInt64(&m.totalRequests),
		"total_errors":         atomic.LoadInt64(&m.totalErrors),
		"total_cached":         atomic.LoadInt64(&m.totalCached),
		"avg_response_time_ms": atomic.LoadInt64(&m.avgResponseTime),
		"requests_by_endpoint": m.requestsByEndpoint,
		"errors_by_endpoint":   m.errorsByEndpoint,
	}
}

// CacheEntry represents a cached response
type CacheEntry struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	ExpiresAt  time.Time
	Created    time.Time
}

// ResponseCache handles caching with TTL
type ResponseCache struct {
	cache map[string]*CacheEntry
	mu    sync.RWMutex
}

func NewResponseCache() *ResponseCache {
	rc := &ResponseCache{
		cache: make(map[string]*CacheEntry),
	}
	// Cleanup expired entries every minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rc.cleanup()
		}
	}()
	return rc
}

func (rc *ResponseCache) Get(key string) (*CacheEntry, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	entry, exists := rc.cache[key]
	if !exists || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry, true
}

func (rc *ResponseCache) Set(key string, entry *CacheEntry) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.cache[key] = entry
}

func (rc *ResponseCache) cleanup() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	now := time.Now()
	for key, entry := range rc.cache {
		if now.After(entry.ExpiresAt) {
			delete(rc.cache, key)
		}
	}
}

// RateLimiter implements token bucket algorithm
type RateLimiter struct {
	clients map[string]*TokenBucket
	mu      sync.RWMutex
	limit   int
	window  time.Duration
}

type TokenBucket struct {
	tokens    float64
	maxTokens float64
	refillRate float64
	lastRefill time.Time
}

func NewRateLimiter(requestsPerSecond int) *RateLimiter {
	return &RateLimiter{
		clients: make(map[string]*TokenBucket),
		limit:   requestsPerSecond,
		window:  1 * time.Second,
	}
}

func (rl *RateLimiter) Allow(clientIP string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.clients[clientIP]
	if !exists {
		bucket = &TokenBucket{
			tokens:     float64(rl.limit),
			maxTokens:  float64(rl.limit),
			refillRate: float64(rl.limit),
			lastRefill: time.Now(),
		}
		rl.clients[clientIP] = bucket
	}

	now := time.Now()
	timePassed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens = bucket.tokens + (timePassed * bucket.refillRate)
	if bucket.tokens > bucket.maxTokens {
		bucket.tokens = bucket.maxTokens
	}
	bucket.lastRefill = now

	if bucket.tokens >= 1 {
		bucket.tokens--
		return true
	}
	return false
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	endpoint     string
	failureCount int32
	lastFailTime time.Time
	state        string // "closed", "open", "half-open"
	mu           sync.RWMutex
	threshold    int32
	timeout      time.Duration
}

func NewCircuitBreaker(endpoint string) *CircuitBreaker {
	return &CircuitBreaker{
		endpoint:  endpoint,
		state:     "closed",
		threshold: 5,
		timeout:   30 * time.Second,
	}
}

func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.state == "open" {
		if time.Since(cb.lastFailTime) > cb.timeout {
			cb.state = "half-open"
			return false
		}
		return true
	}
	return false
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount = 0
	cb.state = "closed"
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	cb.lastFailTime = time.Now()
	if cb.failureCount >= cb.threshold {
		cb.state = "open"
	}
}

// HealthChecker performs periodic health checks
type HealthChecker struct {
	backends map[string]*BackendServer
	mu       sync.RWMutex
	interval time.Duration
}

type BackendServer struct {
	URL       *url.URL
	Healthy   bool
	LastCheck time.Time
}

func NewHealthChecker(interval time.Duration) *HealthChecker {
	hc := &HealthChecker{
		backends: make(map[string]*BackendServer),
		interval: interval,
	}
	go hc.start()
	return hc
}

func (hc *HealthChecker) RegisterBackend(urlStr string) error {
	u, err := url.Parse(urlStr)
	if err != nil {
		return err
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.backends[urlStr] = &BackendServer{
		URL:     u,
		Healthy: true,
	}
	return nil
}

func (hc *HealthChecker) start() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()
	for range ticker.C {
		hc.checkAll()
	}
}

func (hc *HealthChecker) checkAll() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	for _, backend := range hc.backends {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := http.Get(backend.URL.String() + "/health")
		cancel()

		healthy := err == nil && resp.StatusCode == 200
		if resp != nil {
			resp.Body.Close()
		}
		backend.Healthy = healthy
		backend.LastCheck = time.Now()
	}
}

func (hc *HealthChecker) GetHealthyBackend() *url.URL {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	for _, backend := range hc.backends {
		if backend.Healthy {
			return backend.URL
		}
	}
	return nil
}

// RequestQueue handles queued requests during outages
type RequestQueue struct {
	queue chan *QueuedRequest
	mu    sync.Mutex
	count int32
}

type QueuedRequest struct {
	ID        string
	Request   *http.Request
	CreatedAt time.Time
}

func NewRequestQueue(maxSize int) *RequestQueue {
	return &RequestQueue{
		queue: make(chan *QueuedRequest, maxSize),
	}
}

func (rq *RequestQueue) Enqueue(req *http.Request) (string, error) {
	qr := &QueuedRequest{
		ID:        uuid.New().String(),
		Request:   req,
		CreatedAt: time.Now(),
	}
	select {
	case rq.queue <- qr:
		atomic.AddInt32(&rq.count, 1)
		return qr.ID, nil
	default:
		return "", fmt.Errorf("queue full")
	}
}

func (rq *RequestQueue) Dequeue() *QueuedRequest {
	select {
	case qr := <-rq.queue:
		atomic.AddInt32(&rq.count, -1)
		return qr
	default:
		return nil
	}
}

func (rq *RequestQueue) Size() int32 {
	return atomic.LoadInt32(&rq.count)
}

// FastProxy is the main proxy handler
type FastProxy struct {
	backends      []*url.URL
	cache         *ResponseCache
	rateLimiter   *RateLimiter
	metrics       *MetricsCollector
	circuitBreaker map[string]*CircuitBreaker
	healthChecker *HealthChecker
	requestQueue  *RequestQueue
	mu            sync.RWMutex
	currentBackend int
	tlsConfig     *tls.Config
}

func NewFastProxy(backendURLs []string, rateLimitPerSec int) (*FastProxy, error) {
	backends := make([]*url.URL, 0, len(backendURLs))
	for _, urlStr := range backendURLs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		backends = append(backends, u)
	}

	fp := &FastProxy{
		backends:       backends,
		cache:          NewResponseCache(),
		rateLimiter:    NewRateLimiter(rateLimitPerSec),
		metrics:        &MetricsCollector{requestsByEndpoint: make(map[string]int64), errorsByEndpoint: make(map[string]int64)},
		circuitBreaker: make(map[string]*CircuitBreaker),
		healthChecker:  NewHealthChecker(10 * time.Second),
		requestQueue:   NewRequestQueue(1000),
	}

	for _, urlStr := range backendURLs {
		fp.healthChecker.RegisterBackend(urlStr)
		fp.circuitBreaker[urlStr] = NewCircuitBreaker(urlStr)
	}

	return fp, nil
}

func (fp *FastProxy) getNextBackend() *url.URL {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Round-robin with health check
	for i := 0; i < len(fp.backends); i++ {
		idx := (fp.currentBackend + i) % len(fp.backends)
		backend := fp.backends[idx]
		if !fp.circuitBreaker[backend.String()].IsOpen() {
			fp.currentBackend = (idx + 1) % len(fp.backends)
			return backend
		}
	}
	fp.currentBackend = (fp.currentBackend + 1) % len(fp.backends)
	return fp.backends[0]
}

func (fp *FastProxy) shouldCache(req *http.Request) bool {
	return req.Method == "GET" && req.Header.Get("Cache-Control") != "no-cache"
}

func (fp *FastProxy) getCacheKey(req *http.Request) string {
	return req.Method + ":" + req.RequestURI
}

func (fp *FastProxy) compressResponse(w http.ResponseWriter, statusCode int, body []byte, headers http.Header) error {
	acceptEncoding := ""
	for k, v := range headers {
		w.Header()[k] = v
		if k == "Accept-Encoding" {
			acceptEncoding = v[0]
		}
	}

	// Check client's accepted encoding
	clientAccepts := ""
	if len(acceptEncoding) > 0 {
		clientAccepts = acceptEncoding
	}

	if clientAccepts == "" || statusCode != http.StatusOK {
		w.WriteHeader(statusCode)
		_, err := w.Write(body)
		return err
	}

	w.Header().Set("Content-Encoding", clientAccepts)
	w.WriteHeader(statusCode)

	if clientAccepts == "gzip" {
		gw := gzip.NewWriter(w)
		defer gw.Close()
		_, err := gw.Write(body)
		return err
	} else if clientAccepts == "deflate" {
		fw, _ := flate.NewWriter(w, flate.DefaultCompression)
		defer fw.Close()
		_, err := fw.Write(body)
		return err
	}

	_, err := w.Write(body)
	return err
}

func (fp *FastProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	traceID := uuid.New().String()
	startTime := time.Now()
	clientIP := r.Header.Get("X-Forwarded-For")
	if clientIP == "" {
		clientIP = r.RemoteAddr
	}

	// Rate limiting
	if !fp.rateLimiter.Allow(clientIP) {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Check cache for GET requests
	if fp.shouldCache(r) {
		cacheKey := fp.getCacheKey(r)
		if entry, ok := fp.cache.Get(cacheKey); ok {
			fp.metrics.RecordCacheHit()
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("X-Trace-ID", traceID)
			fp.compressResponse(w, entry.StatusCode, entry.Body, entry.Headers)
			return
		}
	}

	backend := fp.getNextBackend()
	backendStr := backend.String()

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-Trace-ID", traceID)
		resp.Header.Set("X-Backend", backendStr)
		resp.Header.Set("X-Cache", "MISS")

		// Cache successful responses
		if fp.shouldCache(r) && resp.StatusCode == http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			cacheEntry := &CacheEntry{
				StatusCode: resp.StatusCode,
				Headers:    resp.Header.Clone(),
				Body:       bodyBytes,
				Created:    time.Now(),
				ExpiresAt:  time.Now().Add(5 * time.Minute),
			}
			fp.cache.Set(fp.getCacheKey(r), cacheEntry)
		}
		return nil
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		fp.metrics.RecordError(backendStr)
		fp.circuitBreaker[backendStr].RecordFailure()
		log.Printf("[ERROR] Trace %s: %v", traceID, err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
	}

	// Execute request
	proxy.ServeHTTP(w, r)

	// Record metrics
	duration := time.Since(startTime).Milliseconds()
	fp.metrics.RecordRequest(backendStr, duration)
	fp.circuitBreaker[backendStr].RecordSuccess()

	log.Printf("[PROXY] Trace: %s | Client: %s | Method: %s | Path: %s | Backend: %s | Duration: %dms",
		traceID, clientIP, r.Method, r.RequestURI, backendStr, duration)
}

// MetricsHandler returns JSON metrics
func (fp *FastProxy) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fp.metrics.GetMetrics())
}

// HealthHandler for health checks
func (fp *FastProxy) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// QueueStatusHandler returns queue status
func (fp *FastProxy) QueueStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queue_size": fp.requestQueue.Size(),
	})
}

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	backends := flag.String("backends", "http://localhost:9001,http://localhost:9002", "Comma-separated backend URLs")
	rateLimit := flag.Int("rate-limit", 1000, "Requests per second per client")
	flag.Parse()

	backendList := []string{}
	// Parse backends string (would need proper CSV parsing in production)
	if *backends != "" {
		backendList = append(backendList, *backends)
	}

	proxy, err := NewFastProxy([]string{"http://localhost:9001"}, *rateLimit)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	// Setup routes
	http.HandleFunc("/", proxy.ServeHTTP)
	http.HandleFunc("/metrics", proxy.MetricsHandler)
	http.HandleFunc("/health", proxy.HealthHandler)
	http.HandleFunc("/queue-status", proxy.QueueStatusHandler)

	// Setup server with timeouts
	server := &http.Server{
		Addr:         ":" + *port,
		Handler:      http.DefaultServeMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down proxy...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
	}()

	log.Printf("🚀 FART Proxy starting on port %s", *port)
	log.Printf("📊 Metrics available at http://localhost:%s/metrics", *port)
	log.Printf("💚 Health check at http://localhost:%s/health", *port)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
