// Package main is the entry point for the AIEN API Gateway.
//
// WHAT IS AN API GATEWAY?
// =======================
// An API Gateway is the single entry point for all external clients.
// Instead of exposing every microservice directly (which creates
// coupling, security risks, and protocol complexity), the Gateway:
//
// 1. Accepts HTTP/JSON requests (easy for browsers, curl, Postman).
// 2. Translates them into gRPC calls to internal services.
// 3. Translates gRPC responses back to HTTP/JSON.
// 4. Handles cross-cutting concerns:
//    - Authentication (verify tokens before routing)
//    - Rate limiting (protect backend services)
//    - Request logging (centralized audit trail)
//    - CORS (browser security headers)
//
// WHY NOT EXPOSE gRPC DIRECTLY?
// =============================
// 1. Browsers can't speak gRPC natively (binary protocol over HTTP/2).
// 2. Curl can't easily call gRPC endpoints.
// 3. Mobile apps often prefer REST/JSON for simplicity.
// 4. Security: Internal services should not be directly reachable.
//
// The Gateway pattern is used by Google, Netflix, Amazon, and
// virtually every large-scale microservice architecture.
//
// TRADE-OFFS:
// - Adds one network hop (client → gateway → service).
// - Single point of failure (mitigated by running multiple replicas).
// - Must be kept in sync with backend proto changes.
package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aien-platform/aien/shared/config"
	"github.com/aien-platform/aien/shared/logger"

	intentv1 "github.com/aien-platform/aien/proto/intent/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aien_gateway_http_requests_total",
			Help: "Total number of HTTP requests processed by AIEN Gateway.",
		},
		[]string{"method", "path", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aien_gateway_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

//go:embed web/*
var webAssets embed.FS

// Gateway holds the HTTP server, gRPC client connections, and Redis client.
type Gateway struct {
	logger       *slog.Logger
	intentClient intentv1.IntentServiceClient
	rdb          *redis.Client
	jwtSecret    string
}

func main() {
	log := logger.New("gateway")

	// Load config from environment.
	httpPort := config.GetEnv("GATEWAY_PORT", ":8081")
	intentServiceAddr := config.GetEnv("INTENT_SERVICE_ADDR", "localhost:50051")
	redisAddr := config.GetEnv("REDIS_URL", "localhost:6379")
	jwtSecret := config.GetEnv("GATEWAY_JWT_SECRET", "aien-jwt-session-secret-key")

	log.Info("Loaded configuration parameters",
		"port", httpPort,
		"intent_service_addr", intentServiceAddr,
		"redis_url", redisAddr,
		"jwt_secret_configured", jwtSecret != "",
	)

	// --- Connect to Redis Distributed Cache ---
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()
	if err := rdb.Ping(ctxPing).Err(); err != nil {
		log.Error("Failed to connect to Redis", "addr", redisAddr, "error", err)
		os.Exit(1)
	}
	log.Info("Connected to Redis cache", "addr", redisAddr)

	// --- Connect to Intent Service via gRPC ---
	//
	// grpc.Dial creates a CLIENT connection to the Intent Service.
	// This is the other side of the gRPC equation:
	//   intent-service runs grpc.NewServer() + Serve()  (SERVER)
	//   gateway runs grpc.Dial()                        (CLIENT)
	//
	// Load TLS credentials if environment variables are provided
	caCertPath := config.GetEnv("TLS_CA_CERT", "")
	clientCertPath := config.GetEnv("TLS_CLIENT_CERT", "")
	clientKeyPath := config.GetEnv("TLS_CLIENT_KEY", "")

	var clientOpts []grpc.DialOption

	if caCertPath != "" && clientCertPath != "" && clientKeyPath != "" {
		log.Info("Configuring gRPC client with Mutual TLS (mTLS)...")

		// Load client certificate and key
		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			log.Error("Failed to load client TLS key pair", "cert", clientCertPath, "key", clientKeyPath, "error", err)
			os.Exit(1)
		}

		// Load CA cert to verify server certs
		caCertBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			log.Error("Failed to read CA cert file", "path", caCertPath, "error", err)
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			log.Error("Failed to append CA certificate to pool")
			os.Exit(1)
		}

		// Configure TLS client settings
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
			ServerName:   "intent-service", // Matches CommonName/SAN in server cert
		}

		creds := credentials.NewTLS(tlsConfig)
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(creds))
		log.Info("mTLS successfully configured for gRPC client.")
	} else {
		log.Warn("TLS environment variables not set; starting insecure gRPC client.")
		clientOpts = append(clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(intentServiceAddr, clientOpts...)
	if err != nil {
		log.Error("Failed to connect to intent-service", "addr", intentServiceAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	log.Info("Connected to intent-service", "addr", intentServiceAddr)

	// Create a typed client from the generated gRPC code.
	// This gives us type-safe methods like SubmitIntent(), GetIntent(), etc.
	gw := &Gateway{
		logger:       log,
		intentClient: intentv1.NewIntentServiceClient(conn),
		rdb:          rdb,
		jwtSecret:    jwtSecret,
	}

	// --- Set up HTTP routes ---
	mux := http.NewServeMux()

	// Expose Web Dashboard
	subFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		log.Error("Failed to initialize embedded web filesystem", "error", err)
		os.Exit(1)
	}
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard/", http.FileServer(http.FS(subFS))))

	// Expose API endpoints
	mux.HandleFunc("/api/ledger", gw.handleAPILedger)
	mux.HandleFunc("/api/login", gw.handleLogin)

	// Health check endpoint (unchanged from Phase 0).
	mux.HandleFunc("/health", gw.handleHealth)

	// Expose Prometheus metrics route.
	mux.Handle("/metrics", promhttp.Handler())

	// Intent endpoints.
	// POST /intents     → Submit a new intent.
	// GET  /intents     → List intents.
	// GET  /intents/{id} → Get a specific intent.
	mux.HandleFunc("/intents", gw.handleIntents)
	mux.HandleFunc("/intents/", gw.handleIntentByID)

	// Wrap with authentication, logging, and rate limiting middleware.
	handler := gw.loggingMiddleware(gw.authMiddleware(gw.rateLimitMiddleware(mux)))

	srv := &http.Server{
		Addr:         httpPort,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// --- Graceful shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Info("Received shutdown signal", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	log.Info("API Gateway starting", "port", httpPort)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("Gateway server failed", "error", err)
		os.Exit(1)
	}

	log.Info("API Gateway stopped gracefully")
}

// =============================================================================
// HTTP HANDLERS
// =============================================================================

// SubmitIntentHTTPRequest is the JSON structure accepted by POST /intents.
//
// WHY A SEPARATE HTTP REQUEST TYPE?
// The proto-generated SubmitIntentRequest uses protobuf types (bytes,
// enums as integers). The HTTP API should be human-friendly:
// - Type as a string ("TRANSFER") instead of an integer (1).
// - Payload as a JSON string instead of raw bytes.
// This struct performs the translation.
type SubmitIntentHTTPRequest struct {
	Type               string           `json:"type"`
	Payload            string           `json:"payload"`
	SubmitterID        string           `json:"submitter_id"`
	Signature          string           `json:"signature"`
	SubmitterPublicKey string           `json:"submitter_public_key"`
	Constraints        *ConstraintsHTTP `json:"constraints,omitempty"`
}

// ConstraintsHTTP is the JSON-friendly version of IntentConstraints.
type ConstraintsHTTP struct {
	Priority   int32  `json:"priority,omitempty"`
	MaxRetries int32  `json:"max_retries,omitempty"`
	Idempotent bool   `json:"idempotent,omitempty"`
	Deadline   string `json:"deadline,omitempty"` // RFC3339 format.
}

// handleIntents routes POST and GET /intents to the appropriate handler.
func (gw *Gateway) handleIntents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		gw.submitIntent(w, r)
	case http.MethodGet:
		gw.listIntents(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// submitIntent handles POST /intents.
//
// FLOW:
// 1. Parse JSON body into SubmitIntentHTTPRequest.
// 2. Translate to protobuf SubmitIntentRequest.
// 3. Call intent-service via gRPC.
// 4. Translate protobuf response to JSON.
// 5. Return JSON response.
func (gw *Gateway) submitIntent(w http.ResponseWriter, r *http.Request) {
	// Step 1: Parse HTTP JSON body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		gw.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	defer r.Body.Close()

	var httpReq SubmitIntentHTTPRequest
	if err := json.Unmarshal(body, &httpReq); err != nil {
		gw.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Step 2: Translate HTTP request → protobuf request.
	intentType := parseIntentType(httpReq.Type)
	if intentType == intentv1.IntentType_INTENT_TYPE_UNSPECIFIED {
		gw.writeError(w, http.StatusBadRequest, "invalid intent type: "+httpReq.Type)
		return
	}

	// Step 2b: Idempotency filter check
	if httpReq.Signature != "" {
		cachedResp, err := gw.rdb.Get(r.Context(), "idempotency:"+httpReq.Signature).Result()
		if err == nil && cachedResp != "" {
			gw.logger.Info("Duplicate intent signature detected; returning cached response", "signature", httpReq.Signature)
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(cachedResp))
			return
		}
	}

	grpcReq := &intentv1.SubmitIntentRequest{
		Type:               intentType,
		Payload:            []byte(httpReq.Payload),
		SubmitterId:        httpReq.SubmitterID,
		Signature:          httpReq.Signature,
		SubmitterPublicKey: httpReq.SubmitterPublicKey,
	}

	// Translate constraints if provided.
	if httpReq.Constraints != nil {
		grpcReq.Constraints = &intentv1.IntentConstraints{
			Priority:   httpReq.Constraints.Priority,
			MaxRetries: httpReq.Constraints.MaxRetries,
			Idempotent: httpReq.Constraints.Idempotent,
		}
	}

	// Step 3: Call intent-service via gRPC.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	grpcResp, err := gw.intentClient.SubmitIntent(ctx, grpcReq)
	if err != nil {
		gw.handleGRPCError(w, err)
		return
	}

	// Step 4: Cache response for idempotency and return JSON response.
	respData := map[string]interface{}{
		"intent_id":  grpcResp.IntentId,
		"status":     grpcResp.Status.String(),
		"created_at": grpcResp.CreatedAt.AsTime().Format(time.RFC3339),
	}
	respBytes, err := json.Marshal(respData)
	if err == nil && httpReq.Signature != "" {
		gw.rdb.Set(r.Context(), "idempotency:"+httpReq.Signature, respBytes, 24*time.Hour)
	}

	w.Header().Set("X-Cache", "MISS")
	gw.writeJSON(w, http.StatusCreated, respData)
}

// handleIntentByID handles GET /intents/{id}.
func (gw *Gateway) handleIntentByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from URL path: /intents/abc-123 → "abc-123"
	id := r.URL.Path[len("/intents/"):]
	if id == "" {
		gw.writeError(w, http.StatusBadRequest, "intent_id is required in path")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	grpcResp, err := gw.intentClient.GetIntent(ctx, &intentv1.GetIntentRequest{
		IntentId: id,
	})
	if err != nil {
		gw.handleGRPCError(w, err)
		return
	}

	intent := grpcResp.Intent
	gw.writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":           intent.Id,
		"type":         intent.Type.String(),
		"status":       intent.Status.String(),
		"submitter_id": intent.SubmitterId,
		"payload":      string(intent.Payload),
		"created_at":   intent.CreatedAt.AsTime().Format(time.RFC3339),
		"updated_at":   intent.UpdatedAt.AsTime().Format(time.RFC3339),
	})
}

// listIntents handles GET /intents with optional query parameters.
func (gw *Gateway) listIntents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	grpcResp, err := gw.intentClient.ListIntents(ctx, &intentv1.ListIntentsRequest{
		SubmitterIdFilter: r.URL.Query().Get("submitter_id"),
		PageToken:         r.URL.Query().Get("page_token"),
	})
	if err != nil {
		gw.handleGRPCError(w, err)
		return
	}

	// Translate each intent to a JSON-friendly format.
	intents := make([]map[string]interface{}, 0, len(grpcResp.Intents))
	for _, intent := range grpcResp.Intents {
		intents = append(intents, map[string]interface{}{
			"id":           intent.Id,
			"type":         intent.Type.String(),
			"status":       intent.Status.String(),
			"submitter_id": intent.SubmitterId,
			"created_at":   intent.CreatedAt.AsTime().Format(time.RFC3339),
		})
	}

	gw.writeJSON(w, http.StatusOK, map[string]interface{}{
		"intents":         intents,
		"next_page_token": grpcResp.NextPageToken,
		"total_count":     grpcResp.TotalCount,
	})
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// handleHealth returns the gateway health status.
func (gw *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	gw.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "UP",
		"service":   "gateway",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// parseIntentType converts a string like "TRANSFER" to the protobuf enum.
func parseIntentType(s string) intentv1.IntentType {
	switch s {
	case "TRANSFER":
		return intentv1.IntentType_INTENT_TYPE_TRANSFER
	case "COMPUTE":
		return intentv1.IntentType_INTENT_TYPE_COMPUTE
	case "QUERY":
		return intentv1.IntentType_INTENT_TYPE_QUERY
	case "SCHEDULED":
		return intentv1.IntentType_INTENT_TYPE_SCHEDULED
	default:
		return intentv1.IntentType_INTENT_TYPE_UNSPECIFIED
	}
}

// handleGRPCError translates gRPC errors into appropriate HTTP responses.
//
// WHY THIS MAPPING EXISTS:
// gRPC uses its own status codes (codes.NotFound, codes.Internal, etc.)
// HTTP uses status codes (404, 500, etc.)
// This function bridges the two so HTTP clients get meaningful errors.
func (gw *Gateway) handleGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		gw.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	var httpCode int
	switch st.Code() {
	case 3: // codes.InvalidArgument
		httpCode = http.StatusBadRequest
	case 5: // codes.NotFound
		httpCode = http.StatusNotFound
	case 4: // codes.DeadlineExceeded
		httpCode = http.StatusGatewayTimeout
	case 14: // codes.Unavailable
		httpCode = http.StatusServiceUnavailable
	case 16: // codes.Unauthenticated
		httpCode = http.StatusUnauthorized
	case 7: // codes.PermissionDenied
		httpCode = http.StatusForbidden
	default:
		httpCode = http.StatusInternalServerError
	}

	gw.writeError(w, httpCode, st.Message())
}

// writeJSON writes a JSON response with the given status code.
func (gw *Gateway) writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		gw.logger.Error("Failed to encode JSON response", "error", err)
	}
}

// writeError writes a JSON error response.
func (gw *Gateway) writeError(w http.ResponseWriter, code int, message string) {
	gw.writeJSON(w, code, map[string]string{"error": message})
}

type responseWriterDelegator struct {
	http.ResponseWriter
	status int
}

func (r *responseWriterDelegator) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// loggingMiddleware logs every incoming HTTP request and its duration.
func (gw *Gateway) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		gw.logger.Info("HTTP request started",
			"method", r.Method,
			"path", r.URL.Path,
		)

		d := &responseWriterDelegator{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(d, r)

		duration := time.Since(start)
		gw.logger.Info("HTTP request completed",
			"method", r.Method,
			"path", r.URL.Path,
			"status", d.status,
			"duration", duration.String(),
		)

		// Exclude metrics endpoint requests from polluting charts
		if r.URL.Path != "/metrics" && r.URL.Path != "/health" {
			statusStr := fmt.Sprintf("%d", d.status)
			httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, statusStr).Inc()
			httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())
		}
	})
}

// handleAPILedger reads the local ledger log file and returns it as JSON.
func (gw *Gateway) handleAPILedger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	file, err := os.Open("/app/data/ledger.log")
	if err != nil {
		if os.IsNotExist(err) {
			w.Write([]byte("[]"))
			return
		}
		gw.writeError(w, http.StatusInternalServerError, "failed to open ledger: "+err.Error())
		return
	}
	defer file.Close()

	var blocks []json.RawMessage
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		blockCopy := make([]byte, len(line))
		copy(blockCopy, line)
		blocks = append(blocks, json.RawMessage(blockCopy))
	}

	if err := scanner.Err(); err != nil {
		gw.writeError(w, http.StatusInternalServerError, "failed to scan ledger: "+err.Error())
		return
	}

	// Reverse blocks so latest blocks appear first
	for i, j := 0, len(blocks)-1; i < j; i, j = i+1, j-1 {
		blocks[i], blocks[j] = blocks[j], blocks[i]
	}

	if len(blocks) == 0 {
		w.Write([]byte("[]"))
		return
	}

	gw.writeJSON(w, http.StatusOK, blocks)
}

// rateLimitMiddleware restricts clients to a maximum number of requests per minute.
func (gw *Gateway) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if os.Getenv("DISABLE_RATE_LIMITER") == "true" {
			next.ServeHTTP(w, r)
			return
		}
		// Only rate limit submission API endpoint
		if r.Method == http.MethodPost && r.URL.Path == "/intents" {
			// Extract client IP address
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}

			ctx := r.Context()
			// Build Redis key
			minuteKey := fmt.Sprintf("rate:%s:%d", ip, time.Now().Unix()/60)

			// Atomic increment
			count, err := gw.rdb.Incr(ctx, minuteKey).Result()
			if err != nil {
				gw.logger.Error("Redis rate limit query failed", "error", err)
				// Fail open to prevent locking out users on Redis failure
				next.ServeHTTP(w, r)
				return
			}

			// Set expiry on new keys
			if count == 1 {
				gw.rdb.Expire(ctx, minuteKey, 60*time.Second)
			}

			// Set rate limit headers
			w.Header().Set("X-RateLimit-Limit", "10")
			remaining := 10 - count
			if remaining < 0 {
				remaining = 0
			}
			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))

			if count > 10 {
				gw.logger.Warn("Rate limit exceeded for client", "ip", ip, "count", count)
				w.Header().Set("Retry-After", "60")
				gw.writeError(w, http.StatusTooManyRequests, "Too many requests. Limit is 10 per minute.")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// JWTClaims defines the standard claims in our custom JWT implementation.
type JWTClaims struct {
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
}

// generateJWT signs a new JWT using HS256 algorithm with zero external dependencies.
func (gw *Gateway) generateJWT(username string) (string, error) {
	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}
	headerBytes, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	claims := JWTClaims{
		Sub: username,
		Exp: time.Now().Add(24 * time.Hour).Unix(),
	}
	claimsBytes, _ := json.Marshal(claims)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)

	signingInput := headerB64 + "." + claimsB64
	key := []byte(gw.jwtSecret)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(signingInput))
	signature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	return signingInput + "." + signature, nil
}

// validateJWT parses and verifies the signature of a JWT token string.
func (gw *Gateway) validateJWT(tokenStr string) (*JWTClaims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}

	signingInput := parts[0] + "." + parts[1]
	key := []byte(gw.jwtSecret)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(signingInput))
	expectedSignature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	if parts[2] != expectedSignature {
		return nil, fmt.Errorf("invalid signature")
	}

	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode claims: %w", err)
	}

	var claims JWTClaims
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal claims: %w", err)
	}

	if time.Now().Unix() > claims.Exp {
		return nil, fmt.Errorf("token expired")
	}

	return &claims, nil
}

// handleLogin processes POST /api/login and returns a JWT token if credentials match.
func (gw *Gateway) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		gw.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		gw.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	// Pre-configured admin credentials for demonstration
	if req.Username != "admin" || req.Password != "aien-admin-2026" {
		gw.logger.Warn("Failed login attempt", "username", req.Username, "client_ip", r.RemoteAddr)
		gw.writeError(w, http.StatusUnauthorized, "Invalid username or password")
		return
	}

	token, err := gw.generateJWT(req.Username)
	if err != nil {
		gw.logger.Error("Failed to generate JWT", "error", err)
		gw.writeError(w, http.StatusInternalServerError, "Internal server error generating token")
		return
	}

	gw.logger.Info("Successful login", "username", req.Username, "client_ip", r.RemoteAddr)
	gw.writeJSON(w, http.StatusOK, map[string]string{
		"token": token,
	})
}

// authMiddleware blocks any request to non-exempt API endpoints if the Authorization Bearer token is missing or incorrect.
func (gw *Gateway) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Exempt public dashboard assets, login, health check, and metrics endpoint
		if path == "/health" || path == "/metrics" || path == "/api/login" || strings.HasPrefix(path, "/dashboard/") {
			next.ServeHTTP(w, r)
			return
		}

		// Extract Bearer token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			gw.logger.Warn("Missing or malformed Authorization header", "path", path, "client_ip", r.RemoteAddr)
			gw.writeError(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := gw.validateJWT(tokenStr)
		if err != nil {
			gw.logger.Warn("Unauthorized access attempt (invalid JWT)", "path", path, "client_ip", r.RemoteAddr, "error", err)
			gw.writeError(w, http.StatusUnauthorized, fmt.Sprintf("unauthorized: %v", err))
			return
		}

		// Add verified subject to context
		ctx := context.WithValue(r.Context(), "user", claims.Sub)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
