# Dockerfile for AIEN Intent Service
#
# WHY MULTI-STAGE BUILDS?
# =======================
# A naive Dockerfile uses the official golang image as the final container:
#   FROM golang:1.26.5
#   COPY . .
#   CMD ["go", "run", "main.go"]
#
# This has massive drawbacks:
# 1. Size: The image contains the entire Go compiler, SDK, and tooling (~800MB).
# 2. Performance: Compiling code on startup slows container boot times.
# 3. Security: If a container is compromised, the attacker has a compiler
#    and shell tools, making it easy to compile malicious payloads.
#
# Multi-stage builds solve this:
# - Stage 1 (builder): Uses the heavy Go SDK image to compile a statically
#   linked binary.
# - Stage 2 (final): Starts with a minimal Linux environment (like alpine).
#   We copy ONLY the compiled binary. The final image size is ~20MB and has
#   zero developer tools, making it extremely secure.
#
# STAGE 1: Build the binary
FROM golang:1.26.5-alpine AS builder

# Install build dependencies (git, build-base might be needed for CGO).
# We disable CGO (CGo enabled = 0) so Go builds a statically linked binary
# that runs anywhere without needing dynamic libraries (libc).
RUN apk add --no-cache git build-base

WORKDIR /app

# Copy the workspace configuration and module descriptors.
# We copy these first and run "go work sync" or "go mod download" so
# Docker can cache the dependencies. If dependencies haven't changed,
# subsequent builds skip this step, reducing build times.
COPY go.work .
COPY gateway/go.mod gateway/
COPY intent-service/go.mod intent-service/
COPY proto/go.mod proto/
COPY shared/go.mod shared/
COPY wallet/go.mod wallet/
COPY scheduler/go.mod scheduler/
COPY execution-engine/go.mod execution-engine/
COPY validator/go.mod validator/

# Copy the actual source code.
COPY proto/ proto/
COPY shared/ shared/
COPY intent-service/ intent-service/

# Compile the binary.
# CGO_ENABLED=0: Disable dynamic linking.
# GOOS=linux: Target Linux OS (even if building on Windows/macOS).
# -ldflags="-w -s": Strip debug symbols to reduce binary size by ~30%.
WORKDIR /app/intent-service
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o intent-service .

# STAGE 2: Minimal run-time container
FROM alpine:3.20

# Install ca-certificates (needed for making secure HTTPS calls).
RUN apk --no-cache add ca-certificates

WORKDIR /app/

# Copy the compiled binary from the builder stage.
COPY --from=builder /app/intent-service/intent-service .

# Copy the schema file so the service can run migrations on startup.
# We recreate the folder structure so it matches config defaults.
COPY --from=builder /app/intent-service/store/schema.sql store/

# Inform Docker that the container listens on port 50051.
EXPOSE 50051

# Run the binary.
ENTRYPOINT ["./intent-service"]
