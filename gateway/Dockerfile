# Dockerfile for AIEN API Gateway
#
# Follows the same multi-stage build architecture as the Intent Service
# to produce a minimal (~20MB), secure production container.
#
# STAGE 1: Build the binary
FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /app

# Copy the workspace configuration and module descriptors.
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
COPY gateway/ gateway/

# Compile the gateway binary.
WORKDIR /app/gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o gateway .

# STAGE 2: Minimal run-time container
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app/

# Copy the compiled binary from the builder stage.
COPY --from=builder /app/gateway/gateway .

# Inform Docker that the gateway listens on port 8081.
EXPOSE 8081

# Run the binary.
ENTRYPOINT ["./gateway"]
