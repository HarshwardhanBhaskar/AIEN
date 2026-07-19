# Dockerfile for AIEN Execution Engine
FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /app

COPY go.work .
COPY gateway/go.mod gateway/
COPY intent-service/go.mod intent-service/
COPY proto/go.mod proto/
COPY shared/go.mod shared/
COPY wallet/go.mod wallet/
COPY scheduler/go.mod scheduler/
COPY execution-engine/go.mod execution-engine/
COPY validator/go.mod validator/

COPY proto/ proto/
COPY shared/ shared/
COPY execution-engine/ execution-engine/

WORKDIR /app/execution-engine
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o execution-engine .

FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app/

COPY --from=builder /app/execution-engine/execution-engine .

ENTRYPOINT ["./execution-engine"]
