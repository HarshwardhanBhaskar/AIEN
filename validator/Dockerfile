# Dockerfile for AIEN Validator Service
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
COPY validator/ validator/

WORKDIR /app/validator
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o validator .

FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app/

# Copy the compiled binary.
COPY --from=builder /app/validator/validator .

# Create the data volume directory.
RUN mkdir -p /app/data

EXPOSE 8083

ENTRYPOINT ["./validator"]
