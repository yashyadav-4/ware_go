#stage 1;build binary
FROM golang:alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=vendor \
    -ldflags="-s -w" \
    -o /app/warego-server ./cmd/server

#stage 2= minimal production runtime
FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates curl && \
    addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /app/warego-server /app/warego-server

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/warego-server"]