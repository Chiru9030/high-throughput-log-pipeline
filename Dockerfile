FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /log-pipeline ./cmd/pipeline

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data
COPY --from=builder /log-pipeline /usr/local/bin/log-pipeline
WORKDIR /data
USER appuser
EXPOSE 8090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD /bin/sh -c 'wget -qO- "http://127.0.0.1:${PORT:-8090}/healthz" || exit 1'
ENTRYPOINT ["log-pipeline"]
