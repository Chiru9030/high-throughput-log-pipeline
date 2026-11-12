FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -o /log-pipeline ./cmd/pipeline

FROM alpine:3.20
COPY --from=builder /log-pipeline /usr/local/bin/log-pipeline
EXPOSE 8090
ENTRYPOINT ["log-pipeline"]

