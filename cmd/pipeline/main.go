package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/httpapi"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/pipeline"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	ndjsonPath := os.Getenv("NDJSON_PATH")

	store, err := storage.NewIndexedStore(200_000, ndjsonPath)
	if err != nil {
		logger.Error("store_init_failed", "error", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	detector := anomaly.NewDetector(3.0, 20)
	pipe := pipeline.New(pipeline.DefaultConfig(), detector, store)
	srv := &http.Server{
		Addr:         addr,
		Handler:      httpapi.NewServer(pipe, store),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("log_pipeline_listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_error", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = pipe.Shutdown(ctx)
	logger.Info("log_pipeline_stopped")
}
