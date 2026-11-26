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
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/wal"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		if port := os.Getenv("PORT"); port != "" {
			addr = ":" + port
		} else {
			addr = ":8090"
		}
	}
	ndjsonPath := os.Getenv("NDJSON_PATH")
	walPath := os.Getenv("WAL_PATH")
	ingestAPIKey := os.Getenv("INGEST_API_KEY")

	store, err := storage.NewIndexedStore(200_000, ndjsonPath)
	if err != nil {
		logger.Error("store_init_failed", "error", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	var walLog *wal.WAL
	if walPath != "" {
		walLog, err = wal.Open(walPath)
		if err != nil {
			logger.Error("wal_init_failed", "error", err.Error())
			os.Exit(1)
		}
		defer walLog.Close()
	}

	detector := anomaly.NewDetector(3.0, 20)
	pipe := pipeline.NewWithWAL(pipeline.DefaultConfig(), detector, store, walLog)
	pipe.WarmDetectorFromStore()

	if walLog != nil {
		recovered, recErr := walLog.RecoverRecords()
		if recErr != nil {
			logger.Warn("wal_recovery_partial", "error", recErr.Error(), "recovered_count", len(recovered))
		}
		if len(recovered) > 0 {
			if err := pipe.ReplayRecoveredRecords(recovered); err != nil {
				logger.Error("wal_replay_failed", "error", err.Error())
			} else {
				logger.Info("wal_recovered_events", "count", len(recovered))
			}
		}
	}

	if ingestAPIKey == "" {
		logger.Warn("ingest_api_key_unset", "detail", "API endpoints are open without INGEST_API_KEY; set INGEST_API_KEY in production")
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      httpapi.NewServerWithAuth(pipe, store, ingestAPIKey),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("log_pipeline_listening", "addr", addr, "wal_enabled", walPath != "", "cold_storage_enabled", ndjsonPath != "")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_error", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	if err := srv.Shutdown(httpCtx); err != nil {
		logger.Error("http_shutdown_error", "error", err.Error())
	}

	pipeCtx, pipeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pipeCancel()
	if err := pipe.Shutdown(pipeCtx); err != nil {
		logger.Error("pipeline_shutdown_error", "error", err.Error())
	}
	logger.Info("log_pipeline_stopped")
}
