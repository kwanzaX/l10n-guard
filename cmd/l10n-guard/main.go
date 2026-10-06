// l10n-guard runs the HTTP API, the outbox relay and the check worker in one binary.
// Each part can also be scaled on its own with -mode.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kwanzaX/l10n-guard/internal/pipeline"
	"github.com/kwanzaX/l10n-guard/internal/store"
	"github.com/segmentio/kafka-go"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	mode := flag.String("mode", "all", "all | api | relay | worker")
	flag.Parse()

	pgURL := env("PG_URL", "postgres://postgres:postgres@localhost:5432/l10n")
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, pgURL)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if *mode != "api" {
		if err := pipeline.EnsureTopic(ctx, brokers[0], store.TopicSubmitted, 3); err != nil {
			slog.Error("kafka topic", "err", err)
			os.Exit(1)
		}
	}

	errc := make(chan error, 3)
	if *mode == "all" || *mode == "relay" {
		w := &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
		defer w.Close()
		go func() { errc <- pipeline.Relay(ctx, st, w, 500*time.Millisecond) }()
	}
	if *mode == "all" || *mode == "worker" {
		r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "l10n-checker", Topic: store.TopicSubmitted})
		defer r.Close()
		go func() { errc <- pipeline.Work(ctx, st, r) }()
	}
	if *mode == "all" || *mode == "api" {
		srv := &http.Server{Addr: *addr, Handler: routes(st), ReadHeaderTimeout: 5 * time.Second}
		go func() { errc <- srv.ListenAndServe() }()
		go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
		slog.Info("listening", "addr", *addr)
	}

	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("stopped", "err", err)
			os.Exit(1)
		}
	}
}

func routes(st *store.Store) http.Handler {
	mux := http.NewServeMux()

	// PUT /keys/{key}  {"source": "...", "max_length": 40}
	mux.HandleFunc("PUT /keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Source    string `json:"source"`
			MaxLength int    `json:"max_length"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Source == "" {
			http.Error(w, `{"error":"source is required"}`, http.StatusBadRequest)
			return
		}
		if err := st.PutKey(r.Context(), r.PathValue("key"), body.Source, body.MaxLength); err != nil {
			http.Error(w, `{"error":"store failed"}`, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// PUT /keys/{key}/translations/{locale}  {"text": "..."}  -> 202, checked asynchronously
	mux.HandleFunc("PUT /keys/{key}/translations/{locale}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		v, err := st.SubmitTranslation(r.Context(), r.PathValue("key"), r.PathValue("locale"), body.Text)
		switch {
		case errors.Is(err, store.ErrUnknownKey):
			http.Error(w, `{"error":"unknown key"}`, http.StatusNotFound)
		case err != nil:
			http.Error(w, `{"error":"store failed"}`, http.StatusInternalServerError)
		default:
			writeJSON(w, http.StatusAccepted, map[string]any{"version": v, "status": "pending"})
		}
	})

	// GET /keys/{key}/translations/{locale}
	mux.HandleFunc("GET /keys/{key}/translations/{locale}", func(w http.ResponseWriter, r *http.Request) {
		t, err := st.GetTranslation(r.Context(), r.PathValue("key"), r.PathValue("locale"))
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, `{"error":"store failed"}`, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, t)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
