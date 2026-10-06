package pipeline

// Integration tests against real PostgreSQL and Kafka. Skipped unless both are configured:
//   PG_URL=postgres://postgres:pw@127.0.0.1:5432/l10n KAFKA_BROKERS=127.0.0.1:9092 go test ./...

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kwanzaX/l10n-guard/internal/store"
	"github.com/segmentio/kafka-go"
)

func setup(t *testing.T) (context.Context, *store.Store, *kafka.Writer, *kafka.Reader, string) {
	pg, brokers := os.Getenv("PG_URL"), os.Getenv("KAFKA_BROKERS")
	if pg == "" || brokers == "" {
		t.Skip("PG_URL and KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	st, err := store.Open(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := EnsureTopic(ctx, strings.Split(brokers, ",")[0], store.TopicSubmitted, 3); err != nil {
		t.Fatal(err)
	}
	run := fmt.Sprintf("t%d", time.Now().UnixNano())
	w := &kafka.Writer{Addr: kafka.TCP(strings.Split(brokers, ",")...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	t.Cleanup(func() { w.Close() })
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(brokers, ","), GroupID: "test-" + run, Topic: store.TopicSubmitted, StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	t.Cleanup(func() { r.Close() })
	return ctx, st, w, r, run
}

// consume handles messages for this run until `want` events were seen, returning their payloads.
func consume(t *testing.T, ctx context.Context, st *store.Store, r *kafka.Reader, run string, want int) [][]byte {
	t.Helper()
	var seen [][]byte
	for len(seen) < want {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("fetch: %v (saw %d of %d)", err, len(seen), want)
		}
		if strings.HasPrefix(string(msg.Key), run) {
			if err := Handle(ctx, st, msg.Value); err != nil {
				t.Fatal(err)
			}
			seen = append(seen, msg.Value)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	return seen
}

func relay(t *testing.T, ctx context.Context, st *store.Store, w *kafka.Writer) {
	t.Helper()
	for {
		n, err := RelayOnce(ctx, st, w)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func status(t *testing.T, ctx context.Context, st *store.Store, key, locale string) (string, int, string) {
	t.Helper()
	tr, err := st.GetTranslation(ctx, key, locale)
	if err != nil {
		t.Fatal(err)
	}
	return tr.Status, tr.Version, string(tr.Findings)
}

func TestPipeline(t *testing.T) {
	ctx, st, w, r, run := setup(t)
	key := run + ".checkout.pay"
	if err := st.PutKey(ctx, key, "Pay {amount} now", 40); err != nil {
		t.Fatal(err)
	}

	// 1. Happy path and a rejected translation, through outbox -> Kafka -> worker.
	if _, err := st.SubmitTranslation(ctx, key, "pt-PT", "Pague {amount} agora"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SubmitTranslation(ctx, key, "de-DE", "Jetzt bezahlen"); err != nil {
		t.Fatal(err)
	}
	relay(t, ctx, st, w)
	events := consume(t, ctx, st, r, run, 2)

	if s, _, _ := status(t, ctx, st, key, "pt-PT"); s != "ok" {
		t.Fatalf("pt-PT status = %s, want ok", s)
	}
	s, _, f := status(t, ctx, st, key, "de-DE")
	if s != "rejected" || !strings.Contains(f, "missing placeholders: {amount}") {
		t.Fatalf("de-DE = %s %s, want rejected for the missing placeholder", s, f)
	}

	// 2. A duplicate delivery is a no-op.
	var ev store.Submitted
	_ = json.Unmarshal(events[0], &ev)
	applied, err := st.ApplyCheck(ctx, ev, nil) // fn is never called for a processed event
	if err != nil || applied {
		t.Fatalf("duplicate applied=%v err=%v, want skipped", applied, err)
	}

	// 3. A stale event (older version) never overwrites the result for a newer text.
	if _, err := st.SubmitTranslation(ctx, key, "de-DE", "Jetzt {amount} bezahlen"); err != nil {
		t.Fatal(err)
	}
	stale := store.Submitted{EventID: "replayed-v1-" + run, Key: key, Locale: "de-DE", Version: 1}
	if applied, err := st.ApplyCheck(ctx, stale, nil); err != nil || applied {
		t.Fatalf("stale applied=%v err=%v, want skipped", applied, err)
	}
	if s, v, _ := status(t, ctx, st, key, "de-DE"); s != "pending" || v != 2 {
		t.Fatalf("after stale event: %s v%d, want pending v2", s, v)
	}
	relay(t, ctx, st, w)
	consume(t, ctx, st, r, run, 1)
	if s, v, _ := status(t, ctx, st, key, "de-DE"); s != "ok" || v != 2 {
		t.Fatalf("de-DE v2 = %s v%d, want ok v2", s, v)
	}
}

func TestOutboxKeepsEventsWhenKafkaFails(t *testing.T) {
	ctx, st, w, r, run := setup(t)
	key := run + ".receipt.total"
	if err := st.PutKey(ctx, key, "Total: {total}", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SubmitTranslation(ctx, key, "fr-FR", "Total : {total}"); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("broker unavailable")
	if _, err := st.PublishOutbox(ctx, 100, func([]store.OutboxRow) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("publish error = %v, want %v", err, boom)
	}
	// Nothing was marked published, so the next relay delivers it.
	relay(t, ctx, st, w)
	consume(t, ctx, st, r, run, 1)
	if s, _, _ := status(t, ctx, st, key, "fr-FR"); s != "ok" {
		t.Fatalf("fr-FR status = %s, want ok", s)
	}
}
