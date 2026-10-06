// Package pipeline moves events between PostgreSQL and Kafka: the relay publishes the
// outbox, the worker consumes submitted translations and records their check results.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/kwanzaX/l10n-guard/internal/check"
	"github.com/kwanzaX/l10n-guard/internal/store"
	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates the topic if it does not exist and waits until the broker serves
// metadata for every partition. Relying on auto-creation makes the first publish fail with
// "Unknown Topic Or Partition" while the broker creates it in the background.
func EnsureTopic(ctx context.Context, broker, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	err = cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1})
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	for {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) > 0 {
			ready := true
			for _, p := range parts {
				if p.Leader.Host == "" {
					ready = false
				}
			}
			if ready {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Relay publishes outbox rows to Kafka until ctx is cancelled.
func Relay(ctx context.Context, st *store.Store, w *kafka.Writer, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := RelayOnce(ctx, st, w); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("relay", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// RelayOnce publishes one batch and returns how many events went out.
func RelayOnce(ctx context.Context, st *store.Store, w *kafka.Writer) (int, error) {
	return st.PublishOutbox(ctx, 100, func(rows []store.OutboxRow) error {
		msgs := make([]kafka.Message, len(rows))
		for i, r := range rows {
			// Keyed by key/locale so every version of one translation lands on the same
			// partition and is consumed in order.
			msgs[i] = kafka.Message{Topic: r.Topic, Key: []byte(r.MsgKey), Value: r.Payload}
		}
		return w.WriteMessages(ctx, msgs...)
	})
}

// Work consumes submitted events and stores check results. The offset is committed only
// after the database transaction commits, so a crash re-delivers instead of losing work;
// ApplyCheck makes the re-delivery harmless.
func Work(ctx context.Context, st *store.Store, r *kafka.Reader) error {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := Handle(ctx, st, msg.Value); err != nil {
			slog.Error("check failed, will retry on restart", "offset", msg.Offset, "err", err)
			return err // stop without committing; the message is re-delivered
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// Handle processes one event payload.
func Handle(ctx context.Context, st *store.Store, payload []byte) error {
	var ev store.Submitted
	if err := json.Unmarshal(payload, &ev); err != nil {
		slog.Warn("skipping malformed event", "err", err)
		return nil // a poison message must not block the partition
	}
	applied, err := st.ApplyCheck(ctx, ev, func(p store.Pending) ([]check.Finding, check.Status) {
		return check.Run(p.Source, p.Text, check.Options{MaxLength: p.MaxLength})
	})
	if err == nil {
		slog.Info("checked", "event", ev.EventID, "applied", applied)
	}
	return err
}
