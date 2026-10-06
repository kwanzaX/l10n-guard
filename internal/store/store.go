// Package store keeps keys, translations and check results in PostgreSQL.
//
// Writes that must reach Kafka go through a transactional outbox: the translation row and
// its event are committed together, so an event can never be lost (or invented) if the
// process dies between the database write and the Kafka publish.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kwanzaX/l10n-guard/internal/check"
)

const schema = `
CREATE TABLE IF NOT EXISTS l10n_keys (
  key         TEXT PRIMARY KEY,
  source      TEXT NOT NULL,
  max_length  INT  NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS l10n_translations (
  key        TEXT NOT NULL REFERENCES l10n_keys(key),
  locale     TEXT NOT NULL,
  text       TEXT NOT NULL,
  version    INT  NOT NULL,
  status     TEXT NOT NULL DEFAULT 'pending',   -- pending | ok | warned | rejected
  findings   JSONB,
  checked_version INT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (key, locale)
);
CREATE TABLE IF NOT EXISTS l10n_outbox (
  id           BIGSERIAL PRIMARY KEY,
  topic        TEXT  NOT NULL,
  msg_key      TEXT  NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS l10n_outbox_unpublished ON l10n_outbox (id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS l10n_processed_events (
  event_id     TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

const TopicSubmitted = "l10n.translation.submitted"

// Submitted is the event emitted when a translation is written.
type Submitted struct {
	EventID string `json:"event_id"`
	Key     string `json:"key"`
	Locale  string `json:"locale"`
	Version int    `json:"version"`
}

type Store struct{ DB *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(ctx, schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{DB: db}, nil
}

func (s *Store) Close() { s.DB.Close() }

var ErrUnknownKey = errors.New("unknown key")

func (s *Store) PutKey(ctx context.Context, key, source string, maxLength int) error {
	_, err := s.DB.Exec(ctx, `
		INSERT INTO l10n_keys (key, source, max_length) VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET source = EXCLUDED.source, max_length = EXCLUDED.max_length`,
		key, source, maxLength)
	return err
}

// SubmitTranslation stores a new version of a translation and its outbox event in one
// transaction. It returns the new version.
func (s *Store) SubmitTranslation(ctx context.Context, key, locale, text string) (int, error) {
	var version int
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM l10n_keys WHERE key = $1)`, key).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrUnknownKey
		}
		err := tx.QueryRow(ctx, `
			INSERT INTO l10n_translations (key, locale, text, version) VALUES ($1, $2, $3, 1)
			ON CONFLICT (key, locale) DO UPDATE
			  SET text = EXCLUDED.text, version = l10n_translations.version + 1,
			      status = 'pending', findings = NULL, updated_at = now()
			RETURNING version`, key, locale, text).Scan(&version)
		if err != nil {
			return err
		}
		ev := Submitted{EventID: eventID(key, locale, version), Key: key, Locale: locale, Version: version}
		payload, _ := json.Marshal(ev)
		_, err = tx.Exec(ctx, `INSERT INTO l10n_outbox (topic, msg_key, payload) VALUES ($1, $2, $3)`,
			TopicSubmitted, key+"/"+locale, payload)
		return err
	})
	return version, err
}

func eventID(key, locale string, version int) string {
	return key + "/" + locale + "@" + itoa(version)
}

// OutboxRow is an event waiting to be published.
type OutboxRow struct {
	ID      int64
	Topic   string
	MsgKey  string
	Payload []byte
}

// PublishOutbox locks a batch of unpublished events, hands them to publish, and marks
// them published only if publish succeeds. SKIP LOCKED lets several relays run safely.
// If the process dies after publish but before commit, the batch is sent again: Kafka
// delivery is at-least-once, and the worker is idempotent on event_id.
func (s *Store) PublishOutbox(ctx context.Context, limit int, publish func([]OutboxRow) error) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, topic, msg_key, payload FROM l10n_outbox
			WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (OutboxRow, error) {
			var o OutboxRow
			err := r.Scan(&o.ID, &o.Topic, &o.MsgKey, &o.Payload)
			return o, err
		})
		if err != nil || len(batch) == 0 {
			return err
		}
		if err := publish(batch); err != nil {
			return err
		}
		ids := make([]int64, len(batch))
		for i, o := range batch {
			ids[i] = o.ID
		}
		n = len(batch)
		_, err = tx.Exec(ctx, `UPDATE l10n_outbox SET published_at = now() WHERE id = ANY($1)`, ids)
		return err
	})
	return n, err
}

// Pending loads what a check needs for one submitted event.
type Pending struct {
	Source, Text string
	Version      int
	MaxLength    int
}

// ApplyCheck runs fn for the event and stores its result exactly once.
// Duplicate deliveries hit the processed_events primary key and are skipped; results for
// an old version are dropped because a newer text has already replaced it.
func (s *Store) ApplyCheck(ctx context.Context, ev Submitted, fn func(Pending) ([]check.Finding, check.Status)) (applied bool, err error) {
	err = pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO l10n_processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, ev.EventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already processed
		}
		var p Pending
		err = tx.QueryRow(ctx, `
			SELECT k.source, t.text, t.version, k.max_length
			FROM l10n_translations t JOIN l10n_keys k USING (key)
			WHERE t.key = $1 AND t.locale = $2 FOR UPDATE OF t`, ev.Key, ev.Locale).Scan(&p.Source, &p.Text, &p.Version, &p.MaxLength)
		if err != nil {
			return err
		}
		if p.Version != ev.Version {
			return nil // stale event: a newer version is already queued
		}
		findings, status := fn(p)
		fj, _ := json.Marshal(findings)
		_, err = tx.Exec(ctx, `
			UPDATE l10n_translations SET status = $3, findings = $4, checked_version = $5, updated_at = now()
			WHERE key = $1 AND locale = $2`, ev.Key, ev.Locale, string(status), fj, ev.Version)
		applied = err == nil
		return err
	})
	return applied, err
}

// Translation is the read model served by the API.
type Translation struct {
	Key       string          `json:"key"`
	Locale    string          `json:"locale"`
	Text      string          `json:"text"`
	Version   int             `json:"version"`
	Status    string          `json:"status"`
	Findings  json.RawMessage `json:"findings,omitempty"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func (s *Store) GetTranslation(ctx context.Context, key, locale string) (Translation, error) {
	var t Translation
	var findings []byte
	err := s.DB.QueryRow(ctx, `
		SELECT key, locale, text, version, status, findings, updated_at
		FROM l10n_translations WHERE key = $1 AND locale = $2`, key, locale).
		Scan(&t.Key, &t.Locale, &t.Text, &t.Version, &t.Status, &findings, &t.UpdatedAt)
	t.Findings = findings
	return t, err
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
