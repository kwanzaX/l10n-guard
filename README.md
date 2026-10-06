# l10n-guard

A Go service that runs automated quality checks on translations before they ship. Translations come in over a REST API, go through PostgreSQL and Kafka, and come out with a status (`ok`, `warned`, `rejected`) and the reasons.

```
PUT /keys/checkout.pay/translations/fr-FR  {"text": "Payer {montant} à {merchant}"}
GET /keys/checkout.pay/translations/fr-FR
{"status":"rejected","findings":[
  {"rule":"placeholders","severity":"error","message":"missing placeholders: {amount}"},
  {"rule":"placeholders","severity":"error","message":"unexpected placeholders: {montant}"}]}
```

## The checks

Pure functions in `internal/check`, so the same rules can run in the worker, in CI or in an editor:

| Rule | Severity | Catches |
|---|---|---|
| `placeholders` | error | `{name}`, `{{name}}`, `%s`, `%1$s`, `%.2f` missing, renamed or added |
| `icu_braces` | error | unbalanced ICU braces (quoted `'{'` ignored) |
| `icu_plural` | error | a `plural`/`selectordinal` without its `other` branch |
| `markup` | error | HTML tags that differ from the source |
| `length` | error | over a per-key character limit (e.g. a button) |
| `empty` | error | blank translations |
| `length_growth` | warning | more than 1.6× the source length |
| `untranslated` | warning | identical to the source (short tokens like `PDF` allowed) |
| `whitespace` | warning | leading/trailing whitespace that differs from the source |

## How data moves

```
HTTP PUT ──► Postgres tx: translation v(n) + outbox row ──► relay ──► Kafka ──► worker ──► Postgres tx: result
```

- **Transactional outbox.** The translation and its event commit in one transaction, so an event is never lost or invented if the process dies between the database and Kafka. The relay locks batches with `FOR UPDATE SKIP LOCKED`, so several relays can run, and marks rows published only after Kafka acknowledges them (`acks=all`).
- **Idempotent worker.** Delivery is at-least-once. Each event id is recorded in `l10n_processed_events` in the same transaction as the result, so a re-delivered message is a no-op. The Kafka offset is committed only after that transaction commits.
- **Ordering and stale events.** Messages are keyed by `key/locale`, so versions of one translation stay on one partition, in order. A result for an old version never overwrites a newer text.
- **Topic created explicitly.** Relying on auto-creation made the first publish fail with `Unknown Topic Or Partition` on a fresh broker; the service now creates the topic and waits for partition leaders before relaying.

## Run

```
PG_URL=postgres://... KAFKA_BROKERS=host:9092 go run ./cmd/l10n-guard           # api + relay + worker
go run ./cmd/l10n-guard -mode worker                                              # or scale each part
```

## Tests

```
go test ./...                                                    # checks only
PG_URL=postgres://... KAFKA_BROKERS=host:9092 go test ./...     # plus integration
```

The integration tests run against real PostgreSQL 16 and Apache Kafka 3.9 (KRaft): the happy path, a rejected translation, a duplicate delivery, a stale event racing a newer version, and a Kafka failure during publish that must leave the outbox untouched. They pass on a freshly created broker.
