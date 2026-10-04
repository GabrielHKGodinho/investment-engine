# investment-engine

[![CI](https://github.com/GabrielHKGodinho/investment-engine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GabrielHKGodinho/investment-engine/actions/workflows/ci.yml)

An event-driven order execution service in Go. A REST API accepts orders and
publishes events to RabbitMQ; a separate consumer executes them asynchronously
against prices from an internal gRPC service and records the result in
PostgreSQL. The focus is on what happens when things fail: at-least-once
delivery with idempotent execution, bounded retries before dead-lettering,
graceful shutdown, and traces that follow one order across HTTP, AMQP and gRPC.

**Live demo:** [list the orders](https://api-production-594c.up.railway.app/orders)
(JSON in the browser) · [OpenAPI spec](openapi.yaml) · [Architecture decisions](DECISIONS.md)

## Live demo

The API runs at **https://api-production-594c.up.railway.app** on a zero-cost
setup ([ADR-012](DECISIONS.md#adr-012-zero-cost-deployment-on-railway-with-free-tier-managed-backing-services)). To create an order (bash: Linux, macOS or Git Bash):

```bash
curl -X POST https://api-production-594c.up.railway.app/orders \
  -H "Content-Type: application/json" \
  -d '{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}'

curl https://api-production-594c.up.railway.app/orders
```

- The price service knows three symbols: `PETR4`, `VALE3` and `ITUB4`.
- Execution is asynchronous: list again after a moment to see `PENDING` become
  `EXECUTED`.
- The database sleeps after 5 idle minutes, so the first request after a pause
  can take a few seconds.
- It is a shared public demo with no authentication: every visitor sees the
  same orders (see [Known limitations](#known-limitations)).

## What this project demonstrates

- **Asynchronous processing with at-least-once delivery:** messages are acked
  only after the database transaction commits, one at a time (prefetch 1).
  `internal/execution/consumer.go`, [ADR-006](DECISIONS.md#adr-006-at-least-once-delivery-for-order-execution-messages).
- **Idempotent execution:** a redelivered event finds the order no longer
  `PENDING` and changes nothing. `ExecuteOrder` in `internal/order/store.go`,
  [ADR-008](DECISIONS.md#adr-008-idempotent-order-execution-through-a-conditional-status-transition).
- **Failure classification, bounded retries and dead-lettering:** permanent
  failures go straight to a dead-letter queue; transient ones are retried
  through a TTL queue first. `internal/execution/failure.go`,
  [ADR-009](DECISIONS.md#adr-009-dead-letter-queue-for-rejected-order-messages), [ADR-013](DECISIONS.md#adr-013-bounded-retries-with-a-fixed-delay-for-transient-failures).
- **Graceful shutdown:** on SIGTERM the API drains in-flight requests and the
  consumer stops between messages. [ADR-007](DECISIONS.md#adr-007-graceful-shutdown-on-sigterm-and-sigint).
- **Distributed tracing across HTTP, AMQP and gRPC:** W3C trace context travels
  in the AMQP message headers, so one order is a single trace across all three
  services. `internal/rabbitmq/tracing.go`; runs locally with Jaeger.
- **Metrics and structured logs:** Prometheus counters and latency histograms in
  every service, JSON logs through `log/slog`.
- **Cursor pagination:** an opaque cursor over `(created_at, id)`, backed by a
  composite index. `internal/pagination`.
- **Containers and CI/CD:** distroless images of 6.7–8.3 MB compressed; CI runs
  race-enabled tests on every push and deploys from `main`.
  [ADR-011](DECISIONS.md#adr-011-multi-stage-docker-build-with-distroless-runtime-images), [ADR-012](DECISIONS.md#adr-012-zero-cost-deployment-on-railway-with-free-tier-managed-backing-services).

## Architecture

```mermaid
flowchart LR
    client([Client])

    subgraph go["Go services"]
        api["api<br/>REST, net/http"]
        consumer["consumer<br/>order execution"]
        price["priceservice<br/>gRPC"]
    end

    subgraph mq["RabbitMQ"]
        ex{{"order_events<br/>direct exchange"}}
        q["order_execution<br/>queue"]
        retry["order_execution.retry<br/>queue, TTL 5s"]
        dlx{{"order_execution.dlx"}}
        dlq["order_execution.dlq<br/>queue"]
    end

    db[("PostgreSQL<br/>orders, executions")]

    client -- "POST / GET /orders" --> api
    api -- "INSERT order (PENDING)<br/>SELECT orders" --> db
    api -- "publish order.created" --> ex
    ex -- "order.created" --> q
    q -- "deliver (manual ack, prefetch 1)" --> consumer
    consumer -- "GetQuote" --> price
    consumer -- "PENDING to EXECUTED + execution row<br/>in one transaction" --> db
    consumer -. "transient failure:<br/>republish with x-retry-count + 1" .-> retry
    retry -. "TTL expires:<br/>dead-lettered back by queue name" .-> q
    q -. "rejected: permanent failure<br/>or 3rd failed attempt" .-> dlx
    dlx -.-> dlq
```

Solid arrows are the normal path; dotted arrows are failure handling.

### Life of an order

1. `POST /orders`: the API validates the body, takes the user from the
   (simulated) auth context, normalizes the asset symbol and inserts the order
   as `PENDING`. It then publishes `order.created` to the `order_events`
   exchange and answers `201` with the order ID: execution has not happened yet.
2. RabbitMQ routes the event to the durable `order_execution` queue.
3. The consumer takes one message at a time (prefetch 1, manual ack), decodes
   and validates the event, and asks the price service for a quote over gRPC.
4. In a single transaction it moves the order from `PENDING` to `EXECUTED`,
   only if it is still `PENDING`, and inserts the execution row with the price.
   The message is acked only after the commit.
5. A duplicate delivery finds the order no longer `PENDING`, changes nothing
   and is acked ([ADR-008](DECISIONS.md#adr-008-idempotent-order-execution-through-a-conditional-status-transition)).
6. A transient failure, such as the price service being unreachable, is
   republished to `order_execution.retry`, waits 5 s and returns to the queue.
   After the third failed attempt, or immediately for a permanent failure (an
   invalid event or an unknown symbol), the message is rejected to the
   dead-letter queue.
7. `GET /orders` shows the new status. The execution price is stored but not
   exposed by the API yet.

## Design decisions & trade-offs

Each decision has its full context and consequences in [DECISIONS.md](DECISIONS.md).

| Decision | What it bought | What it cost | ADR |
|---|---|---|---|
| RabbitMQ instead of Kafka | Per-message acks, native dead-lettering and exchange-based routing; push delivery with prefetch fits a single task-processing consumer | No replay: a message is gone once acked, so a consumer bug can't be fixed by reprocessing past events, and a new consumer only sees new ones | [ADR-001](DECISIONS.md#adr-001-use-rabbitmq-for-asynchronous-order-execution) |
| gRPC for the internal price service | A typed contract checked at compile time and a generated client, instead of hand-parsed JSON | Not callable with curl or from a browser; ad hoc debugging needs grpcurl; generated code lives in the repository | [ADR-002](DECISIONS.md#adr-002-use-grpc-for-internal-priceservice-communication) |
| Simulated execution, no order book | Time spent on messaging, failure handling and observability, which is what this project is about | It shows no trading-specific skills: no matching, price-time priority or partial fills | [ADR-003](DECISIONS.md#adr-003-simulate-order-execution-against-an-external-price-no-real-order-book) |
| At-least-once delivery with an idempotent status transition | A consumer that crashes before acking loses nothing: the message is redelivered | Every redelivery must be harmless; the guarantee lives in a conditional `UPDATE ... WHERE status = 'PENDING'` that any new handler has to respect. It does not stop a client from creating two orders by retrying the `POST` | [ADR-006](DECISIONS.md#adr-006-at-least-once-delivery-for-order-execution-messages), [ADR-008](DECISIONS.md#adr-008-idempotent-order-execution-through-a-conditional-status-transition) |
| Fixed 5 s retry delay, 3 attempts | One retry queue with a queue-level TTL: no plugin, no queue per attempt | A tolerance window of about 10 s: a dependency down for longer sends orders to the dead-letter queue | [ADR-013](DECISIONS.md#adr-013-bounded-retries-with-a-fixed-delay-for-transient-failures) |
| Cursor pagination instead of offset | Pages stay consistent while orders are inserted (no skipped or repeated rows), and with the composite index a page's cost doesn't grow with its depth | No jumping to page N and no total count; clients must treat the cursor as opaque | — |
| Distroless runtime images | Runtime images of 6.7–8.3 MB compressed, non-root, with no shell or package manager to exploit | No shell to `docker exec` into: debugging relies on logs, metrics and traces | [ADR-011](DECISIONS.md#adr-011-multi-stage-docker-build-with-distroless-runtime-images) |
| Zero-cost deployment across three free tiers | A public demo at $0/month, and evidence that backing services move between providers by configuration alone | Three providers with public TLS hops between them, a database that sleeps after 5 minutes idle, free-tier quotas, and no hosted trace collector | [ADR-012](DECISIONS.md#adr-012-zero-cost-deployment-on-railway-with-free-tier-managed-backing-services) |

## Known limitations

- **Orders can stay `PENDING` forever.** There is no terminal failure state and
  no reconciliation, so every path that fails to execute ends there: an unknown
  symbol, a dependency down for longer than the retry window, a publish that
  fails after the insert ([ADR-007](DECISIONS.md#adr-007-graceful-shutdown-on-sigterm-and-sigint)), an unroutable message (only
  logged), an invalid event, or a message lost by the broker before it was
  persisted (publisher confirms are not enabled).
- **LIMIT orders ignore `limitPrice`.** The limit is validated and stored, but
  the consumer executes at the quoted price even when that price is worse than
  the limit.
- **No authentication or rate limiting.** Every request acts as the same
  simulated user, so all visitors see the same orders, and nothing bounds
  request volume: abuse could exhaust the free-tier quotas and take the demo
  down.
- **Prices are `float64` / `DOUBLE PRECISION`.** Binary floating point can't
  represent most decimal prices exactly. Nothing sums prices yet, so no error
  accumulates today, but balances or PnL would need integer cents or `NUMERIC`
  first. Separately, simulated quotes are not rounded to the R$ 0.01 tick, so
  executions store prices no exchange would print.
- **The API only creates and lists orders.** Cancel, get-by-id, portfolio and
  PnL were planned but not built; `CANCELLED` and `REJECTED` exist in the schema
  and in the OpenAPI enum but are never reached. The execution price is stored
  but visible only in the database.
- **No AMQP reconnection.** When the broker connection drops, the API and the
  consumer exit and the platform restarts them (crash-only). Requests in flight
  fail, and Railway's Free plan allows at most 10 restarts, so a broker that
  keeps dropping connections leaves a service down until a manual redeploy.
- **The price service has no graceful shutdown.** On SIGTERM, which every deploy
  sends, it exits with code 2 without running deferred cleanup: in-flight calls
  are cut (the consumer retries them as transient failures), buffered spans are
  lost, and every stop looks like a crash.
- **Tracing is off in the live demo.** The tracing code ships in every image,
  but no collector is hosted within the $1/month budget; traces are visible
  only when running locally.

## Running locally

Requirements: Docker with Compose v2.

```bash
docker compose up -d --build
```

This starts PostgreSQL (schema applied on first start), RabbitMQ, Jaeger, the
price service, the consumer and the API. The API and the consumer wait for
PostgreSQL and RabbitMQ to be healthy before starting.

| Service              | Address                                      |
|----------------------|----------------------------------------------|
| REST API             | http://localhost:8080                        |
| Jaeger UI            | http://localhost:16686                       |
| RabbitMQ management  | http://localhost:15672 (user/pass: `engine`) |
| Price service (gRPC) | localhost:50051                              |

Create an order and list it (bash):

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}'

curl http://localhost:8080/orders
```

Stop everything with `docker compose down` (data is kept), or
`docker compose down -v` to also delete the database and broker data.

## Testing

Requires Go 1.25.

```bash
go test -race ./...
```

Unit tests need no running infrastructure: handlers are tested with
`httptest` and fakes, and the PostgreSQL store with `go-sqlmock`. One
integration test, `TestPublishOrderCreated_Unroutable`, needs a real RabbitMQ
and **is skipped silently** unless `RABBITMQ_URL` is set. With the compose
stack running:

```bash
RABBITMQ_URL=amqp://engine:engine@localhost:5672/ go test -race -v -run Unroutable ./internal/order/
```

CI runs `go mod tidy -diff`, `gofmt`, `go vet`, `go build` and
`go test -race -count=1 ./...` on every push. The retry policy was verified by
hand (see [ADR-013](DECISIONS.md#adr-013-bounded-retries-with-a-fixed-delay-for-transient-failures)); it has no automated test yet.

## Observability

- **Traces:** with the compose stack, open the Jaeger UI at
  http://localhost:16686 and search the `api` service. A successful order is
  one trace of five spans across three services: `POST /orders`, the publish,
  the consumer's processing, and the `GetQuote` client and server spans.
  Tracing is opt-in: a service exports spans only when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- **Metrics:** each service serves Prometheus metrics on an internal port
  (API `:9100`, consumer `:9101`, price service `:9102`). They are not
  published to the host, and no Prometheus server is included.
- **Logs:** JSON lines through `log/slog`, with a `service` attribute; the
  level comes from `LOG_LEVEL` (default `info`).

## Deployment

- **Pipeline:** every merge to `main` runs CI, publishes one image per service
  to GHCR tagged with the commit SHA and `latest`, then redeploys the services
  on Railway. `main` only accepts pull requests that pass CI.
- **Topology:** only the API is public, behind HTTPS at Railway's edge. The
  consumer and the price service are reachable only on Railway's private
  network. PostgreSQL runs on Neon and RabbitMQ on CloudAMQP, both over TLS.
- **Configuration:** environment variables only: `DATABASE_URL`,
  `RABBITMQ_URL`, `PRICE_SERVICE_ADDR`, `PORT` and, optionally,
  `OTEL_EXPORTER_OTLP_ENDPOINT`.

## Code map

- `cmd/api`, `cmd/consumer`, `cmd/priceservice`: the three binaries. Each
  `main` only wires configuration, telemetry and shutdown.
- `internal/order`: the HTTP handlers, the PostgreSQL store (`ExecuteOrder`
  holds the idempotent transition), the `order.created` event and its
  publisher.
- `internal/execution`: the consumer loop with the ack, retry and reject
  decisions, the queue topology, failure classification, and the handler that
  executes one order.
- `internal/priceservice` and `proto/price`: the gRPC server, client and
  contract.
- `internal/rabbitmq`: generic AMQP helpers and the trace-context carrier.
- `internal/pagination`: cursor encoding and decoding.
- `internal/telemetry`, `internal/metrics`, `internal/logging`: tracing,
  metrics and logging setup shared by all binaries.
- `internal/postgres`, `internal/apierror`, `internal/auth`: database
  connection, the JSON error envelope, and the simulated user.

Two boundaries are kept on purpose: `internal/rabbitmq` knows nothing about
orders, and the execution handler knows nothing about AMQP, so it can be
tested without a broker.

## What I'd do differently

**From the start**

- **Design the failure states before the happy path.** Most limitations above
  trace back to building `PENDING → EXECUTED` first and adding failure handling
  afterwards: a failed order never got a place to land.
- **Change the documentation in the same pull request as the code.** The
  OpenAPI description and one ADR fell behind the code until a review caught
  them.

**Next, in order**

1. **Versioned migrations** (goose or golang-migrate). Everything below changes
   the schema, and `schema.sql` only runs against an empty database.
2. **A terminal state for every order.** The consumer would write `REJECTED` or
   `FAILED` on a permanent failure or exhausted retries, including a LIMIT order
   whose limit isn't met, and a transactional outbox with publisher confirms
   would replace the direct publish. This closes the largest limitation.
3. **Integer cents and tick rounding** in the price service, before any balance
   or PnL arithmetic. The price already crosses five boundaries (JSON, the
   event, protobuf, Go signatures, SQL), and each new one makes the change more
   expensive.
4. **Authentication and rate limiting**, so the public demo stops sharing one
   user and can't be used to exhaust its quotas.
5. **Exponential backoff, a dead-letter replay tool and an alert on dead-letter
   depth.** Today the retry window is about 10 s, and a message in the
   dead-letter queue is a dead end nobody is told about.
6. **Cancel, get-by-id, executions and portfolio endpoints**, the features the
   domain was chosen for.
