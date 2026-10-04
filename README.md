# investment-engine

[![CI](https://github.com/GabrielHKGodinho/investment-engine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GabrielHKGodinho/investment-engine/actions/workflows/ci.yml)

Order execution engine in Go. A REST API accepts orders and publishes an event
to RabbitMQ; a consumer executes each order against a price from an internal
gRPC service and records the result in PostgreSQL. Locally, all three services export traces to Jaeger; tracing is opt-in and
stays off in the live deployment.

## Live demo

The system runs at **https://api-production-594c.up.railway.app**, on a
zero-cost setup (see ADR-012 in [DECISIONS.md](DECISIONS.md)):

```bash
curl -X POST https://api-production-594c.up.railway.app/orders \
  -H "Content-Type: application/json" \
  -d '{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}'

curl https://api-production-594c.up.railway.app/orders
```

The database sleeps after a few idle minutes, so the first request after a
pause can take a few seconds. Orders are executed asynchronously: list them
again after a moment to see `PENDING` become `EXECUTED`. This is a shared
public demo with no authentication yet.

### How it is deployed

- **Pipeline:** every merge to `main` runs CI (tidy, gofmt, vet, build and
  race-enabled tests), publishes one image per service to GHCR tagged with
  the commit SHA and `latest`, then redeploys the services on Railway.
  `main` only accepts pull requests that pass CI.
- **Topology:** only the API is public, behind HTTPS at Railway's edge. The
  consumer and the price service are reachable only on Railway's private
  network. PostgreSQL runs on Neon and RabbitMQ on CloudAMQP, both over TLS.
- **Configuration:** environment variables only: `DATABASE_URL`,
  `RABBITMQ_URL`, `PRICE_SERVICE_ADDR`, `PORT` and, optionally,
  `OTEL_EXPORTER_OTLP_ENDPOINT`.

## Running locally

Requirements: Docker with Compose v2.

```bash
docker compose up -d --build
```

This starts PostgreSQL (schema applied on first start), RabbitMQ, Jaeger, the
price service, the consumer and the API. The API waits for PostgreSQL and
RabbitMQ to be healthy before starting.

| Service              | Address                                      |
|----------------------|----------------------------------------------|
| REST API             | http://localhost:8080                        |
| Jaeger UI            | http://localhost:16686                       |
| RabbitMQ management  | http://localhost:15672 (user/pass: `engine`) |
| Price service (gRPC) | localhost:50051                              |

Create an order and list it:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}'

curl http://localhost:8080/orders
```

Stop everything with `docker compose down` (data is kept), or
`docker compose down -v` to also delete the database and broker data.