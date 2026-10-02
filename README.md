# investment-engine

Order execution engine in Go. A REST API accepts orders and publishes an event
to RabbitMQ; a consumer executes each order against a price from an internal
gRPC service and records the result in PostgreSQL. All three services export
traces to Jaeger.

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