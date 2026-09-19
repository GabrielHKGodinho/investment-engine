# ADR-001: Use RabbitMQ for asynchronous order execution

## Status
Accepted

## Context
When an order is created, it needs to be executed asynchronously: the API
must respond to the client immediately after persisting the order, without
waiting for execution to complete. Execution involves fetching a current
price (via the PriceService) and updating the order/portfolio state.

This requires a message broker between the Order API (producer) and the
Order Consumer (single consumer processing execution). The workload is
task-oriented: each order execution must happen exactly once, with retry
on failure and a dead-letter path for messages that fail repeatedly. There
is currently no requirement for multiple independent services reacting to
the same event, nor for replaying historical events.

## Decision
We will use RabbitMQ as the message broker between the Order API and the
Order Consumer.

## Consequences
- Gained: built-in retry semantics, automatic dead-lettering for messages
  that fail repeatedly, and a simple push-based delivery model that fits
  our single-consumer task-processing use case. Fan-out to additional
  consumers (e.g. a future analytics service reacting to the same
  "order created" event) is achievable via a fanout/topic exchange if
  needed later.
- Accepted trade-off: messages are deleted once acknowledged, so there is
  no way to replay historical events. If the consumer processes a message
  incorrectly (e.g. a bug in the execution logic), the original event
  cannot be recovered — we are limited to whatever state was already
  written to PostgreSQL. A consumer added in the future will only see
  messages published after it starts listening; it cannot retroactively
  process events from before it existed. If event replay or historical
  reprocessing becomes a real requirement, this decision should be
  revisited in favor of a log-based broker such as Kafka.

# ADR-002: Use gRPC for internal PriceService communication

## Status
Accepted

## Context
Before an order can be executed, the Order Consumer must fetch the
current price for the asset from the PriceService. This call happens on
every order execution, so as volume grows the system needs to sustain
many order executions per minute without price lookups becoming a
bottleneck. The PriceService is called exclusively by internal
components (the Order Consumer) — no external client or browser ever
calls it directly.

## Decision
We will expose the PriceService over gRPC (Protocol Buffers), rather
than as a REST/JSON endpoint.

## Consequences
- Gained: a typed contract enforced by the .proto file (compile-time
  safety instead of runtime JSON parsing errors), lower serialization
  overhead than JSON at high call volume, and HTTP/2 multiplexing over a
  single connection.
- Accepted trade-off: gRPC is harder to inspect and debug ad hoc than
  REST — plain curl doesn't work, and even a purpose-built tool like
  grpcurl required troubleshooting due to shell-specific quoting issues
  (encountered firsthand on 2026-09-11). If the PriceService ever needs
  to be called by an external client or a browser, it would require an
  additional REST gateway layer rather than being consumed directly.

# ADR-003: Simulate order execution against an external price, no real order book

## Status
Accepted

## Context
A complete order book requires implementing a real-time matching engine
for buy and sell orders, along with continuous management of order
depth and price-time priority queues. This demands significant
development time on the algorithmic complexity of a trading engine
itself. The primary goal of this project is to master event-driven
messaging patterns, resilience, observability, and distributed
infrastructure — not to build a trading engine.

## Decision
Order execution will be simulated against a price fetched from the
PriceService (mocked internally or sourced from a public API), rather
than matched against a real order book of competing buy/sell orders.

## Consequences
- Gained: development time stays focused on the messaging, resilience,
  and observability patterns this project is meant to teach, rather
  than on matching-engine algorithms unrelated to those goals.
- Accepted trade-off: the project does not demonstrate trading-system-
  specific skills — matching algorithms, price-time priority, partial
  fills, or order book depth management. Anyone reading the code without
  this ADR could mistake the simplification for a knowledge gap rather
  than a deliberate scope decision; this should be called out explicitly
  in the README and in interviews when discussing the project.

# ADR-004: Use PostgreSQL as the primary datastore

## Status
Accepted

## Context
The system needs to persist orders and, after execution, update portfolio
state (positions and balance) consistently — an order's status change and
its corresponding portfolio update must succeed or fail together. The API
also needs to filter and paginate orders by multiple fields at once
(status, side, execution type, asset, date range), as implemented in
ListOrders. There is also existing hands-on operational experience with
PostgreSQL from prior production work, including a database migration
project.

## Decision
We will use PostgreSQL as the primary datastore for orders, executions,
and portfolio state.

## Consequences
- Gained: native transactional guarantees (ACID) for keeping order status
  and portfolio updates consistent, flexible ad-hoc filtering via SQL
  without needing to pre-define query patterns, and directly applicable
  prior operational experience.
- Accepted trade-off: a relational schema requires more upfront design
  (migrations, foreign keys) than a schemaless store would, and horizontal
  scaling later would require more deliberate work (sharding, read
  replicas) than a natively distributed database would offer out of the
  box.

## ADR-005: Direct Exchange for Order Lifecycle Events

**Status:** Accepted

**Context:**

The Order API needs to notify the Order Consumer asynchronously when an
order is created, so the consumer can process execution without blocking
the HTTP request. RabbitMQ was already chosen as the message broker
(ADR-001). The remaining decision is which exchange type to use for
routing order events from the API to the consumer(s).

At this stage, the system has exactly one event type (`order created`)
and exactly one consumer type (Order Consumer). The event set is flat —
there is no hierarchical structure to the event names yet (e.g. no
region or asset-class dimension to route on).

Three exchange types were considered:

- **Fanout**: delivers a copy of every message to every bound queue,
  ignoring the routing key entirely. This would force any future
  consumer (e.g. an audit service) to receive *all* event types and
  filter them in application code, even if it only cares about one —
  pushing filtering work from the broker into every consumer.
- **Topic**: allows pattern-based routing over a dot-separated routing
  key (e.g. `order.br.created`) using wildcards. This is strictly more
  powerful than `direct`, but the extra power is unused today — there
  is no hierarchical dimension in the current event names to justify
  wildcard matching. Introducing it now is speculative complexity.
- **Direct**: routes a message to a queue only if the queue's binding
  key exactly matches the message's routing key. Multiple queues *can*
  bind to the same routing key — this is not a 1:1 relationship — so
  a future second consumer can subscribe to `order.created` by adding
  a new binding, without changing the exchange type.

**Decision:**

Use a `direct` exchange named `order_events`. The order-created event is
published with routing key `order.created`. The Order Consumer's queue
binds to that routing key on `order_events`.

Future order lifecycle events (e.g. `order.cancelled`) will be published
to the same exchange under their own routing key, letting each consumer
bind only to the event types it actually needs to handle.

**Consequences:**

- Adding a new consumer for an existing event type (e.g. an audit
  service reacting to `order.created`) requires only a new queue +
  binding — no change to the exchange, the publisher, or existing
  consumers.
- Adding a *new* event type (e.g. `order.cancelled`) requires only a
  new routing key on publish and a new binding on the consumer side —
  the exchange itself does not change.
- If the event model later grows a genuine hierarchical dimension
  (e.g. per-region routing) that needs wildcard subscriptions, this
  decision will need to be revisited in favor of a `topic` exchange.
  That migration is non-trivial: it requires redeclaring the exchange
  (RabbitMQ does not allow changing an existing exchange's type) and
  updating every publisher and binding.
- Unlike `fanout`, a message published with a routing key that no
  queue is bound to is silently dropped — there is no catch-all queue
  today. This is an accepted trade-off, not yet mitigated (no
  dead-letter-style safety net for unrouted messages at this stage).