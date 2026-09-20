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

# ADR-005: Direct Exchange for Order Lifecycle Events

## Status
Accepted

## Context

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

## Decision

Use a `direct` exchange named `order_events`. The order-created event is
published with routing key `order.created`. The Order Consumer's queue
binds to that routing key on `order_events`.

Future order lifecycle events (e.g. `order.cancelled`) will be published
to the same exchange under their own routing key, letting each consumer
bind only to the event types it actually needs to handle.

## Consequences

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

# ADR-006: At-least-once delivery for order execution messages

## Status
Accepted

## Context
ADR-001 requires that each order is executed exactly once. A broker and a
database cannot be updated atomically, so exactly-once delivery is not
achievable end to end: a consumer that dies between executing an order and
telling the broker either loses the message or causes a duplicate.

The consumer has to choose which failure it accepts:

- **Ack before processing (at-most-once):** a crash mid-processing loses the
  order silently. Nothing records that it was ever attempted.
- **Ack after processing (at-least-once):** a crash between executing and
  acking makes RabbitMQ redeliver the message, so an order can be executed
  twice. A duplicate can be detected and neutralized; a lost message leaves
  no trace to detect.

Messages that can never succeed (malformed JSON, invalid event) must also
not be requeued forever.

## Decision
- Manual acknowledgements: the consumer acks only after the handler finishes
  successfully (at-least-once delivery).
- Prefetch of 1: RabbitMQ keeps one unacknowledged delivery per consumer.
  Processing is sequential, so a larger window would only pile deliveries up
  in memory and increase how many messages are redelivered after a crash.
- Any handler error rejects the delivery without requeue (`Reject(false)`),
  so a poison message cannot loop forever. Until the dead-letter queue
  exists, rejected messages are dropped.
- The API and the consumer both declare the topology they use (declaring an
  identical exchange or queue is a no-op), so the start order of the
  processes does not matter.
- Duplicates are handled by making the handler idempotent, not by trying to
  prevent redelivery.

## Consequences
- Gained: no message is lost when the consumer crashes, is redeployed or
  loses its connection; the broker returns unacknowledged messages to the
  queue.
- Cost: the same order can be delivered twice, so executing an order must be
  idempotent. Not implemented yet.
- Known gaps: rejected messages are lost until a dead-letter queue is added;
  transient failures (database or PriceService down) are treated like poison
  messages; a retry policy only makes sense once real execution exists.

# ADR-007: Graceful shutdown on SIGTERM and SIGINT

## Status
Accepted

## Context
Deploys stop a process by sending SIGTERM and, after a grace period,
SIGKILL (10s by default in `docker stop`, 30s in Kubernetes). A Go program
without a signal handler exits immediately on SIGTERM, so:

- the consumer drops the delivery it was processing, and RabbitMQ redelivers
  it (see ADR-006);
- the API cuts requests in flight.

The second case is worse than it looks, because `POST /orders` performs two
writes with no shared transaction: an INSERT and the publish of the
order-created event. A request cut between them leaves a PENDING order with
no event, which nobody will ever execute. This was reproduced by holding a
request open after the INSERT for longer than the shutdown timeout: the
order was persisted and the request never reached the publish call.

## Decision
- Both binaries follow the `run() error` pattern, so deferred cleanup runs
  (`log.Fatalf` skips it), and cancel a context with `signal.NotifyContext`
  on SIGINT and SIGTERM. After the first signal the default signal behavior
  is restored (`context.AfterFunc(ctx, stop)`), so a second signal
  force-kills a stuck shutdown.
- Consumer: the signal context means "stop taking new work", not "abort the
  current work". It is only checked between deliveries, so the delivery in
  progress finishes and is acked. A delivery received after the signal is
  left unacked, and RabbitMQ requeues it when the channel closes. `Run`
  returns nil on a clean shutdown.
- API: `http.Server.Shutdown` with an 8s timeout, on a context derived from
  `context.Background()` (the signal context is already canceled by then).
  It runs in the body of `run()`, before the deferred closes of RabbitMQ and
  PostgreSQL, so requests in flight keep their dependencies until they
  finish. The server also sets `ReadHeaderTimeout`.
- The shutdown timeout stays below the platform grace period (8s against
  Docker's 10s), leaving room for the cleanup that runs after it.

## Consequences
- Gained: a planned stop finishes what is in flight within the timeout, so a
  normal deploy causes no redeliveries and no cut requests; the process exits
  with code 0 on a clean shutdown.
- Not solved: a crash, a SIGKILL after the grace period, or a request that
  outlives the timeout still cuts the flow between the INSERT and the publish
  (the dual-write problem). The usual fix is a transactional outbox: write
  the event to a table in the same transaction as the order and let a
  separate relay publish it. Not implemented; candidate for a future ADR.
- The consumer's in-flight work must fit in the grace period. When real
  execution (PriceService + database) replaces the simulated 5s, the handler
  must not receive the signal context, which would abort the work we want to
  finish. It needs its own context with a timeout.
- The consumer does not call `Channel.Cancel` before exiting. With prefetch 1
  and sequential processing, at most one extra delivery is buffered, and
  closing the channel returns it to the queue. Revisit if deliveries start
  being processed concurrently.

# ADR-008: Idempotent order execution through a conditional status transition

## Status
Accepted

## Context
ADR-006 chose at-least-once delivery, so the same order-created message can
reach the consumer more than once: a crash between execution and ack, a lost
connection before the ack, a SIGKILL after the shutdown grace period, and
later multiple replicas. Executing an order twice must not duplicate its
effect.

There are two usual ways to make a consumer idempotent: a table of processed
message ids, inserted in the same transaction as the effect, or a conditional
state change on the business entity. An order already has a lifecycle
(PENDING, EXECUTED, CANCELLED, REJECTED) and the effect of executing it is a
state transition.

## Decision
- Execution claims the order with a single statement:
  `UPDATE orders SET status = 'EXECUTED' WHERE id = $1 AND status = 'PENDING'`.
  One affected row means this delivery performed the transition; zero means it
  did not. The check and the write are one atomic statement, so two concurrent
  deliveries cannot both win: under PostgreSQL's default Read Committed level
  the second one waits for the row lock and re-evaluates the condition against
  the updated row. A SELECT followed by an UPDATE would not be safe.
- Zero affected rows is disambiguated with an existence check. An order that
  exists but is not PENDING (already executed, cancelled or rejected) is
  skipped and acked. An order that does not exist is an error
  (`ErrOrderNotFound`) and the message is rejected: treating it as a duplicate
  would hide a broken event.
- The claim runs after the read-only work. Claiming first would lose the
  effect if the consumer died right after it: the order would be EXECUTED with
  nothing done, and the redelivery would be skipped.
- The handler receives a context detached from the shutdown signal, with its
  own timeout (see ADR-007).

## Consequences
- Duplicates and orders cancelled while queued are absorbed by the same guard:
  skipped and acked, with no effect.
- When the portfolio update arrives, it must run in the same database
  transaction as the status transition, so both happen or neither does.
  Otherwise the ordering problem above comes back. Effects outside the
  database (email, third-party calls) cannot be made atomic and would need an
  idempotency key at the receiving system or a processed-messages table.
- A client that retries `POST /orders` creates two orders. That needs an
  idempotency key on the API and is not covered here.
- A zero-row update costs one extra query, only on the rare path.

# ADR-009: Dead-letter queue for rejected order messages

## Status
Accepted

## Context
ADR-006 rejects failed deliveries without requeue, so a message that can
never succeed cannot loop forever, and it left those messages dropped until a
dead-letter queue existed. A dropped message leaves no trace: nothing to
inspect and nothing to replay.

## Decision
- `order_execution` is declared with `x-dead-letter-exchange` set to
  `order_execution.dlx` and `x-dead-letter-routing-key` set to
  `order_execution.dlq`. The DLX is a durable direct exchange and the DLQ is a
  durable queue bound to it with that key. The consumer code is unchanged:
  rejecting without requeue triggers the dead-lettering.
- The arguments live in code, because the processes declare their own topology
  (see ADR-005 and ADR-006), not in policies. The RabbitMQ documentation
  recommends policies, since queue arguments cannot change without
  redeploying and deleting the queue. We trade that flexibility for a topology
  that lives in the repository. Revisit it with the docker-compose setup.
- Duplicates and already-resolved orders are acked, not rejected, so they
  never reach the DLQ. It holds only what needs a human.
- Nothing consumes the DLQ. Inspection and replay are manual, through the
  management UI.

## Consequences
- Failed messages are kept, with an `x-death` header that records the reason,
  the original queue and how many times it happened.
- Silent failure mode: if the DLX or its binding were missing, RabbitMQ would
  drop dead-lettered messages without any error. The configuration is only
  trustworthy if a test sends a bad message and finds it in the DLQ. This was
  done by hand on 18/09 and should be automated with the messaging tests.
- Known gap: transient failures (database down, timeouts) are still rejected
  like poison messages and end in the DLQ, although a retry would succeed. A
  retry policy (requeue with a limit and a delay, then dead-letter) is pending
  until real execution exists.
- Changing the queue arguments requires deleting the queue.
- Nothing alerts on a growing DLQ, so it goes unnoticed until someone looks.