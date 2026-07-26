# System Design — SportsPulse

A living technical reference documenting the distributed systems
concepts underpinning SportsPulse's architecture. Each concept is
tied to a concrete decision made in this project, with production
context and tradeoffs explained.

This document grows as each layer is built.

---

## 1. CAP Theorem

In any distributed system, only two of three properties can be
guaranteed simultaneously:

```
C — Consistency
    Every read returns the most recent write.
    All nodes see the same data at the same time.

A — Availability
    Every request receives a response.
    The system never refuses requests.

P — Partition Tolerance
    The system continues operating when network
    messages between nodes are lost or delayed.
```

Network partitions are inevitable in production systems. P is
non-negotiable — the real choice is always between C and A.

### SportsPulse decision: AP (Cassandra)

Match events are write-heavy and append-only. If two Cassandra nodes
temporarily disagree on whether an event was written, they reconcile
within milliseconds. Refusing writes during a network partition would
cause permanent data loss — unacceptable for a real-time pipeline.

Contrast with a financial settlement system where two nodes disagreeing
on a balance is unacceptable — CP (Postgres) is correct there.
Different business requirement, different tradeoff.

---

## 2. Consistency Models

Consistency exists on a spectrum:

```
STRONG CONSISTENCY            EVENTUAL CONSISTENCY
──────────────────────────────────────────────────
Every read sees the most      Reads eventually reflect
recent write immediately.     the most recent write.

Lower throughput              Higher throughput
Higher correctness            Higher availability
```

### Cassandra's tunable consistency

| Level | Behaviour | Appropriate use |
|---|---|---|
| ONE | Read/write from 1 node | High-throughput event ingestion |
| QUORUM | Majority of nodes agree | Important operational reads |
| ALL | All nodes must agree | Critical financial operations |

### SportsPulse decision

Match event writes use `ONE` (default) — maximum throughput, accepts
milliseconds of staleness. A query feeding a pricing engine would
use `QUORUM` — latency tradeoff justified by correctness requirement.

---

## 3. Event Sourcing

State is derived from an immutable sequence of events rather than
storing current state directly.

```
State-based:
players: { id: 1, goals: 3, cards: 1 }
→ Updated on every change, history not preserved

Event sourcing:
match_events:
  { minute: 35, type: Goal, player: Bellingham }
  { minute: 43, type: Goal, player: Saka }
  { minute: 48, type: Card, player: Pouraliganji }
→ State derived by querying events
→ Full history preserved
→ Point-in-time reconstruction possible
```

### SportsPulse implementation

The Kafka topic and Cassandra `match_events` table implement event
sourcing. Every match event is stored as an immutable fact. Player
performance metrics are derived by querying events — never stored
as mutable state.

This enables accurate ML feature engineering: the model receives
exactly the data available at any point during a match, preventing
data leakage from future events.

---

## 4. CQRS — Command Query Responsibility Segregation

Separates the write path (Commands) from the read path (Queries),
optimising each independently.

```
Write path:  API → Kafka → Spark → Cassandra
             Optimised for high-throughput, append-only ingestion

Read path:   Cassandra → FastAPI → Client
             Optimised for low-latency, specific query patterns
```

### SportsPulse implementation

The Cassandra schema is designed around read query patterns:

```sql
PRIMARY KEY (fixture_id, minute, event_id)
```

Partition key (`fixture_id`) groups all events for one match on
the same nodes — efficient for "give me all events for match X."
This is a CQRS decision: schema driven by read requirements, not
by the natural shape of the data.

---

## 5. Idempotency

An operation is idempotent if executing it multiple times produces
the same result as executing it once.

```
Idempotent:     SET goals = 3       → always results in goals = 3
NOT idempotent: INCREMENT goals     → result depends on how many times run
```

### Why this matters in streaming pipelines

A Spark consumer that crashes mid-batch restarts and reprocesses
the last checkpoint's messages. Non-idempotent writes create
duplicates on reprocessing.

### SportsPulse implementation

UUID as part of the primary key makes writes idempotent:

```sql
PRIMARY KEY (fixture_id, minute, event_id)
-- event_id = UUID generated once per event
```

If Spark reprocesses a batch, the second write of the same event
has the same UUID — same primary key — Cassandra upserts to
identical data. Result is the same as a single write.

This achieves exactly-once write semantics without distributed
transactions.

### The silent upsert problem this solved

Without UUID, `PRIMARY KEY (fixture_id, minute)` meant three
substitutions at minute 46 resulted in one row — each write
silently overwrote the previous. UUID as a third key component
ensures each event occupies a unique row.

---

## 6. Partitioning Strategies

How data is distributed across nodes. The partition key determines
which node stores which data.

### Hash partitioning (Cassandra default)
```
hash(partition_key) % num_nodes = target node
→ Even distribution, no hot spots
→ Range queries across partitions require scatter-gather
```

### Composite partitioning (SportsPulse)

```sql
PRIMARY KEY ((sport, league_id), fixture_id, minute, event_id)
```

- **Partition key:** `(sport, league_id)` — distributes load by
  sport and league. Prevents hot spots during peak events by
  spreading traffic across competitions.
- **Clustering key:** `(fixture_id, minute, event_id)` — physically
  orders rows within a partition by match then by time.

A partition key of just `fixture_id` would concentrate all traffic
for a popular match on a single node — hot spot under peak load.

---

## 7. Message Ordering and Delivery Guarantees

### Kafka's ordering guarantee

Order is guaranteed **within a partition only**, not across
partitions. Messages that must be processed in order must share
a partition key.

```
Key = fixture_id → all events for one match → same partition
→ Minute 35 event always processed before minute 62 ✓

No key (round-robin) → events across multiple partitions
→ Minute 62 could arrive before minute 35 ✗
```

SportsPulse uses `fixture_id` as the Kafka message key — per-match
ordering is guaranteed while parallelism across matches is preserved.

### Delivery semantics

| Guarantee | Meaning |
|---|---|
| At-most-once | Messages may be lost, never duplicated |
| At-least-once | Messages never lost, may be duplicated |
| Exactly-once | Never lost, never duplicated |

SportsPulse uses at-least-once delivery combined with idempotent
Cassandra writes to achieve exactly-once semantics at the storage
layer — without the overhead of distributed transactions.

---

## 8. Backpressure

The condition when a consumer processes data slower than the
producer generates it.

```
Producer: 1,000 events/second
Consumer: 500 events/second
→ 500 events/second accumulating in Kafka
→ Consumer lag increasing indefinitely
```

### Kafka's role

Kafka buffers the backlog durably — nothing is lost. The consumer
processes at its own pace and catches up when capacity allows.
This is a core reason for Kafka's existence: decoupling producer
and consumer throughput.

### Spark's backpressure control

```python
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)
```

`maxOffsetsPerTrigger` bounds batch size. Without this, a traffic
spike causes Spark to attempt processing thousands of events in
one batch — exceeding executor memory. With it, batches remain
bounded in size and memory usage is predictable.

### Production considerations

- Monitor consumer lag as a primary operational metric
- Alert when lag exceeds a defined threshold
- Auto-scale executors on sustained lag
- Tune `maxOffsetsPerTrigger` based on observed executor memory usage

---

## 9. Ordering and Exactly-Once Delivery

Two of the hardest problems in distributed systems engineering:

**Guaranteed ordering** — solved by partitioning Kafka messages
by `fixture_id`, ensuring per-match event ordering within a
Kafka partition.

**Exactly-once delivery** — solved by combining at-least-once
Kafka delivery with idempotent UUID-keyed Cassandra writes.

The practical pattern: avoid exactly-once delivery at the transport
layer (expensive, complex) and instead design idempotent consumers
that handle duplicate delivery correctly.

---

## Concepts to be added

| Layer | Concepts |
|---|---|
| Layer 3 — ML | Feature stores, train/serve skew, model versioning, data leakage, online vs offline features |
| Layer 4 — RAG | Vector databases, embedding models, retrieval strategies, semantic vs keyword search |
| Layer 5 — API | REST design, rate limiting, caching, API versioning, WebSocket vs polling |
| Layer 6 — MCP | Tool interfaces, structured data access patterns, agent protocols |
| Layer 7 — A2A | Agent orchestration, multi-agent coordination, failure modes |
| Deployment | Container orchestration, health checks, rolling deployments, observability |

---

## Architecture decisions reference

| Decision | Choice | Rationale |
|---|---|---|
| Event storage | Cassandra | AP — availability over consistency for event data |
| Streaming backbone | Kafka | Durable log, multi-consumer, decoupled throughput |
| Consistency level | ONE | Event data tolerates milliseconds of staleness |
| Partition key | (sport, league_id) | Even distribution, prevents hot spots |
| Primary key includes UUID | Yes | Idempotent writes, exactly-once storage |
| Message key = fixture_id | Yes | Per-match event ordering guaranteed |
| CQRS | Yes | Write and read paths optimised independently |
| maxOffsetsPerTrigger | Planned | Backpressure control during traffic spikes |
