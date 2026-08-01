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

## 10. Model Deployment Patterns

### Train → Register → Promote → Serve

The standard pattern for productionising an ML model, regardless of
whether the tooling is self-hosted or managed cloud:

```
Train        → produces a model artifact + evaluation metrics
Register     → artifact + metadata stored in a versioned registry
Promote      → model moves through stages: None → Staging → Production
Serve        → serving layer loads whichever model is tagged Production
```

**Why staged promotion matters:** it decouples "a model was trained"
from "a model is live." A newly trained model sits in Staging until
it passes an evaluation gate — this prevents a regressed model from
silently replacing a working one in production.

### SportsPulse implementation

```
scikit-learn/scipy Poisson model trained on PL historical data
    ↓
mlflow.log_model() — artifact stored in MinIO, experiment logged
    ↓
Model registered in MLflow Model Registry, stage = Staging
    ↓
Evaluation gate: does this model beat the current Production
model on held-out data? (champion/challenger pattern)
    ↓
If yes → mlflow.transition_model_version_stage(stage="Production")
    ↓
Serving layer: mlflow.pyfunc.load_model("models:/football-poisson/Production")
```

### Equivalence to managed platforms

This pattern is identical in structure to AWS SageMaker (Training Job
→ Model Registry → Endpoint), Databricks Model Serving, and Vertex AI
Model Registry. The registry/staging/serving concepts are platform-
agnostic — MLflow's open-source implementation teaches the same
mental model as any managed equivalent. Migrating between them is a
tooling change, not an architectural one.

### Train/serve skew

A common production ML failure mode: the features used at training
time are computed differently from the features used at serving
(inference) time — e.g. training reads from a batch CSV, serving
computes features live from a slightly different code path. This
produces silently degraded predictions with no error thrown.

**Feast (planned, Phase 2)** addresses this directly — the same
feature definitions serve both the offline store (training) and
online store (serving), eliminating the two-code-path problem.

---

## 11. Microservice Deployment for Multi-Agent Systems

### Why each agent is a separate service, not a shared process

A naive multi-agent implementation runs all agents as functions
within one Python process. This breaks down for the same reasons
monolithic architectures generally do:

- One agent's bug or crash takes down all agents
- Agents can't be scaled independently based on their own load
- Testing one agent requires the full system running
- No clear boundary for what data each agent is allowed to touch

### SportsPulse's agent deployment pattern

Each agent (Orchestrator, Stats, Prediction, News, RAG, Commentary)
is deployed as an independent container with its own service
boundary. Agents communicate via the A2A protocol — structured
JSON messages over HTTP, conceptually similar to how MCP structures
tool calls.

**The critical design rule:** no agent accesses Cassandra, MLflow,
or Qdrant directly. All data access is mediated through the MCP
server's typed tool interfaces. This means:

- Every data access is auditable at a single choke point
- Adding a new agent never requires new direct database credentials
- The same tool interface serves both human-facing API calls and
  agent-to-agent calls — one access pattern, not two

### Sequencing dependency

Agent deployment (A2A) requires the MCP server to exist first, since
every agent's tool calls route through MCP's typed interfaces. This
is why SportsPulse's roadmap places MCP (Phase 4, before) ahead of
full A2A orchestration (Phase 4, after) — agents need something
concrete to call before they can coordinate.

---

## Concepts to be added

| Layer | Concepts |
|---|---|
| Layer 3 — ML | Feature stores, train/serve skew ✅, model versioning ✅, data leakage, online vs offline features |
| Layer 4 — RAG | Vector databases, embedding models, retrieval strategies, semantic vs keyword search |
| Layer 5 — API | REST design, rate limiting, caching, API versioning, WebSocket vs polling |
| Layer 6 — MCP | Tool interfaces, structured data access patterns, agent protocols |
| Layer 7 — A2A | Agent orchestration ✅, multi-agent coordination, failure modes |
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
| Model registry pattern | MLflow, champion/challenger | Equivalent to SageMaker, self-hosted |
| Agent deployment | One container per agent | Independent scaling, clear service boundaries |
| Agent data access | Via MCP only, no direct DB access | Single auditable access point |
