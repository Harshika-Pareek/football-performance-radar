# Football Performance Radar — Architecture & Design Document

**Project:** Football Performance Radar  
**Repo:** football-performance-radar  
---

## 1. What this system does

A real-time pipeline that ingests live football match events, computes
expected vs. actual player performance using a statistically justified ML
model, and uses a RAG-grounded LLM to explain meaningful deviations — with
full MLOps governance and a real cloud deployment.

The system is built as a portfolio/learning project to demonstrate
end-to-end capability across: streaming data engineering, predictive ML
modeling, RAG, LLMOps, and production deployment.

---

## 2. The core problem it solves

Most sports AI projects do one of two things:
- Train an ML model to predict a score, then stop
- Wrap an LLM around static text with no real model underneath

This project does both properly: a real statistical model produces an
"expected performance" baseline for each player, the system flags when
actual performance deviates meaningfully from that baseline in real time,
and a RAG-grounded LLM explains *why* — grounded only in retrieved match
context, never hallucinated.

---

## 3. Full architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 1 — STREAMING INGESTION (COMPLETE)                         │
│                                                                   │
│  API-Football (match events: goals, cards, subs, VAR)            │
│       ↓ Python producer (polls every 90s on free tier)           │
│  Kafka topic: worldcup_match_events                              │
│       - KRaft mode (no Zookeeper)                                │
│       - Key: fixture_id (ensures per-match event ordering)       │
│       - Two listeners: localhost:9092 (external/Python scripts)  │
│                        kafka:29092 (internal/Docker containers)  │
│       ↓                                                           │
│  Kafka UI (localhost:8080) — visual monitoring                   │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 2 — STREAM PROCESSING (NEXT)                               │
│                                                                   │
│  Spark Structured Streaming                                       │
│       - Consumes worldcup_match_events topic                     │
│       - Computes rolling player stats (shots, key passes, etc.)  │
│       - Normalises schema across event types                      │
│       ↓                                                           │
│  Cassandra                                                        │
│       - Keyspace: football_radar                                  │
│       - Tables: live_events, player_stats (time-series)          │
│       - Wide-column store: correct fit for write-heavy,          │
│         time-ordered event data with no joins needed             │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 3 — ML MODELING                                            │
│                                                                   │
│  Model choice: deliberately chosen, not defaulted                │
│       - Poisson regression for count-based stats                 │
│         (shots, key passes — counts, not continuous values)      │
│       - Logistic regression for binary outcomes                  │
│         (did player outperform expectation: yes/no)              │
│       - XGBoost used only as a comparison baseline,             │
│         not the primary model                                    │
│                                                                   │
│  Evaluation: proper statistical rigor                            │
│       - Residual analysis                                         │
│       - Goodness-of-fit tests                                    │
│       - Confusion matrix / ROC where applicable                  │
│       - Overdispersion check (negative binomial if needed)       │
│                                                                   │
│  MLflow: model registry                                          │
│       - Every training run tracked                               │
│       - Serving code pulls "production" stage model              │
│       - No hardcoded model file paths in serving layer           │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 4 — RAG + LLM EXPLANATION                                  │
│                                                                   │
│  Ollama (local, free): Llama 3.1 8B                              │
│  Embeddings: nomic-embed-text                                     │
│                                                                   │
│  RAG pipeline:                                                    │
│       - Retrieves: recent form, matchup history, match context   │
│       - Grounds every answer in retrieved data                   │
│       - Guardrail: if retrieval returns nothing, the model       │
│         says "I don't have data on that" — never guesses         │
│                                                                   │
│  LLMOps governance:                                              │
│       - Golden eval dataset (20-30 Q&A pairs)                   │
│       - Prompt versioning (versioned prompts/ folder + git)      │
│       - Quality gate: prompt changes must pass eval before       │
│         shipping (min 75% pass rate)                             │
│       - Every query + response logged for drift monitoring       │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 5 — PRODUCT SURFACE + DEPLOYMENT                           │
│                                                                   │
│  FastAPI backend                                                  │
│       - REST endpoints: live player stats, flagged deviations    │
│       - WebSocket: live push to dashboard                        │
│       - /ask endpoint: RAG-grounded player/match questions       │
│       - Pulls ML model from MLflow registry, not a file          │
│                                                                   │
│  React dashboard                                                  │
│       - Live radar: flagged players (over/under-performing)      │
│       - AI explanation panel (grounded, sourced)                 │
│       - Live score + event feed                                  │
│                                                                   │
│  Deployment                                                       │
│       - Docker Compose (local dev, already working)              │
│       - Target: Railway/Render/Fly.io (simple, free tier)        │
│       - Public URL as portfolio demo                              │
│       - AWS migration: planned after v1 is live and stable       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Tech stack

| Layer | Tool | Why this tool specifically |
|---|---|---|
| Streaming ingestion | Kafka (KRaft, Docker) | Decouples producer from consumer; durable log means no events lost if consumer is slow or briefly down |
| Stream processing | Spark Structured Streaming | DataFrame API over streams; rolling aggregations; familiar Spark model |
| Storage | Cassandra | Wide-column store; correct fit for write-heavy, time-series event data; no joins needed |
| Data source | API-Football (free tier) | Multi-sport; live fixtures, events, player stats, odds — all in one API |
| ML modeling | Poisson + logistic regression | Chosen for the *shape* of the data (count-based outcomes), not defaulted. XGBoost as comparison baseline only |
| Model tracking | MLflow | Registry + versioning; serving layer pulls production-stage model |
| RAG + LLM | Ollama (Llama 3.1 8B) + nomic-embed-text | Local, free; full LLMOps depth without cloud cost |
| LLMOps | Eval dataset, prompt versioning, quality gates | Governance discipline before any model change ships |
| Backend | FastAPI | Async, production-grade Python API |
| Frontend | React | Live-updating dashboard components |
| Deployment | Docker → Railway/Render/Fly.io | Get a real public URL live; AWS migration later |

---

## 5. Key design decisions and why

### Why Kafka over a simpler queue
Match events genuinely need to be streamed — latency is the actual point.
Multiple consumers (Spark, a logging service, future consumers) can all read
the same topic independently without the producer needing to know about them.
Events are never lost if a consumer is temporarily down. A simple queue
(RabbitMQ, SQS) would work for one consumer but loses the multi-consumer and
replay capabilities that make this a real streaming system.

### Why Cassandra over Postgres
Match events are write-heavy, time-ordered, and never need joins — exactly
the use case Cassandra's wide-column model is designed for. Postgres would
work but would require more careful indexing and schema design to handle
high write throughput. That said: a Postgres read replica for analytics
queries is a reasonable future addition once the write path is proven.

### Why Poisson regression, not XGBoost, as the primary model
Player performance metrics (shots, key passes, duels) are *count data* —
non-negative integers with a specific distributional shape. Poisson
regression is the statistically correct model for count data; XGBoost
would work but assumes nothing about the data's distribution, making
evaluation and interpretation harder. Per the O'Reilly Soccer Analytics
book approach: choose the model based on what the data actually is, not
what's currently popular.

### Why local Ollama, not an OpenAI/Anthropic API
Zero cost for as many inference calls as needed during development. Full
LLMOps depth: model version control, local tracing, no rate limits, no
data leaving the machine. The governance story ("I run my own inference,
I log everything, I have guardrails") is more impressive in an interview
than "I called an external API."

### Why two Kafka listener ports (9092 and 29092)
Kafka advertises an address back to every connecting client for future
requests. Python scripts run on Windows (outside Docker) and need
`localhost:9092`. The kafka-ui container runs inside Docker and needs
`kafka:29092` (the internal Docker network name). A single advertised
address cannot work correctly for both audiences simultaneously — hence
two listeners, each advertising the address appropriate for whoever
connected through it.

---

## 6. Data flow — step by step

1. **Producer** polls API-Football every 90 seconds for live World Cup
   fixture events (goals, cards, substitutions, VAR decisions)
2. Each event is serialised as JSON and sent to the `worldcup_match_events`
   Kafka topic, keyed by `fixture_id` — ensuring all events for one match
   land on the same partition and remain in chronological order
3. **Spark Structured Streaming** reads from the topic as a continuous
   stream, computes rolling stats per player (shots in last 30 min, pass
   accuracy trend, etc.), and writes to Cassandra
4. **ML model** reads player feature vectors from Cassandra, computes
   "expected performance" per player per match, and flags deviations above
   a defined threshold — writing predictions back to Cassandra
5. **FastAPI** serves these predictions via REST and WebSocket, pulling the
   active model from the MLflow registry (not a hardcoded file)
6. When a player is flagged, the **/ask RAG endpoint** retrieves relevant
   context (recent form, matchup history, live match events) and passes it
   to the local LLM to generate a grounded explanation
7. **React dashboard** displays live flagged players, their deviation scores,
   and the AI-generated explanation — updating in real time via WebSocket

---

## 7. What's built so far

| Component | Status | Notes |
|---|---|---|
| Docker + WSL2 setup | Done | Windows + WSL2, KRaft mode, fixed cluster ID |
| Kafka cluster | Done | Two-listener config (external + internal) |
| Kafka UI | Done | localhost:8080, fully connected to broker |
| Python producer | Done | kafka-python-ng (Python 3.13 compatible) |
| Replay simulator | Done | 2022 World Cup replay, England 6-2 Iran, 21 events |
| Virtual environment | Done | Isolated per-project venv |
| Spark Structured Streaming | Not started | Next layer |
| Cassandra | Not started | |
| ML model | Not started | |
| MLflow | Not started | |
| RAG + LLM | Not started | |
| FastAPI | Not started | |
| React dashboard | Not started | |
| Deployment | Not started | |

---

## 8. Lessons learned so far

- **`kafka-python` 2.0.2 breaks on Python 3.13** — use `kafka-python-ng`
  as a drop-in replacement (same import name, actively maintained fork)
- **Kafka's `CLUSTER_ID` must be a valid base64-encoded UUID** — use
  `kafka-storage random-uuid` to generate one, never hand-write it
- **Two listener ports are required for mixed Docker/host access** —
  `KAFKA_ADVERTISED_LISTENERS` must advertise different addresses for
  different audiences; this is standard production Kafka config, not a
  Docker workaround
- **API-Football free tier restricts fixture queries to 2022-2024** —
  solved with a replay simulator using 2022 World Cup data, a standard
  streaming pipeline testing technique
- **Virtual environments are project-scoped** — activating one project's
  venv while working in another silently installs packages to the wrong
  location; always verify `pip show <package>` location after installing
- **`DOCKER_HOST` env variable from old Docker Toolbox installs** silently
  overrides Docker Desktop's daemon address — remove it permanently via
  Windows environment variable settings, not just the terminal session
