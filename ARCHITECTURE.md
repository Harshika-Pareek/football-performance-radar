# SportsPulse — Architecture & Design Document

**Project:** SportsPulse — AI Sports Intelligence Platform  
**Repo:** football-performance-radar  
**Status:** Layer 1 ✅ Layer 2 ✅ Layer 3 🔄 Layers 4-7 ⏳  
**Last updated:** July 2026

---

## 1. Objective

Transform a football streaming pipeline into a production-grade AI Platform
demonstrating Lead AI Engineer capabilities across:
- Distributed Systems & Data Engineering
- MLOps & Feature Engineering  
- LLMOps & RAG
- Agentic AI (MCP + A2A)
- AI Platform Engineering & Observability

---

## 2. Target Architecture

```
INGEST LAYER
─────────────────────────────────────────────────────────────
Football API  → Football Producer  → football.match.events
NBA API       → Basketball Producer → basketball.game.events
Tennis API    → Tennis Producer    → tennis.match.events
Cricket API   → Cricket Producer   → cricket.match.events
                                         ↓
                              Kafka (KRaft, multi-topic)
                              No Zookeeper, production-grade

PROCESS LAYER
─────────────────────────────────────────────────────────────
Spark Structured Streaming
  → reads all sport topics
  → normalises to common schema per sport
  → computes rolling features per player/team
  → writes to Cassandra (operational store)
  → writes to Feast offline store (feature history)

FEATURE STORE
─────────────────────────────────────────────────────────────
Feast (open source, no cloud required)
  Offline store → MinIO (S3-compatible, historical features)
  Online store  → Redis (low-latency, live serving features)
  
  Feature views:
    player_stats_fv     → goals, assists, cards per player
    team_form_fv        → last 5 results, goals for/against
    match_context_fv    → home advantage, head-to-head

ML LAYER
─────────────────────────────────────────────────────────────
MLflow (self-hosted) + MinIO (artifact store)
  → Experiment tracking (parameters, metrics, plots)
  → Champion/Challenger model registry
  → Sport-specific models:
       football-poisson-v1    (champion — Poisson regression)
       football-xgboost-v1    (challenger — tree-based)
       nba-performance-v1     (planned)
       tennis-form-v1         (planned)

Streaming Inference
  → Reads football.match.events from Kafka
  → Runs champion model in real-time per event
  → Writes predictions → football.predictions (Kafka topic)
  → FastAPI consumes predictions topic for live updates

Model Monitoring
  → Data drift detection (feature distribution shifts)
  → Prediction drift (model output distribution shifts)
  → Latency monitoring (p50/p95/p99)
  → Quality gates before champion promotion

VECTOR + RAG LAYER
─────────────────────────────────────────────────────────────
Qdrant (Docker) — vector database
  → Stores embeddings of match summaries
  → Player performance narratives
  → Historical match context for retrieval

RAG Pipeline
  → Hybrid search: semantic (vector) + keyword (BM25)
  → Ragas evaluation framework (faithfulness, relevance)
  → DeepEval for LLM output quality

LLM Layer (Ollama, local, free)
  → Llama 3.1 8B for explanation generation
  → nomic-embed-text for embeddings
  → Grounding guardrail: every claim must exist in retrieved context
  → Golden eval dataset per sport

Observability
  → Langfuse (self-hosted): LLM traces, prompt versions, evals
  → OpenTelemetry: distributed tracing across all services

SERVING LAYER
─────────────────────────────────────────────────────────────
FastAPI (versioned REST + WebSocket)
  /v1/predictions/{sport}/{fixture_id}
  /v1/players/{sport}/{player_id}/stats
  /v1/teams/{sport}/{team_id}/form
  /v1/ask                          (RAG endpoint)
  /v1/morning-briefing/{date}      (daily briefing)
  /v1/notifications/subscribe      (push notification opt-in)
  /health                          (health check)
  
  Security: JWT authentication, RBAC, rate limiting
  Logging: structured JSON, audit trail for AI decisions

React Dashboard (three surfaces)
  → Morning briefing (daily predictions + insights)
  → Live match intelligence (real-time win probability)
  → Post-match analysis (AI-generated, data-grounded)

NOTIFICATION LAYER
─────────────────────────────────────────────────────────────
Trigger conditions (evaluated on each new prediction):
  → New match added to morning briefing
  → Live win probability crosses significant threshold mid-match
  → Model vs market divergence detected (if odds layer built)

Delivery mechanism:
  → Web Push API (VAPID keys, self-hosted) — free, browser-native
  → OR OneSignal (free tier) — simpler, handles device/browser
    permission flow and delivery infrastructure
  → FastAPI endpoint receives subscription on opt-in, stores in
    Cassandra, triggers push on qualifying prediction events

AGENTIC LAYER
─────────────────────────────────────────────────────────────
MCP Server (Model Context Protocol)
  Exposes typed tool interfaces for any LLM agent:
  
  get_player(player_id, sport)
  get_match(fixture_id, sport)
  get_team(team_id, sport)
  get_prediction(fixture_id, sport)
  compare_players(player_a, player_b, sport)
  get_xg(fixture_id)
  find_similar_matches(fixture_id)
  explain_player_performance(player_id, match_id)

Agent Orchestrator (A2A protocol)
  Specialised agents coordinated by orchestrator:
  → Stats Agent       (queries Cassandra/Feature Store)
  → Prediction Agent  (queries MLflow registry)
  → News Agent        (fetches latest injury/lineup news)
  → RAG Agent         (retrieves relevant match context)
  → Commentary Agent  (generates match narratives)
  → Betting Risk Agent (detects value in market prices)

  Deployment pattern (see Section 6a for full detail):
  → Each agent = independent container, own service boundary
  → Agents never touch the database directly — all data access
    goes through the MCP server's typed tools
  → Orchestrator is the single entry point for user/agent queries

PLATFORM LAYER
─────────────────────────────────────────────────────────────
CI/CD: GitHub Actions
  → On every PR: run tests, eval set, lint
  → On merge to main: deploy to Railway
  → Model promotion: automated eval gate (75% pass rate)

Infrastructure as Code: Terraform
  → Targets Railway/Render (not AWS)
  → Reproducible deployments
  → Environment parity (local Docker = production)

Security
  → JWT authentication on all FastAPI endpoints
  → RBAC: different permissions per consumer type
  → Secrets: Docker secrets (local), Railway secrets (prod)
  → Audit logs: every AI decision logged with full context
  → Prompt injection defence on all LLM endpoints

AI Platform Dashboard
  → Model performance over time
  → RAG retrieval quality metrics
  → Agent call traces
  → Data pipeline health
  → Consumer lag monitoring
```

---

## 3. Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Streaming ingestion | Kafka (KRaft) | Durable log, multi-consumer, decoupled throughput |
| Stream processing | Spark Structured Streaming | Rolling aggregations, fault-tolerant checkpointing |
| Operational storage | Cassandra | Write-heavy, time-series, query-first partition design |
| Artifact storage | MinIO (S3-compatible) | Replaces AWS S3, free, same API, production pattern |
| Feature store | Feast | Open source, offline + online store, no cloud required |
| Experiment tracking | MLflow (self-hosted) | Champion/challenger registry, artifact versioning |
| Vector database | Qdrant | Hybrid search, Docker-native, production-grade |
| LLM | Ollama (Llama 3.1 8B) | Local, free, full LLMOps control |
| Embeddings | nomic-embed-text | Fast, free, high quality |
| RAG evaluation | Ragas + DeepEval | Faithfulness, relevance, hallucination detection |
| LLM observability | Langfuse (self-hosted) | Prompt versions, traces, eval scores |
| API | FastAPI | Versioned REST + WebSocket, async |
| Frontend | React | Three product surfaces |
| Notifications | Web Push API / OneSignal | Free, standard browser push mechanism |
| Agents | MCP + A2A | Typed tools, multi-agent orchestration |
| CI/CD | GitHub Actions | Free, integrated with repo |
| IaC | Terraform | Reproducible, targets Railway not AWS |
| Deployment | Railway/Render | Free tier, Docker-native, public URL |

---

## 4. Sports Coverage

| Sport | Data Source | Status |
|---|---|---|
| Football | football-data.org + API-Football | ✅ Active |
| Basketball (NBA) | nba_api (free, no key) | ⏳ Planned |
| Tennis | Jeff Sackmann open data | ⏳ Planned |
| Cricket | Cricsheet.org (free) | ⏳ Planned |
| Formula 1 | Ergast API (free) | ⏳ Planned |

---

## 5. ML Model Strategy

### Champion/Challenger Pattern

```
Training data → Train challenger model
                     ↓
              Automated evaluation
              (does challenger beat champion?)
                     ↓
         Yes → promote challenger to champion
         No  → keep existing champion
                     ↓
              Champion serves live predictions
```

### Model choices by sport

**Football:** Poisson regression (Dixon-Coles)
- Goals are count data — Poisson is statistically correct
- Models home/away goals as independent Poisson processes
- Outputs: home win %, draw %, away win %, predicted goals (pre-match)
- Note: this is a pre-match expected-goals estimate derived from
  historical attack/defence strength — not true shot-based xG, which
  requires shot-level data (location, angle) not present in this
  project's data sources

**NBA:** Regression on team offensive/defensive ratings
- Points scored follow a roughly normal distribution at game level
- Pythagorean expectation as baseline

**Tennis:** Logistic regression on surface-adjusted Elo ratings
- Surface matters enormously (clay vs grass vs hard)
- Head-to-head record as additional feature

**Cricket:** Duckworth-Lewis inspired model
- Run scoring is count data (similar to goals)
- Wickets remaining as key state variable

---

## 6. Key Design Decisions

### Why MinIO instead of AWS S3
MinIO is 100% S3-API compatible. Every line of code that works with MinIO works with S3 unchanged — just swap the endpoint URL. This means:
- Zero cloud spend during development
- Production migration = one config change
- Same skills learned (S3 API is the industry standard)

### Why Feast instead of a managed feature store
Feast runs entirely in Docker with no cloud dependency. The offline store uses MinIO (local S3) and the online store uses Redis. Pattern is identical to Databricks Feature Store or AWS SageMaker Feature Store — same concepts, no vendor lock-in, no cost.

### Why Railway/Render instead of AWS
The architecture patterns (Docker containers, environment variables, health checks, load balancing) are identical regardless of where they run. Railway/Render provides the deployment target without the AWS learning curve or cost. AWS migration, if ever needed, is a config change not an architecture change.

### Why self-hosted MLflow + MinIO instead of managed MLflow
Same reason — the MLflow API is identical whether self-hosted or managed (Databricks MLflow, AWS SageMaker Experiments). Learning self-hosted teaches the fundamentals; upgrading to managed is trivial.

### Why no AWS at all
Every AWS service in the original plan has a free, Docker-native alternative that teaches identical skills:
- S3 → MinIO
- SageMaker Feature Store → Feast
- SageMaker Experiments → MLflow
- RDS → SQLite (for MLflow metadata)
- ECS/EKS → Railway/Render
- CloudWatch → Langfuse + OpenTelemetry

---

## 6a. Model & Agent Deployment Strategy

### Model deployment — the SageMaker-equivalent pattern

Managed cloud ML platforms (AWS SageMaker, Databricks Model Serving)
provide: train → register → promote → serve via managed endpoint.
SportsPulse implements the identical pattern with open, self-hosted
tools:

```
Train model (Poisson regression, scikit-learn/scipy)
    ↓
MLflow tracks the experiment (params, metrics, artifacts)
    ↓
Model artifact stored in MinIO (S3-compatible object storage)
    ↓
MLflow Model Registry: staged as "Staging"
    ↓
Automated evaluation gate (held-out test set, must beat
current champion on defined metrics)
    ↓
Promoted to "Production" stage in registry
    ↓
FastAPI loads the "Production" stage model at startup/refresh
via mlflow.pyfunc.load_model("models:/football-poisson/Production")
    ↓
Serves predictions via REST endpoint
```

**Why this is architecturally equivalent to SageMaker:** the concepts
— experiment tracking, model registry, staged promotion, managed
serving via API — are present in both. The difference is deployment
target (self-hosted vs AWS-managed), not architecture. Migrating to
SageMaker later is a target change: swap MinIO for S3, MLflow's
tracking server for SageMaker Experiments, and the FastAPI serving
layer for a SageMaker endpoint — the model code and registry pattern
carry over unchanged.

### Agent deployment — A2A microservice pattern

Each agent in the A2A layer is deployed as an **independent
containerised service**, not a function or thread inside a monolith.
This mirrors how real multi-agent systems are deployed in production.

```yaml
# docker-compose.yml (Phase 4 addition)
  orchestrator-agent:
    build: ./agents/orchestrator
    depends_on: [mcp-server]

  stats-agent:
    build: ./agents/stats
    depends_on: [mcp-server]

  prediction-agent:
    build: ./agents/prediction
    depends_on: [mcp-server]

  news-agent:
    build: ./agents/news
    depends_on: [mcp-server]
```

**Key architectural rules:**
1. Agents communicate with each other via the A2A protocol
   (structured JSON messages over HTTP)
2. No agent accesses Cassandra, MLflow, or Qdrant directly — every
   data access goes through the MCP server's typed tool interfaces.
   This keeps data access auditable and consistent regardless of
   which agent or external LLM is calling in.
3. The Orchestrator Agent is the single entry point for any
   query — it decomposes the request and delegates to specialist
   agents, then synthesises their responses.
4. Each agent is independently scalable and independently
   deployable — a change to the News Agent doesn't require
   redeploying the Prediction Agent.

**Deployment target:** same Railway/Render project as the rest of
the stack. At this project's scale, independent scaling isn't
required — the value of the microservice boundary is architectural
clarity and testability, not throughput.

**Sequencing dependency:** Agent deployment (Layer 7) requires the
MCP server (Layer 6) to exist first, since every agent's tool calls
route through MCP. Building A2A before MCP would leave agents with
no data access pattern to call.

---

## 7. Delivery Phases

| Phase | Content | Target |
|---|---|---|
| Phase 1 | Streaming pipeline complete | ✅ Done |
| Phase 2 | ML lifecycle + Feature Store + MLflow champion/challenger | August 2026 |
| Phase 3 | RAG + Qdrant + Ragas evaluation + Langfuse observability | September 2026 |
| Phase 4 | MCP server + Multi-agent platform (A2A) | October 2026 |
| Phase 5 | CI/CD + Security + Terraform + Railway deployment + Notifications | November 2026 |
| Phase 6 | AI Platform Dashboard + full observability | December 2026 |

---

## 8. Security & Governance

### API Security
- JWT authentication on all endpoints
- RBAC: read-only vs admin vs agent permissions
- Rate limiting: per-key token bucket algorithm
- TLS: HTTPS on all external endpoints

### AI Governance
- Grounding guardrail: LLM claims must be traceable to retrieved data
- Prompt injection defence: input validation + classifier
- Audit log: every query, retrieved context, and response logged
- Model drift monitoring: scheduled eval set runs with alerting
- Human-in-the-loop: high-stakes outputs require review flag
- Prompt versioning: all prompts versioned in Git

### Data Security
- No PII in the pipeline (public sports data only)
- Secrets: Docker secrets locally, Railway secrets in production
- Encryption at rest: MinIO supports AES-256
- Dependency scanning: pip-audit + Dependabot

---

## 9. Build Status

| Component | Status |
|---|---|
| Kafka (KRaft, multi-listener) | ✅ Complete |
| Replay producer (WC2022) | ✅ Complete |
| Spark Structured Streaming consumer | ✅ Complete |
| Cassandra schema (UUID idempotent writes) | ✅ Complete |
| MinIO (artifact store) | ✅ Running |
| MLflow (experiment tracking) | ✅ Running |
| Feature extraction (2025/26 PL team strengths) | ✅ Complete |
| Multi-sport producer architecture | 🔄 In progress |
| Poisson model + MLflow logging | 🔄 In progress |
| Feast feature store | ⏳ Planned — Phase 2 |
| Champion/challenger promotion | ⏳ Planned — Phase 2 |
| Streaming inference | ⏳ Planned — Phase 2 |
| Qdrant vector database | ⏳ Planned — Phase 3 |
| RAG pipeline + Ragas | ⏳ Planned — Phase 3 |
| Langfuse observability | ⏳ Planned — Phase 3 |
| FastAPI serving layer | ⏳ Planned — Phase 3 |
| React dashboard | ⏳ Planned — Phase 3 |
| Push notifications | ⏳ Planned — Phase 5 |
| MCP server | ⏳ Planned — Phase 4 |
| A2A multi-agent (containerised per-agent) | ⏳ Planned — Phase 4 |
| GitHub Actions CI/CD | ⏳ Planned — Phase 5 |
| Terraform IaC | ⏳ Planned — Phase 5 |
| Railway deployment | ⏳ Planned — Phase 5 |
| AI Platform Dashboard | ⏳ Planned — Phase 6 |

---

## 10. Lessons Learned

- **Kafka two-listener config** — external clients need localhost:9092, internal containers need kafka:29092. Single advertised listener breaks one of the two audiences
- **Cassandra silent upsert** — INSERT is an upsert; UUID in primary key makes writes idempotent and prevents data loss for concurrent events at the same minute
- **Spark executor OOM** — 512MB insufficient for Kafka + Cassandra connectors; minimum 1GB executor memory for connector-heavy jobs
- **Spark driver networking** — spark.driver.host must be container name not localhost; executors connect back to driver across Docker network
- **WSL2 memory** — default 2GB WSL2 allocation causes Docker daemon crashes; .wslconfig memory=6GB resolves this
- **IPv4 vs IPv6** — Windows resolves localhost to ::1 (IPv6) by default; Kafka Docker binds IPv4 only; use 127.0.0.1 explicitly in producer config
- **MinIO vs S3** — identical API, zero cost locally, one config change to migrate to production S3
- **JAR caching** — Spark downloads connector JARs once to /root/.ivy2/jars; subsequent runs start in seconds not minutes
- **"xG" terminology** — true expected goals requires shot-level data (location, angle); a pre-match Poisson lambda is a different, valid metric but should be labelled "predicted goals" not "xG" to avoid overclaiming what the model measures
