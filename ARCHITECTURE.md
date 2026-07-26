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
  /health                          (health check)
  
  Security: JWT authentication, RBAC, rate limiting
  Logging: structured JSON, audit trail for AI decisions

React Dashboard (three surfaces)
  → Morning briefing (daily predictions + insights)
  → Live match intelligence (real-time win probability)
  → Post-match analysis (AI-generated, data-grounded)

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
- Outputs: home win %, draw %, away win %, expected goals

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

## 7. Delivery Phases

| Phase | Content | Target |
|---|---|---|
| Phase 1 | Streaming pipeline complete | ✅ Done |
| Phase 2 | ML lifecycle + Feature Store + MLflow champion/challenger | August 2026 |
| Phase 3 | RAG + Qdrant + Ragas evaluation + Langfuse observability | September 2026 |
| Phase 4 | MCP server + Multi-agent platform (A2A) | October 2026 |
| Phase 5 | CI/CD + Security + Terraform + Railway deployment | November 2026 |
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
| Multi-sport producer architecture | 🔄 In progress |
| Feast feature store | ⏳ Planned — Phase 2 |
| ML models + champion/challenger | ⏳ Planned — Phase 2 |
| Streaming inference | ⏳ Planned — Phase 2 |
| Qdrant vector database | ⏳ Planned — Phase 3 |
| RAG pipeline + Ragas | ⏳ Planned — Phase 3 |
| Langfuse observability | ⏳ Planned — Phase 3 |
| FastAPI serving layer | ⏳ Planned — Phase 3 |
| React dashboard | ⏳ Planned — Phase 3 |
| MCP server | ⏳ Planned — Phase 4 |
| A2A multi-agent | ⏳ Planned — Phase 4 |
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
