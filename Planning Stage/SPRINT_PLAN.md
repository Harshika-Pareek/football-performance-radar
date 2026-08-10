# SportsPulse — Sprint Plan

A realistic, week-by-week delivery plan. Each sprint has a clear
deliverable, explicit dependencies on prior sprints, and honest
buffer for the debugging that real infrastructure work always
involves — this project has already hit Docker memory issues,
MLflow artifact storage failures, IPv4/IPv6 mismatches, and Spark
driver networking bugs, each costing real hours. Future sprints
budget for this rather than assuming friction-free building.

No fixed delivery date. Each sprint is scoped for quality over
speed — the goal is a genuinely well-built, well-understood platform,
not a rushed deadline. A2A multi-agent orchestration is included as
a core sprint, not deferred. The advanced pricing optimization engine
(customer simulation, demand modeling, P&L engine) remains a future
sprint, kept separate so it doesn't compete with finishing the core
agentic platform first.

---

## Sprint status legend

✅ Done | 🔄 In progress | ⏳ Not started | 🧊 Deferred (future phase)

---

## Sprint 1 (complete) — Streaming Foundation

**Goal:** reliable data flowing from a real API through to permanent storage.

| Deliverable | Status |
|---|---|
| Kafka (KRaft mode, two-listener config) | ✅ |
| Replay producer — World Cup 2022 events | ✅ |
| Spark Structured Streaming consumer | ✅ |
| Cassandra schema (UUID idempotent writes) | ✅ |
| Docker Compose stack, memory-tuned | ✅ |
| MinIO + MLflow running | ✅ |

**Real issues hit and resolved:** IPv4/IPv6 localhost mismatch,
Spark executor OOM (512MB → 1GB), Spark driver networking
(127.0.0.1 → container hostname), WSL2 memory limits, Cassandra
silent upsert bug.

---

## Sprint 2 (this week) — ML Model + Basic Pricing + Testing

**Goal:** a real, honestly-evaluated prediction model with a thin,
defensible pricing layer on top, and a testing habit established
from here onward.

| Deliverable | Owner task | Status |
|---|---|---|
| `extract_features.py` — team strength features | Built | ✅ |
| `train_model.py` — Poisson model, season-split backtest | Built | ✅ |
| `predict_all_matches.py` — Week 1 2026/27 predictions, confidence flagged | Build | 🔄 |
| MLflow formal model registration (not just run logging) | Build | ⏳ |
| `pricing/fair_odds.py` — probability → fair odds → market odds | Build | ⏳ |
| `pricing/margin.py` — confidence-linked margin sizing | Build | ⏳ |
| `tests/test_pricing.py` — pytest coverage for pricing logic | Build | ⏳ |
| `tests/test_model.py` — pytest coverage for prediction logic | Build | ⏳ |

**Explicitly NOT in this sprint:** customer simulation, demand
modeling, elasticity curves, P&L engine, pricing optimizer. These
are Phase 8 (see bottom of this document).

**Definition of done:** running `pytest` passes; MLflow shows a
registered model, not just a logged run; a Week 1 fixture can be
priced end-to-end (probability → odds → margin) with a passing test
proving low confidence produces a wider margin than high confidence.

---

## Sprint 3 — RAG Foundations

**Goal:** grounded, non-hallucinating explanations for predictions.

| Deliverable | Status |
|---|---|
| Ollama running locally (Llama 3.1 8B) | ⏳ |
| `rag/ingest.py` — embed match data + generated summaries | ⏳ |
| `rag/retrieve.py` — basic retrieval over embedded data | ⏳ |
| `rag/generate.py` — LLM generates explanation from retrieved context | ⏳ |
| Grounding guardrail — refuse to answer if retrieval is empty | ⏳ |
| `tests/test_rag.py` — verify guardrail actually blocks ungrounded answers | ⏳ |

**Dependency:** requires Sprint 2's model output to have something
real to explain.

**Buffer note:** local LLM setup (Ollama model download, first-run
performance tuning) commonly takes longer than expected on a first
attempt — budget a half-day just for environment setup before any
RAG logic is written.

---

## Sprint 4 — LLMOps Governance

**Goal:** the eval discipline that makes the RAG layer trustworthy,
not just functional.

| Deliverable | Status |
|---|---|
| Golden eval dataset — 20-30 Q&A pairs, manually scored | ⏳ |
| `rag/evaluate.py` — runs eval set, computes pass rate | ⏳ |
| Quality gate — 75% pass rate required before "shipping" a prompt change | ⏳ |
| Prompt versioning — prompts stored in Git, versioned folder | ⏳ |
| Audit logging — every query + retrieved context + response logged | ⏳ |

**Dependency:** requires Sprint 3's RAG pipeline to exist.

---

## Sprint 5 — FastAPI Serving Layer

**Goal:** the platform becomes queryable over HTTP, not just runnable as scripts.

| Deliverable | Status |
|---|---|
| `serving/api.py` — versioned REST endpoints (`/v1/predictions`, `/v1/ask`) | ⏳ |
| `/health` endpoint | ⏳ |
| Structured JSON logging middleware | ⏳ |
| Pydantic input validation on all endpoints | ⏳ |
| `tests/test_api.py` — endpoint tests (status codes, validation errors) | ⏳ |
| MLflow model loaded via registry at API startup, not hardcoded file | ⏳ |

**Dependency:** requires a working model (Sprint 2) and RAG pipeline
(Sprint 3-4) to have something to serve.

---

## Sprint 6 — Deployment

**Goal:** a public URL. This is the single highest-value deliverable
for interview credibility — "here's a live link" beats any amount of
local demo.

| Deliverable | Status |
|---|---|
| Dockerfile for the FastAPI service | ⏳ |
| Deploy to Railway or Render (free tier) | ⏳ |
| Environment variables / secrets configured on the platform | ⏳ |
| Public URL live and responding | ⏳ |
| Basic React dashboard — one view, calling the deployed API | ⏳ |

**Dependency:** requires Sprint 5's API to exist and work locally
first — deploying broken code just moves the debugging to a place
with worse visibility.

**Buffer note:** first-time deployment to any new platform typically
surfaces environment-parity issues (a package that installs fine
locally but not in the platform's build environment, a missing
environment variable). Budget for at least one full debugging cycle.

---

## Sprint 7 — MCP Server

**Goal:** the platform's capabilities become callable by any
MCP-aware agent, not just a human via the dashboard.

| Deliverable | Status |
|---|---|
| `mcp/server.py` — FastMCP server definition | ⏳ |
| `get_prediction()`, `get_player_stats()` tools | ⏳ |
| `explain_deviation()`, `get_market_price()` tools | ⏳ |
| `tests/test_mcp_tools.py` — verify tool contracts | ⏳ |
| MCP server connectable from Claude Desktop as a real test | ⏳ |

**Dependency:** requires Sprint 5's API logic to wrap — MCP tools
call the same underlying functions the REST API calls, just exposed
via a different protocol.

---

## Sprint 8 — A2A Multi-Agent Orchestration

**Goal:** specialised agents, each with one responsibility, coordinated
by a single orchestrator — the genuine agentic architecture, not just
an MCP server sitting unused.

| Deliverable | Status |
|---|---|
| `agents/base.py` — BaseAgent class, AgentMessage protocol | ⏳ |
| `agents/orchestrator.py` — routes queries to the right specialist agent(s) | ⏳ |
| `agents/prediction_agent.py` — wraps get_prediction() MCP tool | ⏳ |
| `agents/pricing_agent.py` — wraps get_market_price() MCP tool | ⏳ |
| `agents/news_agent.py` — fetches injury/lineup RSS signals | ⏳ |
| `agents/synthesis_agent.py` — combines multi-agent outputs into one coherent answer | ⏳ |
| Each agent deployed as its own container | ⏳ |
| `tests/integration/test_agents.py` — verify orchestrator correctly routes and synthesises | ⏳ |

**Dependency:** requires Sprint 7's MCP server — every agent calls
MCP tools rather than touching Cassandra/MLflow/Qdrant directly, so
there must be something real to call.

**Buffer note:** A2A tooling is genuinely new (2026) and less
battle-tested than Kafka/Spark/MLflow. Budget extra debugging time
here specifically — this is the sprint most likely to hit unexpected
library issues, similar to how Spark-on-Windows had unexpected
friction in Sprint 1.

**Definition of done:** asking the orchestrator a compound question
("who should I watch in tonight's match and is the market well
priced?") produces a synthesised answer drawing from at least two
specialist agents, with the full call trace visible in logs.

---

## Full platform complete

By the end of Sprint 8, the platform has: real streaming
infrastructure, a backtested statistical model, a defensible pricing
layer, grounded RAG explanations with LLMOps governance, a deployed
public API and dashboard, an MCP server any agent can call, and
genuine multi-agent orchestration via A2A. This is the complete,
honest, demoable AI platform.

---

## Deferred — Sprint 9+ (Future Work)

### Advanced Pricing Engine
```
pricing/
├── demand_model.py     # SYNTHETIC — segment-specific elasticity curves
├── liability.py        # exposure constraints on the optimizer
└── optimizer.py         # margin that maximizes expected GGR subject
                          #   to liability constraints

simulation/
├── customer_simulator.py  # 4 synthetic segments: Casual, Regular,
│                            High Value, Price Sensitive
├── bet_simulator.py        # simulated stake placement given demand
├── payout_engine.py        # payout calculation given outcome
└── pnl_engine.py            # turnover - payout = GGR, aggregated

evaluation/
└── pricing_metrics.py      # backtest the PRICING STRATEGY (not just
                              #   the model) across the 2025/26 season

dashboard/
└── business_metrics.py     # GGR by segment, margin distribution,
                              #   exposure over time
```

This is genuinely a second, substantial project — a full
simulation-based pricing optimization engine, all clearly labeled
as using synthetic customer data throughout (no real customer data
exists or is used). Realistic scope: 4-6 additional weeks, undertaken
only after the core platform (Sprints 1-7) is complete and, ideally,
after the job-search goal has landed — at which point this becomes a
genuinely differentiated deep-dive rather than a distraction from
finishing the core story.

### Dynamic In-Play Repricing
Recalculating win probability and repricing odds mid-match as events
stream in, using the same Kafka/Spark infrastructure already built
for pre-match predictions. Architecturally straightforward (the hard
infrastructure problem is already solved); deferred because it adds
complexity to Sprint 2's model without adding proportionate interview
value compared to finishing the RAG/MCP/deployment story first.
