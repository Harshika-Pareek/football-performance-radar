# SportsPulse — Personal Sports Intelligence Platform

A real-time, multi-sport intelligence platform that works like a smart
newspaper — data-driven, AI-explained, and personalised to what you follow.

Built to mirror the architecture used by Sportradar, Stats Perform, and
enterprise sports data companies — but as an open, personal platform you
actually use every day.

---

## The problem this solves

Sports generates enormous amounts of data every second — goals, cards,
player movements, odds shifts, injury news. Raw data is worthless. What's
valuable is **intelligence derived from that data, fast enough to act on.**

Every serious sports company — betting platforms (Flutter, DraftKings),
media (Sky Sports, The Athletic), fantasy (FanDuel), and clubs (Premier
League, NFL franchises) — runs the same underlying infrastructure to turn
raw sports data into actionable intelligence. SportsPulse is a personal,
open version of that infrastructure.

---

## Three product surfaces

### 1. Morning Briefing (daily, automated)
Generated every morning — works like a personalised sports newspaper:
- Today's matches across all tracked sports and leagues
- AI predictions with confidence levels per match
- Key injury and lineup news affecting predictions
- Fantasy team recommendations (start/bench/captain)
- Yesterday's results with AI post-match analysis
- Value signals: where model probability diverges from market odds

### 2. Live Match Intelligence (real-time, during a match)
Opens during a live match — works like a personal analyst on your screen:
- Real-time win probability updating every 60 seconds
- Player performance radar: who is over/underperforming their baseline
- Momentum tracker: which team is controlling the match
- AI explanation of key moments
- Model vs market signal: where the ML model disagrees with bookmakers

### 3. Post-Match Analysis (generated within 1 hour of final whistle)
AI-generated analysis no journalist had time to write:
- Player ratings: model-generated, not subjective opinion
- xG vs actual: who was lucky, who was unlucky
- Key tactical insight the data reveals
- Model accuracy review
- Fantasy performance: predicted points vs actual

---

## Who uses this architecture in the real world

| Company | How they use the same pattern |
|---|---|
| **Sportradar** | Powers 1,000+ betting/media companies with real-time sports intelligence |
| **Stats Perform (Opta)** | Player tracking + ML performance models for clubs and broadcasters |
| **Flutter/FanDuel** | Real-time odds pricing engine fed by live event streams |
| **The Athletic** | AI-generated match reports and personalised content feeds |
| **DraftKings** | ML player projections updated in real time for fantasy sports |
| **Premier League clubs** | Player performance baselines for coaching and recruitment |

SportsPulse mirrors this architecture at a personal scale.

---

## Architecture

```
INGEST LAYER
API-Football / API-Sports (multi-sport: football, NFL, cricket)
    ↓ config-driven producer (leagues.yaml defines what flows)
Kafka topics (KRaft):
    football.{league}.match.events
    nfl.nfl.match.events
    cricket.{league}.match.events

PROCESS LAYER
Spark Structured Streaming
    → normalises events across sports
    → computes rolling features per player
    → writes to Cassandra
Cassandra: PRIMARY KEY ((sport, league_id), fixture_id, minute, event_id)

INTELLIGENCE LAYER
ML Models (sport-specific, registered in MLflow)
    football-v1: Poisson regression
    nfl-v1: EPA-based model
RAG + LLM: Ollama (Llama 3.1 8B) + nomic-embed-text
LLMOps: golden eval dataset, prompt versioning, quality gates

DELIVERY LAYER
FastAPI: versioned REST + WebSocket + /health + structured logging
Scheduler: daily briefing at 8am
React: morning / live / post-match surfaces
Deployment: Docker → Railway/Render → public URL

AGENTIC LAYER (planned)
MCP Server: typed tool interfaces for any LLM agent
A2A Multi-Agent: orchestrator + stats + prediction + explanation agents
```

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Streaming | Kafka (KRaft) | Durable log, multi-consumer, decoupled |
| Processing | Spark Structured Streaming | Rolling aggregations, fault-tolerant |
| Storage | Cassandra | Write-heavy, time-series, query-first design |
| Config | leagues.yaml | Add any sport/league without code changes |
| Data | API-Football / API-Sports | Multi-sport free tier |
| ML | Poisson regression + MLflow | Justified by data shape, not defaulted |
| RAG | Ollama + nomic-embed-text | Local, free, grounded |
| LLMOps | Eval dataset, prompt versioning | Governance before shipping |
| API | FastAPI | Versioned REST + WebSocket |
| Frontend | React | Three product surfaces |
| Agents | MCP + A2A | Typed interfaces, multi-agent orchestration |
| Deploy | Docker → Railway | Public URL |

---

## Sports covered

| Sport | League | Status |
|---|---|---|
| Football | World Cup 2022 | ✅ Active |
| Football | Premier League 2024/25 | ⏳ Planned |
| NFL | 2024/25 season | ⏳ Planned |
| Cricket | IPL + International | ⏳ Planned |

---

## Build status

| Layer | Component | Status |
|---|---|---|
| Layer 1 | Kafka ingestion + replay producer | ✅ Complete |
| Layer 2 | Spark Structured Streaming + Cassandra | 🔄 In progress |
| Config | leagues.yaml multi-sport config | ✅ Complete |
| Layer 3 | ML models + MLflow | ⏳ Next |
| Layer 4 | RAG + LLMOps | ⏳ Planned |
| Layer 5 | FastAPI + React + Scheduler | ⏳ Planned |
| Layer 6 | MCP server | ⏳ Planned |
| Layer 7 | A2A multi-agent | ⏳ Planned |

---

## Quick start

```bash
cd docker
docker compose up -d kafka cassandra spark spark-worker
cd ../producer && python replay_producer.py
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0" \
  /opt/spark_apps/consumer.py
docker exec -it cassandra cqlsh -e "SELECT * FROM football_radar.match_events LIMIT 10;"
```
