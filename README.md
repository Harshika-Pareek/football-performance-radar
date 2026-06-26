# Football Performance Radar — World Cup 2026

A real-time system that ingests live World Cup match events, computes
expected vs. actual player performance using a properly justified
statistical model, and uses a RAG-grounded LLM to explain meaningful
deviations — with MLOps governance (model registry, evaluation gates,
versioning) and a real cloud deployment.

## Why this exists

This is a hands-on learning project, built to gain real depth across
streaming data engineering, statistical ML, RAG, LLMOps, and deployment —
using a real, live data source (the 2026 World Cup) rather than a static
dataset or toy example.

It is a portfolio/learning build, not a commercial product.

## The idea

Most "AI + sports" projects either (a) just predict a score with an ML
model and stop, or (b) wrap an LLM around static text with no real model
underneath. This project tries to do both properly: a real statistical
model produces an "expected performance" baseline for each player, the
system flags when actual performance deviates meaningfully from that
baseline in real time, and a RAG-grounded LLM explains *why* — grounded in
retrieved match context, not invented.

## Architecture

```
Kafka (live World Cup match events, KRaft mode)
   -> Spark Structured Streaming (rolling player stats in real time)
   -> Cassandra (live + historical player performance store)
   -> Statistical model (Poisson/logistic regression — chosen deliberately
      based on the shape of the data, evaluated properly: residuals,
      goodness-of-fit, not just accuracy)
        -> flags real-time deviation from expected performance
   -> MLflow (model registry — serving pulls the "production" stage
      model, not a hardcoded file)
   -> RAG (retrieves context: recent form, matchup history, news)
   -> LLM via Ollama (explains the flagged deviation, grounded in
      retrieved context, with a golden eval set and prompt versioning)
   -> FastAPI (serves predictions + explanations)
   -> React dashboard (live radar view of flagged players)
   -> Deployed: Docker -> Railway/Render/Fly.io -> public URL
      (AWS migration considered later, once the pipeline works)
```

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Streaming ingestion | Kafka (KRaft mode, Docker) | Decouples live data fetching from processing |
| Stream processing | Spark Structured Streaming | Real-time rolling stat computation |
| Storage | Cassandra | Wide-column store, fits time-series match events |
| Data source | API-Football (free tier) | Live World Cup fixtures + match events |
| ML modeling | Statistical model chosen deliberately (Poisson regression for count-based stats; logistic regression where binary; tree-based model only as a comparison baseline, not the default) | Real predictive modeling rigor, properly evaluated — not a default tool choice |
| Model tracking | MLflow | Registry + versioning, production-stage model serving |
| RAG + LLM | Ollama (Llama 3.1 8B) + nomic-embed-text | Local, free, grounded explanations |
| LLMOps | Golden eval dataset, prompt versioning, quality gates | Governance before anything ships |
| Backend | FastAPI | Serves predictions + explanations |
| Frontend | React | Live dashboard |
| Deployment | Docker -> Railway/Render/Fly.io | Real public URL, simple first |

## Status

- [x] Day 1: Docker + Kafka environment set up (Windows + WSL2)
- [ ] Kafka fundamentals (reading in progress)
- [ ] Kafka producer: live World Cup events into a topic
- [ ] Spark Structured Streaming consumer
- [ ] Cassandra storage layer
- [ ] End-to-end pipeline milestone
- [ ] FastAPI read layer
- [ ] React dashboard MVP
- [ ] Statistical model: chosen, trained, evaluated
- [ ] MLflow model registry
- [ ] RAG + LLM explanation layer
- [ ] LLMOps eval gates
- [ ] Deployment (public URL)

## Quick start

Not yet runnable end to end — this section will be filled in as each layer
is built. See `docs/` for day-by-day setup notes as they're written.
