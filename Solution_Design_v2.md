# SportsPulse — Solution Design

Architecture diagrams following the C4 model (Context → Container →
Component). These render automatically on GitHub — no extension needed.

---

## Level 1: System Context Diagram

```mermaid
graph TB
    User(["End User<br/>Sports fan, analyst,<br/>fantasy player"])
    Agent(["LLM Agent<br/>Claude, custom agents<br/>via MCP protocol"])

    subgraph SportsPulse["SportsPulse Platform"]
        SP["AI Sports Intelligence<br/>Platform"]
    end

    FootballAPI["football-data.org<br/>Premier League data"]
    NBAAPI["nba_api<br/>NBA statistics"]
    CricketAPI["Cricsheet<br/>Cricket data"]
    TennisAPI["Tennis Abstract<br/>ATP/WTA data"]
    F1API["Ergast API<br/>Formula 1 data"]
    NewsRSS["RSS Feeds<br/>BBC Sport, Sky Sports"]

    User -->|Views predictions and prices| SP
    Agent -->|Calls tools via MCP protocol| SP

    FootballAPI -->|Match results, fixtures| SP
    NBAAPI -->|Game stats, player data| SP
    CricketAPI -->|Ball-by-ball data| SP
    TennisAPI -->|Match results, Elo ratings| SP
    F1API -->|Race results, lap times| SP
    NewsRSS -->|Injury news, lineup updates| SP
```

---

## Level 2: Container Diagram

```mermaid
graph TB
    subgraph Ingest["Ingest Layer"]
        FP["Football Producer"]
        BP["Basketball Producer"]
        CP["Cricket Producer"]
        TP["Tennis Producer"]
    end

    subgraph Streaming["Streaming Layer"]
        KAFKA["Apache Kafka<br/>KRaft mode, multi-topic"]
        SPARK["Spark Structured<br/>Streaming Consumer"]
    end

    subgraph Storage["Storage Layer"]
        CASS["Cassandra<br/>Operational store"]
        MINIO["MinIO<br/>Artifact store"]
        QDRANT["Qdrant<br/>Vector database"]
        REDIS["Redis<br/>Online feature store"]
    end

    subgraph ML["ML Layer"]
        FEAST["Feast<br/>Feature Store"]
        MLFLOW["MLflow<br/>Experiment tracking"]
        INFER["Streaming Inference"]
    end

    subgraph Pricing["Pricing Layer"]
        ODDS["Probability to Odds<br/>Fair and market price"]
        MARGIN["Margin Sizing<br/>Confidence-linked"]
    end

    subgraph RAG["RAG plus LLM Layer"]
        RAG_PIPE["RAG Pipeline<br/>Hybrid search"]
        OLLAMA["Ollama<br/>Llama 3.1 8B"]
        LANGFUSE["Langfuse<br/>LLM Observability"]
    end

    subgraph Serving["Serving Layer"]
        FASTAPI["FastAPI<br/>REST plus WebSocket"]
        REACT["React Dashboard"]
    end

    subgraph Agentic["Agentic Layer"]
        MCP["MCP Server<br/>Typed tools"]
        ORCH["Agent Orchestrator<br/>A2A protocol"]
        AGENTS["Specialised Agents"]
    end

    FP --> KAFKA
    BP --> KAFKA
    CP --> KAFKA
    TP --> KAFKA
    KAFKA --> SPARK
    SPARK --> CASS
    SPARK --> FEAST
    FEAST --> MLFLOW
    FEAST --> REDIS
    MLFLOW --> INFER
    INFER --> KAFKA
    MLFLOW --> ODDS
    ODDS --> MARGIN
    CASS --> RAG_PIPE
    QDRANT --> RAG_PIPE
    RAG_PIPE --> OLLAMA
    OLLAMA --> LANGFUSE
    MLFLOW --> MINIO
    CASS --> FASTAPI
    REDIS --> FASTAPI
    MARGIN --> FASTAPI
    RAG_PIPE --> FASTAPI
    FASTAPI --> REACT
    FASTAPI --> MCP
    MCP --> ORCH
    ORCH --> AGENTS
```

---

## Level 2: Data Flow Diagram

```mermaid
sequenceDiagram
    participant API as Sports API
    participant P as Producer
    participant K as Kafka
    participant S as Spark
    participant C as Cassandra
    participant ML as MLflow
    participant PR as Pricing Layer
    participant FA as FastAPI
    participant U as User

    API->>P: Match events
    P->>K: Push to football.match.events
    K->>S: Spark reads stream
    S->>C: Write events, idempotent UUID key
    C->>ML: Feed historical features
    ML->>PR: Model probability plus confidence flag
    PR->>PR: Fair odds, then margin by confidence
    PR->>FA: Priced market ready to serve
    C->>FA: Historical stats on request
    FA->>U: REST API plus WebSocket push
```

---

## Level 2: ML Pipeline Diagram

```mermaid
graph LR
    FO["Feast Offline Store"]
    TRAIN["Train Challenger Model"]
    EVAL["Automated Evaluation<br/>Out-of-sample, calibration-based"]
    CHAL["Challenger Model<br/>staging"]
    CHAMP["Champion Model<br/>production"]
    SI["Streaming Inference"]
    MONITOR["Model Monitor<br/>Drift detection"]

    FO --> TRAIN
    TRAIN --> EVAL
    EVAL -->|Beats champion| CHAL
    EVAL -->|Fails| TRAIN
    CHAL -->|Promoted| CHAMP
    CHAMP --> SI
    SI --> MONITOR
    MONITOR -->|Drift detected| TRAIN
```

Evaluation prioritises calibration over raw accuracy — a model
that says "70%" should be correct roughly 7 times in 10, which
matters more for a downstream pricing decision than raw outcome
accuracy alone.

---

## Level 2: Pricing and Margin Diagram

```mermaid
graph LR
    PROB["Model Probability<br/>from Poisson plus MLflow"]
    CONF["Confidence Flag<br/>HIGH or LOW"]
    FAIR["Fair Odds<br/>1 divided by probability"]
    MARGINCALC["Margin Sizing<br/>LOW confidence, wider margin"]
    MARKET["Market Odds<br/>fair odds times margin"]

    PROB --> FAIR
    PROB --> CONF
    CONF --> MARGINCALC
    FAIR --> MARKET
    MARGINCALC --> MARKET
```

**Why confidence drives margin:** a team with no training history
(for example, newly promoted) produces a probability estimate with
more underlying uncertainty. Pricing under that uncertainty should
widen the margin to compensate for that uncertainty — the same
human-in-the-loop principle already used in this project's
confidence-scoring pattern, applied here to a pricing decision
instead of a review-queue decision.

**Worked example:**

```
Arsenal vs Coventry City
  Model probability (Arsenal win): 73%
  Confidence: LOW (Coventry has no PL history)
  Fair odds: 1 / 0.73 = 1.37
  Standard margin (HIGH confidence): 5%
  Widened margin (LOW confidence): 8%
  Market odds: 1.37 x (1 - 0.08) = 1.26
```

---

## Level 3: MCP Server Tools

```mermaid
graph TB
    subgraph MCP["MCP Server"]
        T1["get_player"]
        T2["get_match"]
        T3["get_team"]
        T4["get_prediction"]
        T5["compare_players"]
        T6["get_xg"]
        T7["find_similar_matches"]
        T8["explain_performance"]
        T9["get_market_price"]
    end

    subgraph Agents["A2A Agents"]
        OA["Orchestrator Agent"]
        SA["Stats Agent"]
        PA["Prediction Agent"]
        PRA["Pricing Agent"]
        NA["News Agent"]
        RA["RAG Agent"]
    end

    subgraph Data["Data Sources"]
        CASS["Cassandra"]
        MLFLOW["MLflow"]
        QDRANT["Qdrant"]
        NEWS["RSS Feeds"]
    end

    OA --> SA
    OA --> PA
    OA --> PRA
    OA --> NA
    OA --> RA

    SA --> T1
    SA --> T2
    SA --> T3
    PA --> T4
    PRA --> T9
    RA --> T7
    RA --> T8
    NA --> NEWS

    T1 --> CASS
    T2 --> CASS
    T3 --> CASS
    T4 --> MLFLOW
    T9 --> MLFLOW
    T7 --> QDRANT
    T8 --> QDRANT
```

---

## End-State Summary

Six layers, bottom to top:

```
Data (Kafka, Spark, Cassandra)
  -> Model (Poisson regression, MLflow)
    -> Pricing (odds and margin, confidence-linked)
      -> RAG explanation (grounded in retrieved data)
        -> MCP server (typed tools any agent can call)
          -> A2A agents (Stats, Prediction, Pricing, News,
             coordinated by an Orchestrator)
```

Each layer only adds value if the layer beneath it is trustworthy.
This is why the build order is data and model first, pricing and
explanation next, and agentic coordination last — once there is
something real for agents to coordinate around.

| Layer | Status |
|---|---|
| Data | Built and working |
| Model | Built, backtested with out-of-sample season split |
| Pricing | In progress |
| RAG | Planned |
| MCP | Planned |
| A2A | Planned |

---

## Why These Design Choices

**Poisson regression, not deep learning, for match outcomes.**
Football outcome data is small (thousands, not millions, of matches
per league per season) and non-stationary — a 2015 match has minimal
predictive value for 2026, since squads and tactics change every
season. Deep learning would overfit on this scale of data without
outperforming a well-specified statistical model. Deep learning is
reserved in this project's roadmap for tasks where it genuinely
fits: video-based tracking or shot-level xG, not season-to-season
outcome prediction from box-score results.

**Season-based train/test split, not random split.**
A random split across seasons risks temporal leakage — training on
a March 2026 match while testing on an August 2025 match from the
same season lets the model "see the future" relative to what it is
being evaluated on. Splitting by season keeps every test match
strictly after every training match, mirroring how the model would
actually be used in production.

**Calibration over raw accuracy as the evaluation metric.**
For pricing, whether a 70% prediction is correct 7 times in 10
matters more than whether the single most likely outcome was
predicted correctly. A model can have modest raw accuracy and still
be well-calibrated and useful for pricing, which is why evaluation
in this project tracks calibration alongside accuracy, not accuracy
alone.

**Confidence flag drives margin, not just a display label.**
Rather than silently falling back to a league-average estimate for
teams with no training history, the model surfaces this as a LOW
confidence flag. That flag becomes an actionable pricing input:
LOW confidence widens the margin, directly mirroring the
human-in-the-loop, uncertainty-aware pattern already used
elsewhere in this project.

---

## How to Use These Diagrams

**In your GitHub README:** paste any diagram block directly — GitHub
renders Mermaid natively, no extension required.

**In Lucidchart or draw.io:** Insert -> Advanced -> Edit Diagram (XML),
paste the Mermaid code, export as PNG or PDF for presentations.

---

## Product Mockup: Layer Stack (Demo View)

This is the simplified, presentation-friendly version of the layer
stack shown above — useful for walking through the story in an
interview or demo, without the full technical detail of the
Container diagram.

```mermaid
graph TB
    subgraph Foundation["Foundation - Built"]
        DATA["Data Layer<br/>Kafka + Spark<br/>to Cassandra"]
        MODEL["Model Layer<br/>Poisson + MLflow<br/>Probability + confidence"]
        PRICE["Pricing Layer<br/>Odds + margin<br/>Confidence sets margin width"]
    end

    RAG_BOX["RAG Explanation Layer<br/>Grounds every claim in retrieved match data"]

    MCP_BOX["MCP Server<br/>get_prediction, get_player_stats<br/>explain_deviation, get_market_price"]

    A1["Stats Agent"]
    A2["Prediction Agent"]
    A3["Pricing Agent"]
    A4["News Agent"]

    DATA --> RAG_BOX
    MODEL --> RAG_BOX
    PRICE --> RAG_BOX
    RAG_BOX --> MCP_BOX
    MCP_BOX --> A1
    MCP_BOX --> A2
    MCP_BOX --> A3
    MCP_BOX --> A4
```

**The talking-through order (bottom to top):**

1. **Data layer** — reliable ingestion first. No prediction matters
   if the pipeline underneath it is unreliable.
2. **Model layer** — a statistically justified model, honestly
   backtested out-of-sample, with a confidence signal rather than
   false precision.
3. **Pricing layer** — the bridge from a probability to a business
   decision: fair odds, then a margin that widens under uncertainty.
4. **RAG explanation layer** — turns the number into a grounded,
   human-readable explanation, never hallucinating a statistic that
   isn't in the retrieved data.
5. **MCP server** — exposes all of the above as typed tools any
   agent (including Claude itself) can call, without needing to
   know anything about Kafka or Cassandra underneath.
6. **Agentic layer** — specialised agents, each doing one job,
   coordinated by a single orchestrator. Built last, deliberately —
   agentic coordination is only meaningful once there is something
   real underneath for the agents to coordinate around.
