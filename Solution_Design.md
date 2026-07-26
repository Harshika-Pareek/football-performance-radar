# SportsPulse — Architecture Diagrams

These diagrams follow the C4 model (Context → Container → Component).
They render automatically in GitHub and can be copied into Lucidchart/draw.io.

---

## Level 1: System Context Diagram

Shows SportsPulse and everything it connects to.

```mermaid
graph TB
    User(["👤 End User\n(Sports fan, analyst,\nfantasy player)"])
    Agent(["🤖 LLM Agent\n(Claude, custom agents\nvia MCP protocol)"])
    
    subgraph SportsPulse["🏟️ SportsPulse Platform"]
        SP["AI Sports Intelligence\nPlatform"]
    end
    
    FootballAPI["⚽ football-data.org\nPremier League data"]
    NBAAPI["🏀 nba_api\nNBA statistics"]
    CricketAPI["🏏 Cricsheet\nCricket data"]
    TennisAPI["🎾 Tennis Abstract\nATP/WTA data"]
    F1API["🏎️ Ergast API\nFormula 1 data"]
    NewsRSS["📰 RSS Feeds\nBBC Sport, Sky Sports"]
    
    User -->|"Views predictions,\nasks questions"| SP
    Agent -->|"Calls tools via\nMCP protocol"| SP
    
    FootballAPI -->|"Match results,\nfixtures, stats"| SP
    NBAAPI -->|"Game stats,\nplayer data"| SP
    CricketAPI -->|"Ball-by-ball\ndata"| SP
    TennisAPI -->|"Match results,\nElo ratings"| SP
    F1API -->|"Race results,\nlap times"| SP
    NewsRSS -->|"Injury news,\nlineup updates"| SP

    style SportsPulse fill:#1a4a8a,color:#fff
    style SP fill:#0f3460,color:#fff
```

---

## Level 2: Container Diagram

Shows the major technical containers inside SportsPulse.

```mermaid
graph TB
    subgraph Ingest["📥 Ingest Layer"]
        FP["Football\nProducer\nPython"]
        BP["Basketball\nProducer\nPython"]
        CP["Cricket\nProducer\nPython"]
        TP["Tennis\nProducer\nPython"]
    end

    subgraph Streaming["⚡ Streaming Layer"]
        KAFKA["Apache Kafka\nKRaft mode\nMulti-topic"]
        SPARK["Spark Structured\nStreaming\nConsumer"]
    end

    subgraph Storage["💾 Storage Layer"]
        CASS["Cassandra\nOperational store\nTime-series events"]
        MINIO["MinIO\nArtifact store\nS3-compatible"]
        QDRANT["Qdrant\nVector database\nEmbeddings"]
        REDIS["Redis\nOnline feature store\nLow-latency serving"]
    end

    subgraph ML["🧠 ML Layer"]
        FEAST["Feast\nFeature Store\nOffline + Online"]
        MLFLOW["MLflow\nExperiment tracking\nModel registry"]
        INFER["Streaming\nInference\nReal-time predictions"]
    end

    subgraph RAG["💬 RAG + LLM Layer"]
        RAG_PIPE["RAG Pipeline\nHybrid search"]
        OLLAMA["Ollama\nLlama 3.1 8B\nLocal LLM"]
        LANGFUSE["Langfuse\nLLM Observability\nTraces + Evals"]
    end

    subgraph Serving["🌐 Serving Layer"]
        FASTAPI["FastAPI\nREST + WebSocket\nVersioned endpoints"]
        REACT["React\nDashboard\n3 surfaces"]
    end

    subgraph Agentic["🤖 Agentic Layer"]
        MCP["MCP Server\n8 typed tools"]
        ORCH["Agent Orchestrator\nA2A protocol"]
        AGENTS["Specialised Agents\nStats, Prediction,\nNews, RAG, Commentary"]
    end

    FP & BP & CP & TP --> KAFKA
    KAFKA --> SPARK
    SPARK --> CASS
    SPARK --> FEAST
    FEAST --> MLFLOW
    FEAST --> REDIS
    MLFLOW --> INFER
    INFER --> KAFKA
    CASS --> RAG_PIPE
    QDRANT --> RAG_PIPE
    RAG_PIPE --> OLLAMA
    OLLAMA --> LANGFUSE
    MLFLOW --> MINIO
    CASS & REDIS & MLFLOW --> FASTAPI
    RAG_PIPE --> FASTAPI
    FASTAPI --> REACT
    FASTAPI --> MCP
    MCP --> ORCH
    ORCH --> AGENTS

    style KAFKA fill:#e35b1a,color:#fff
    style SPARK fill:#e35b1a,color:#fff
    style MLFLOW fill:#0194e2,color:#fff
    style FASTAPI fill:#059669,color:#fff
    style MCP fill:#7c3aed,color:#fff
```

---

## Level 2: Data Flow Diagram

Shows how data flows through the system end-to-end.

```mermaid
sequenceDiagram
    participant API as Sports API
    participant P as Producer
    participant K as Kafka
    participant S as Spark
    participant C as Cassandra
    participant F as Feast
    participant ML as MLflow
    participant SI as Streaming Inference
    participant KP as Kafka Predictions
    participant FA as FastAPI
    participant U as User

    API->>P: Match events (goals, cards, subs)
    P->>K: Push to football.match.events topic
    K->>S: Spark reads stream (micro-batch)
    S->>C: Write events (idempotent, UUID key)
    S->>F: Write features to offline store
    F->>ML: Trigger model training pipeline
    ML->>SI: Deploy champion model
    K->>SI: Live match events
    SI->>KP: Write predictions to football.predictions
    KP->>FA: FastAPI consumes predictions
    C->>FA: Historical stats on request
    FA->>U: REST API + WebSocket push
```

---

## Level 2: ML Pipeline Diagram

Shows the champion/challenger model lifecycle.

```mermaid
graph LR
    subgraph Training["🏋️ Training Pipeline"]
        FO["Feast\nOffline Store"]
        TRAIN["Train\nChallenger\nModel"]
        EVAL["Automated\nEvaluation\n75% pass rate"]
    end

    subgraph Registry["📋 MLflow Registry"]
        CHAL["Challenger\nModel\n(staging)"]
        CHAMP["Champion\nModel\n(production)"]
    end

    subgraph Serving["⚡ Serving"]
        SI["Streaming\nInference"]
        MONITOR["Model\nMonitor\nDrift detection"]
    end

    FO --> TRAIN
    TRAIN --> EVAL
    EVAL -->|"Pass ✅"| CHAL
    EVAL -->|"Fail ❌"| TRAIN
    CHAL -->|"Beats champion"| CHAMP
    CHAMP --> SI
    SI --> MONITOR
    MONITOR -->|"Drift detected"| TRAIN

    style CHAMP fill:#059669,color:#fff
    style CHAL fill:#d97706,color:#fff
    style EVAL fill:#1a4a8a,color:#fff
```

---

## Level 3: MCP Server Tools

Shows the typed tool interfaces exposed to LLM agents.

```mermaid
graph TB
    subgraph MCP["🔌 MCP Server"]
        T1["get_player\n(player_id, sport)"]
        T2["get_match\n(fixture_id, sport)"]
        T3["get_team\n(team_id, sport)"]
        T4["get_prediction\n(fixture_id, sport)"]
        T5["compare_players\n(player_a, player_b, sport)"]
        T6["get_xg\n(fixture_id)"]
        T7["find_similar_matches\n(fixture_id)"]
        T8["explain_performance\n(player_id, match_id)"]
    end

    subgraph Agents["🤖 A2A Agents"]
        OA["Orchestrator\nAgent"]
        SA["Stats\nAgent"]
        PA["Prediction\nAgent"]
        NA["News\nAgent"]
        RA["RAG\nAgent"]
        CA["Commentary\nAgent"]
    end

    subgraph Data["💾 Data Sources"]
        CASS["Cassandra"]
        MLFLOW["MLflow"]
        QDRANT["Qdrant"]
        NEWS["RSS Feeds"]
    end

    OA --> SA & PA & NA & RA & CA
    SA --> T1 & T2 & T3
    PA --> T4
    RA --> T7 & T8
    CA --> T5 & T6

    T1 & T2 & T3 --> CASS
    T4 --> MLFLOW
    T7 & T8 --> QDRANT
    NA --> NEWS

    style MCP fill:#7c3aed,color:#fff
    style OA fill:#1a4a8a,color:#fff
```

---

## How to use these diagrams

### In your GitHub README
Paste any diagram block directly — GitHub renders Mermaid automatically.

### In Lucidchart / draw.io
1. Go to lucidchart.com or draw.io
2. Insert → Advanced → Edit Diagram (XML)
3. Paste the Mermaid code
4. Export as PNG/PDF for presentations

### In a slide deck
Screenshot the rendered GitHub version or export from draw.io.

### In interviews
When asked "walk me through your architecture" — open your GitHub README
and walk through the diagrams top to bottom: Context → Container → Data Flow → ML Pipeline → MCP.
Each diagram answers a different level of "how does it work?"
