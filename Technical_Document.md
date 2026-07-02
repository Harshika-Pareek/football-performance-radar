# Technical Learning Journal — Football Performance Radar

A running document capturing concepts learned, problems debugged, and
decisions made during the build. Written to be useful as a reference
during interviews and as a personal knowledge base.

---

##  Environment Setup: Docker, WSL2, Kafka

### What we were trying to do
Get a Kafka cluster running locally on Windows using Docker Compose,
with a Python producer pushing live sports data into a topic.

---

### Concept: Why Docker exists

Docker solves the "works on my machine" problem. A Docker **image** is
a frozen, self-contained snapshot of an entire runtime environment —
the OS files, the software, the dependencies, all bundled together.
A **container** is a running instance of that image.

Kafka, Spark, and Cassandra are Linux-native applications. Docker lets
you run them on Windows without installing Java, configuring paths, or
managing processes manually — the container handles all of that.

**Mental model:** an image is a recipe + pre-measured ingredients sealed
in a kit. A container is the actual meal you cook from that kit. You
can cook the same kit (image) many times, getting identical results
every time, regardless of which machine you're using.

---

### Concept: WSL2 — why Docker needs it on Windows

Containers rely on Linux kernel features (namespaces, cgroups) to
isolate processes. Windows doesn't have these natively. WSL2 (Windows
Subsystem for Linux) provides a real, lightweight Linux VM underneath
Windows that Docker Desktop uses as its execution backend.

**No WSL2 = no Linux kernel = Docker Desktop engine can't start.**

This was the root cause of the first error:
```
failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine
```

**Fix:** install WSL2 (`wsl --install`), restart, then Docker Desktop
can find its backend and the named pipe exists.

**Important distinction:** `docker --version` working only confirms
the Docker *client* is installed. The client talks to a separate
background process (the Docker *daemon*). Client installed ≠ daemon
running.

---

### Bug: Stale `DOCKER_HOST` environment variable

**Symptom:** `docker run hello-world` times out with
`dial tcp 192.168.99.101:2376: i/o timeout`

**Root cause:** an old Docker Toolbox installation had set a
`DOCKER_HOST` environment variable pointing at a VirtualBox VM
that no longer existed. This overrides Docker Desktop's default
daemon address silently.

**How to diagnose:**
```powershell
echo $env:DOCKER_HOST
# if this prints tcp://192.168.99.101:2376 — that's the problem
```

**Fix:** delete it permanently via Windows environment variable
settings (not just the terminal session), then restart the terminal.

**Pattern to remember:** "command fails mysteriously → check environment
variables → check for leftovers from old installs." This applies
constantly in engineering.

---

### Concept: Docker Compose

Docker Compose is a tool for defining and running multi-container
applications. Instead of running `docker run` commands manually for
each service, you define all services in a `docker-compose.yml` file
and start everything with one command: `docker compose up -d`.

The `-d` flag means "detached" — run in the background and give the
terminal prompt back immediately.

---

### Concept: Kafka — the core ideas

**The problem Kafka solves:**
If your producer and consumer are directly connected, they must run
at the same speed at the same time. If the consumer is slow or
crashes, data is lost. If you want two consumers reading the same
data, the producer has to know about both.

Kafka solves this by sitting in the middle as a **durable, persistent
log** — producers write to Kafka and forget about it; consumers read
from Kafka at their own pace; nothing is lost if a consumer is
temporarily down.

**Four core concepts:**

| Concept | What it is |
|---|---|
| **Topic** | A named, append-only log. Like a logbook where events are always added to the end. Messages are NOT deleted when read. |
| **Partition** | A topic is split into partitions for parallelism. Order is guaranteed WITHIN a partition, not across the whole topic. |
| **Offset** | Each consumer's bookmark — "I've read up to message #347." Enables replay: start at offset 0 to reprocess all history. |
| **Broker** | The Kafka server process that stores partitions and serves reads/writes. |

**Why partition key matters:**
Kafka routes messages to partitions based on the message key. Using
`fixture_id` as the key means all events for one match land on the
same partition — guaranteeing they're processed in minute order, even
if multiple matches are being ingested simultaneously.

---

### Concept: KRaft mode (no Zookeeper)

Old Kafka needed a separate system called **Zookeeper** to manage
cluster metadata (who's the leader, where partitions live). This was
operationally painful — two systems to maintain instead of one.

**KRaft mode** (introduced in Kafka 2.8, stable in 3.x) removes this
dependency. Kafka manages its own metadata using a built-in Raft
consensus protocol. A subset of brokers act as "controllers"
(managing metadata) alongside their normal broker role (storing data).

Since you're running a single broker, it plays both roles:
`KAFKA_PROCESS_ROLES: broker,controller`

---

### Bug: Invalid `CLUSTER_ID`

**Symptom:** Kafka container starts then immediately exits with:
```
Cluster ID string does not appear to be a valid UUID:
Input string with prefix is too long to be decoded as a base64 UUID
```

**Root cause:** the `CLUSTER_ID` in the Compose file was a hand-written
string that looked like a base64 UUID but wasn't — Kafka validates
this strictly on startup.

**Fix:** generate a real UUID using Kafka's own tool:
```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```
Use the output directly. Never hand-write a cluster ID.

**Why this matters:** Kafka's startup validation is strict by design —
it's better to fail loudly at boot than silently accept corrupt config
and fail mysteriously later under load.

---

### Concept: Kafka Listeners — the most confusing part

**The problem:** Kafka tells every connecting client "here's my address
for future requests" (the "advertised" address). But different clients
live in different network contexts:

- Your Python script runs on **Windows** — it reaches Kafka via
  `localhost:9092` (Docker maps this port through)
- The `kafka-ui` container runs **inside Docker** — it can only reach
  Kafka via `kafka:29092` (the internal Docker network name)

If Kafka advertises `localhost:9092` to everyone, kafka-ui tries to
connect to `localhost` inside its own container — which points to
kafka-ui itself, not Kafka.

**The fix: two separate listeners**

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
```

- Clients connecting via port 9092 (your Python scripts) get told:
  use `localhost:9092`
- Clients connecting via port 29092 (kafka-ui, other containers) get
  told: use `kafka:29092`

**This is standard production Kafka config** — not a Docker workaround.
In production, you'd have internal listeners for services in the same
VPC and external listeners for clients outside the network, each
advertising the correct address for their audience.

---

### Bug: kafka-python incompatible with Python 3.13

**Symptom:** `from kafka import KafkaProducer` raises
`ModuleNotFoundError` or a long traceback about internal Python
modules.

**Root cause:** `kafka-python` 2.0.2 uses internal Python APIs that
changed in Python 3.12+. The package hasn't been updated to handle
this.

**Fix:** use the community-maintained fork:
```bash
pip uninstall kafka-python -y
pip install kafka-python-ng
```
Same import name (`from kafka import KafkaProducer`) — drop-in
replacement with no code changes needed.

**Lesson:** always check if a package is actively maintained before
depending on it. `kafka-python` has been mostly inactive; `kafka-python-ng`
is the maintained fork for modern Python.

---

### Concept: Virtual Environments (venv)

A virtual environment is an isolated, self-contained Python installation
with its own packages — completely separate from any other Python
environment on the machine.

**Why this matters:** if you install `kafka-python-ng` into Project A's
venv, it's invisible to Project B's venv. This prevents the classic
"it works on my machine" dependency conflict where two projects need
different versions of the same package.

**The debugging pattern that matters:**
```bash
pip show kafka-python-ng
# Always check the Location: field
# If it points to a DIFFERENT project's venv — wrong environment
```

**Activation:** always confirm `(venv)` appears in your terminal prompt
before running any Python or pip commands. If it's missing, activate:
```bash
.\venv\Scripts\Activate.ps1   # Windows PowerShell
```

---

### Concept: Replay Simulator

**The problem:** API-Football's free tier restricts fixture queries to
2022-2024. The live 2026 World Cup data exists but isn't accessible
on the free plan.

**The solution: a replay simulator** — reads the complete event timeline
for a historical match (England 6-2 Iran, 2022 World Cup) and pushes
events into Kafka paced out over time, simulating what a live feed
would look like.

**Why this is legitimate:** Kafka has no way to distinguish "real live
data" from "historical data replayed at a realistic pace." From Kafka's
perspective, messages just arrive over time. This is a standard
technique used by real engineering teams to test streaming pipelines
before live data sources are available.

**The pacing logic:**
```python
minutes_elapsed = current_minute - previous_minute
wait_seconds = max(minutes_elapsed, 1) * SECONDS_PER_MATCH_MINUTE
time.sleep(wait_seconds)
```
Events far apart in match time (minute 25 → minute 35) wait longer
between sends than events close together (minute 70 → minute 71) —
preserving the real rhythm of the match, not just firing at uniform
intervals.

---

## Cassandra: Schema Design and Data Modeling

### What we were trying to do
Add Cassandra as the storage layer for match events. Design a schema
that correctly handles the real structure of the data.

---

### Concept: Why Cassandra over Postgres for this workload

This is one of the most important architectural decisions to be able
to justify in an interview.

**The question to ask first:** what queries will you actually run?

For match events:
- "Give me all events for match 855735, in time order" ✓
- "Give me all Goal events for England" ✓
- "Join match events with player transfer history" ✗ — never needed

**No joins** is the critical observation. Cassandra explicitly gives up
join capability in exchange for:

- **Extreme write throughput** — append-only writes with no locking,
  no B-tree rebalancing, no transaction overhead
- **Linear scalability** — add nodes, get proportionally more throughput
- **Built-in time-series ordering** — clustering keys physically order
  rows on disk by the column you specify

**When Cassandra is the wrong choice:**
- You need joins across tables
- You need ad-hoc analytical queries (GROUP BY, aggregations across
  many partitions)
- You don't know your query patterns upfront

**The one-sentence interview answer:**
"Match events are write-heavy, time-ordered, and queried by match or
player — never joined. Cassandra's wide-column model with clustering
keys physically orders events by minute within each match partition,
giving single-partition reads for every query we'd actually run."

---

### Concept: Cassandra Data Modeling

**The fundamental rule:** design tables around your queries, not around
your data's natural shape. The opposite of Postgres.

**Primary key anatomy:**

```
PRIMARY KEY (partition_key, clustering_key)
```

| Part | Purpose |
|---|---|
| **Partition key** | Determines which node stores this data. All rows with the same partition key are stored together — retrieved in one read. |
| **Clustering key** | Determines sort order within a partition. Physically orders rows on disk. |

**For your match_events table:**
- Partition key: `fixture_id` — all events for one match stored together
- Clustering key: `minute` — events ordered chronologically within a match

```sql
PRIMARY KEY (fixture_id, minute, event_id)
```

---

### Bug: Silent Upsert — the most dangerous Cassandra gotcha

**What happened:** inserted two events both at minute 46. SELECT
returned only one row — Player A's data was silently overwritten by
Player B's with no error, no warning.

**Root cause:** Cassandra's INSERT is actually an **upsert**. If a row
with the same primary key already exists, it overwrites it silently.
This is by design — Cassandra trades conflict detection for write speed.

**Why this is dangerous in production:** event counts would be wrong
for weeks before anyone noticed. The England-Iran match had THREE
substitutions at minute 46 — a schema with `PRIMARY KEY (fixture_id,
minute)` would store only one of them.

**The fix:** add a UUID as a third component of the primary key:

```sql
PRIMARY KEY (fixture_id, minute, event_id)
```

Where `event_id` is a `uuid()` generated per event. Now multiple events
at the same minute coexist as separate rows, each uniquely identified.

**This is one of the most common production bugs in Cassandra systems**
— teams discover it weeks after launch when event counts don't match
expectations.

---

### Concept: UUID — what it is and why distributed systems use it

**UUID** (Universally Unique Identifier) is a 128-bit number displayed
as 32 hex characters in 5 groups:
```
d0b379f5-d545-4181-aaba-f4c1974519b2
```

**Why not just use an auto-incrementing integer?**
In a single-machine database, auto-increment works fine. In a
distributed system with multiple nodes accepting writes simultaneously,
nodes would have to coordinate on every write just to agree on the
next number — destroying write performance.

UUID solves this by removing coordination entirely. Each node generates
UUIDs independently, with statistical certainty of no collisions, even
across millions of nodes generating millions of UUIDs per second.

**UUID versions:**
- **v4 (random):** 122 bits of pure randomness. Used in your schema
  via `uuid()` in cqlsh or `uuid.uuid4()` in Python. Guaranteed unique,
  but sorts randomly.
- **v1 / TIMEUUID (time-based):** encodes a timestamp into the UUID.
  Sorts chronologically — UUIDs generated later always sort after
  earlier ones. Useful when you want time-ordering within a partition.

**In Python:**
```python
import uuid
event_id = str(uuid.uuid4())
# "d0b379f5-d545-4181-aaba-f4c1974519b2"
```

---

### Concept: Cassandra Keyspace

A **keyspace** is Cassandra's top-level namespace — equivalent to a
database in Postgres. The most important setting:

```sql
CREATE KEYSPACE IF NOT EXISTS football_radar
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
```

**Replication factor** = how many copies of each row to keep across
nodes. Locally: must be 1 (only one node). In production with 3 nodes:
set to 3 so if one node dies, two others still have the data.

---

### Concept: Cassandra Memory on a Laptop

Cassandra is designed for dedicated servers with large RAM. By default
it tries to allocate 2-4GB JVM heap. On a laptop this causes severe
slowdowns.

**Fix: set explicit memory limits in Docker Compose:**
```yaml
environment:
  MAX_HEAP_SIZE: "512M"
  HEAP_NEWSIZE: "128M"
mem_limit: 1g
```

This caps Cassandra's JVM heap at 512MB and total container memory at
1GB — usable on a laptop without freezing it.

**Lesson:** always set memory limits on JVM-based containers (Kafka,
Cassandra, Spark) when running locally. Their defaults assume dedicated
hardware.

---

### What's in the topic right now

**Topic:** `worldcup_match_events`
**Match:** England 6-2 Iran (fixture_id: 855735, 2022 World Cup)
**Messages:** 21 events (goals, cards, substitutions, VAR)

Notable events that tested the schema:
- Minute 46: 3 simultaneous substitutions — caught and fixed the
  silent upsert bug, added UUID to primary key
- Minute 90+: VAR review + 2 late goals — shows full range of
  event types the system needs to handle

---

## Decisions Log

| Decision | Chosen | Alternative considered | Reason |
|---|---|---|---|
| Kafka mode | KRaft (no Zookeeper) | Classic Zookeeper mode | Simpler ops, modern standard, one less container |
| Kafka listeners | Two listeners (9092 external, 29092 internal) | Single listener | Single listener breaks kafka-ui inside Docker |
| Kafka Python client | kafka-python-ng | kafka-python | kafka-python broken on Python 3.13 |
| Storage | Cassandra | Postgres | Write-heavy, time-series, no joins — Cassandra's sweet spot |
| Cassandra primary key | (fixture_id, minute, event_id) | (fixture_id, minute) | Minute alone not unique — silent overwrites discovered in testing |
| UUID type | UUID v4 (random) | TIMEUUID (time-based) | event_id only needs uniqueness, not time ordering — minute already handles ordering |
| Memory limits | 512MB heap, 1GB total | Default (2-4GB) | Laptop usability — defaults assume dedicated hardware |
| Data source | 2022 World Cup (replay) | 2026 live data | Free API tier restricts 2026 season; replay is standard testing technique |

---

## Commands Reference

### Docker
```bash
docker compose up -d          # start all services in background
docker compose down           # stop and remove containers
docker compose down -v        # stop and remove containers + volumes
docker compose ps             # check status of all services
docker logs <container>       # see what a container printed before dying
docker ps                     # list all running containers
docker ps -a                  # list all containers including stopped
```

### Kafka
```bash
# Generate a valid Kafka cluster ID
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```

### Cassandra (cqlsh)
```bash
# Enter Cassandra shell
docker exec -it cassandra cqlsh

# Inside cqlsh:
DESCRIBE keyspaces;
USE football_radar;
DESCRIBE TABLE match_events;
SELECT * FROM match_events WHERE fixture_id = 855735;
DELETE FROM match_events WHERE fixture_id = 855735;
```

### Python / venv
```bash
# Create venv
python -m venv venv

# Activate (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Verify correct environment
pip show kafka-python-ng   # check Location: field points to THIS project

# Run scripts
python producer/replay_producer.py
python producer/check_api_key.py
```

### Git
```bash
git checkout -b feature/branch-name     # create feature branch
git add .                               # stage all changes
git commit -m "message"                 # commit
git push --set-upstream origin branch   # first push of new branch
git push                                # subsequent pushes
git pull                                # pull remote changes before pushing
```

---

## What's Next — Layer 2 (Spark Structured Streaming)

**The goal:** write a Spark job that reads from `worldcup_match_events`
and writes processed events into the Cassandra `match_events` table.

**Three concepts to understand before the code:**

1. **readStream vs read** — `spark.readStream` creates an unbounded
   DataFrame that keeps growing as new Kafka messages arrive, processed
   in micro-batches.

2. **Schema on read** — Kafka stores everything as raw bytes. Spark
   needs to be told "this binary value is JSON with this shape" at
   read time.

3. **Checkpointing** — Spark writes its Kafka offset progress to a
   folder. On restart, it picks up exactly where it left off — no
   events reprocessed, none skipped.

**The code shape:**
```python
# 1. Read from Kafka as a stream
raw_stream = spark.readStream.format("kafka")...

# 2. Parse JSON, extract fields, generate UUID per event
parsed = raw_stream.select(from_json(...), uuid(), ...)

# 3. Write to Cassandra continuously
parsed.writeStream.format("org.apache.spark.sql.cassandra")...
```
