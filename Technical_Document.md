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

Day 3 — PySpark Setup on Windows + Spark Consumer

What we were trying to do

Install PySpark locally and write a Spark Structured Streaming consumer
that reads from the Kafka topic and writes parsed events to Cassandra.


Concept: Why local PySpark over Docker for Spark

With only 8GB RAM running Kafka + Cassandra + kafka-ui already, adding
a Spark Docker container would have pushed the machine over the edge.
Running PySpark locally (installed via pip into the venv) uses less
overhead than a full container, is faster to iterate on, and produces
clearer error messages during development.

The tradeoff: local PySpark requires more manual environment setup
(JAVA_HOME, HADOOP_HOME, winutils) that a container would handle
automatically. Worth it for a dev machine; in production you'd
containerise or use a managed Spark service (EMR, Databricks).


Concept: PySpark is a Python wrapper around a JVM

PySpark isn't a pure Python implementation of Spark — it's a thin
Python API that communicates with a real Java/Scala Spark process
running underneath. When you call SparkSession.builder.getOrCreate(),
Python launches a Java process (the "gateway") and communicates with it
via a socket connection (py4j library).

This means:


Java must be installed and reachable
JAVA_HOME must point to the Java installation directory (not the
executable itself)
The path to Java must be parseable by the subprocess launching it


All three of these caused problems today.


Bug: "The system cannot find the path specified" — three root causes

This error appeared repeatedly but had three distinct causes, each
requiring a different fix. Working through them in order:

Cause 1: HADOOP_HOME not set
PySpark on Windows requires winutils.exe and hadoop.dll to handle
filesystem operations (even when not using HDFS at all). Without
HADOOP_HOME pointing to a folder containing bin/winutils.exe,
Spark fails immediately on startup.

Fix: download winutils.exe and hadoop.dll from
github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin and place
them in C:\hadoop\bin\, then set:

powershell$env:HADOOP_HOME = "C:\hadoop"

Cause 2: JAVA_HOME set to the executable, not the directory
The existing JAVA_HOME was set to:
C:\Program Files\Common Files\Oracle\Java\javapath\java.exe

This is wrong — JAVA_HOME must point to the installation directory,
not the java.exe file itself. Spark constructs the Java command as
%JAVA_HOME%\bin\java — if JAVA_HOME already ends in java.exe,
this becomes an invalid path.

Diagnosis:

powershell& "path\to\java.exe" -XshowSettings:all 2>&1 | Select-String "java.home"
# prints: java.home = C:\Program Files\Java\jdk-17

Fix:

powershell$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"

Cause 3: Space in username path breaks PySpark's subprocess call
Even with correct JAVA_HOME and HADOOP_HOME, launch_gateway kept
failing. The diagnostic block confirmed Java itself worked fine (exit
code 0, correct version) — so the issue was in how PySpark built its
internal command string.

Root cause: the Windows username Whiskey golden contains a space.
PySpark's launch_gateway builds a command list internally using the
path to spark-submit, which passes through the path
C:\Users\Whiskey golden\... — the space causes CreateProcess to
misparse the command, hence FileNotFoundError [WinError 2].

Fix: create a symlink from a no-space path to the project:

powershell# Run as Administrator
New-Item -ItemType Directory -Path "C:\projects" -Force
New-Item -ItemType SymbolicLink -Path "C:\projects\radar" -Target "C:\Users\Whiskey golden\Downloads\football-performance-radar"

Then always run Spark from C:\projects\radar\spark\ instead of the
original path. From Spark's perspective, the path is clean with no
spaces anywhere.

Lesson: on Windows, spaces in usernames/paths cause subtle,
hard-to-diagnose failures in JVM-based tools. Creating a symlink to a
clean path is the most reliable fix — don't try to quote or escape your
way around it in subprocess calls.


Concept: Environment variables in PowerShell are session-scoped

Setting $env:JAVA_HOME = "..." in PowerShell only applies to that
terminal session. Open a new terminal window and it's gone. This caused
confusion repeatedly today — the variable was set in one window but
the consumer was run from another.

Two ways to make it permanent:


Set via Windows UI: Win key → "Edit environment variables for your
account" → New → add name/value → OK
Set via PowerShell (requires restart to take effect in new sessions):


powershell   [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-17", "Machine")

For now: always set the three variables at the start of each session
before running Spark:

powershell$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_LOCAL_DIRS = "C:\tmp\spark"


Concept: Spark Structured Streaming — the three things that matter

1. readStream vs read
Normal Spark: spark.read.csv(...) — reads once, finishes.
Streaming Spark: spark.readStream.format("kafka")... — never finishes,
processes new messages continuously in micro-batches.

2. Schema on read
Kafka stores messages as raw bytes. Spark needs to be told the exact
shape of the JSON inside those bytes at read time — every field name
and type must match what the producer actually sends.

3. Checkpointing
Spark writes its Kafka offset progress to a folder after every
micro-batch. On restart, it reads the checkpoint and picks up exactly
where it left off — no events reprocessed, none skipped.


Concept: RAM management for JVM-heavy local dev

Running Kafka + Cassandra + PySpark on 8GB RAM is tight. The rule:
never run all three simultaneously unless you have to.


Developing Kafka producer: only Kafka needs to be running
Developing Spark consumer: Kafka + Spark (stop Cassandra until
ready to test writes)
Testing full pipeline: all three, but with memory caps on everything


Memory caps applied to this project:

yaml# Cassandra (Docker Compose)
MAX_HEAP_SIZE: "512M"
HEAP_NEWSIZE: "128M"
mem_limit: 1g

# WSL2 (.wslconfig in user home)
[wsl2]
memory=2GB
processors=2
swap=1GB

# Spark (SparkSession config)
spark.driver.memory: 512m
spark.executor.memory: 512m
spark.ui.enabled: false  # saves ~100MB by disabling the web UI
spark.sql.shuffle.partitions: 2  # reduces parallelism overhead


Concept: UDF (User Defined Function) in Spark

A UDF lets you apply a plain Python function to every row in a Spark
DataFrame. Used in the consumer to generate a UUID per event:

pythonfrom pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

df = df.withColumn("event_id", generate_uuid())

The StringType() argument tells Spark what type this function returns
— Spark needs this at compile time to build its execution plan.

Performance note: UDFs are slower than built-in Spark functions
because they break out of the JVM into Python for each row. For
production at scale, expr("uuid()") (Spark's native UUID function)
is faster. For a learning project, UDF is fine and teaches the pattern.


What's next — completing Layer 2

Spark consumer is written and ready. Tomorrow:


Free RAM (stop cassandra + kafka-ui before starting)
Set env vars in the terminal session
Navigate via symlink: cd C:\projects\radar\spark
Run: python consumer.py
Watch Maven download the Kafka + Cassandra JARs (first run, 2-3 min)
Verify events land in Cassandra: docker exec -it cassandra cqlsh
then SELECT * FROM football_radar.match_events;


Once data is in Cassandra, Layer 2 is complete.


Commands reference — Spark session startup sequence

powershell# Run every session before starting Spark
docker stop cassandra
docker stop kafka-ui
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_LOCAL_DIRS = "C:\tmp\spark"
cd C:\projects\radar\spark
C:\projects\radar\venv\Scripts\Activate.ps1
python consumer.py


Spark Consumer: Docker Setup and RAM Constraints

What we were trying to do

Submit the Spark Structured Streaming consumer to run inside Docker,
reading from Kafka and writing to Cassandra.


Concept: Spark local mode vs cluster mode

Local mode (--master local[2]): Spark runs entirely in one JVM
process — driver and executor are the same process. No separate worker
needed. Good for development and testing on a single machine.

Cluster mode (--master spark://host:port): Spark separates the
driver (coordinates the job) from workers (execute the tasks). Requires
at least one registered worker node before any job can run. This is
what production Spark clusters look like.

We attempted cluster mode (master + worker) but hit RAM constraints
on an 8GB machine. The consumer code itself is correct and proven —
"Writing batch 0 — 21 rows" printed successfully, confirming Spark
read and parsed all 21 Kafka events correctly.


Concept: Spark master vs worker

Master — the cluster coordinator. Knows which workers exist, how
many resources they have, and assigns tasks to them. Runs the Spark UI
(port 4040). Does NOT execute any actual data processing itself.

Worker — registers with the master and executes actual tasks
(reading from Kafka, processing rows, writing to Cassandra). A job
submitted to the master sits waiting ("Initial job has not accepted
any resources") until at least one worker is registered.

This is why --master local[2] works without a worker: in local mode,
the driver IS the worker — no separate process needed.


Bug: bitnami/spark image no longer publicly available

Symptom: docker.io/bitnami/spark:3.5.1: not found

Root cause: Bitnami moved their Spark image to a paid "Bitnami
Secure Images" tier — the free public image was removed from Docker Hub.

Fix: use the official Apache Spark image instead:

yamlimage: apache/spark:3.5.1

This is maintained by the Apache Software Foundation, genuinely free,
no account or payment needed.


Concept: Spark submit packages vs SparkSession config

There are two ways to load extra JARs (like the Kafka and Cassandra
connectors) into a Spark job:

Via SparkSession config (what we had initially):

python.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka...")

Works fine for local/embedded mode. Less clean for cluster submission.

Via spark-submit --packages (correct approach for cluster mode):

bashspark-submit --packages "org.apache.spark:spark-sql-kafka..." app.py

This downloads JARs before the job starts, makes them available to
all workers. The preferred approach when submitting to a cluster.

When using --packages on the command line, remove the
spark.jars.packages config from SparkSession to avoid duplication.


Concept: Docker internal network addresses

When running inside Docker, containers reach each other via their
container name, not localhost. Key changes needed for Docker mode:

ConfigLocal (Windows)Docker containerKafka bootstraplocalhost:9092kafka:29092Cassandra hostlocalhostcassandra

kafka:29092 specifically because: 29092 is the internal listener
port (for Docker-internal clients), and 9092 is the external listener
(for Windows-host clients). Using 9092 from inside Docker would try
to connect to the wrong advertised address.


Concept: RAM requirements for this stack

Running the full stack simultaneously on 8GB RAM is genuinely too tight:

ComponentRAM usageDocker Desktop + WSL2~1.5GBWindows + VS Code~2GBKafka container~300MBCassandra container (capped)~1GBSpark master container~900MBSpark worker container~600MBTotal~6.3GB — leaves ~1.7GB for OS paging

At this utilisation level, Docker commands themselves start freezing
because there's insufficient RAM for the Docker daemon to respond.

Fix: upgrade to 16GB RAM. With 16GB, each component gets proper
headroom and the whole stack runs without any memory pressure.

Lesson for production design: always account for infrastructure
overhead when sizing machines. A "512MB Spark executor" still needs
~900MB total RAM for the container, JVM startup overhead, and OS buffers.
