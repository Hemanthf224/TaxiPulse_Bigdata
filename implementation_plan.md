# TaxiPulse — Implementation Plan

**Real-Time Taxi Demand Forecasting with Fare and ETA Prediction**

Course: 23AID302 – Big Data Analytics | Group-6 | AY 2026–27
Team: Pranav Kumar Reddy (CB.AI.U4AID24069), Tharun Kumar Reddy (CB.AI.U4AID24033), Rajdeep K (CB.AI.U4AID24022), M Hemanth Reddy (CB.AI.U4AID24066)

---

## 0. How to use this document

This is a complete, self-contained build plan for an AI coding agent. **Everything needed is inside this file** — no other document, PDF, or folder is required. It is written to be run on a fresh laptop with nothing installed but Docker, Git, and an IDE.

Work **phase by phase, in order**. Phase 1 is graded on a fixed date and everything else depends on it.

Each task has a **Definition of Done (DoD)**. A task is not finished until its DoD is literally satisfied and verified by running something. Do not mark work complete because "the code looks right".

**Create the repository at:** `<your-workspace>/TaxiPulse/`

---

## 1. What the First Review actually demands

These are the graded requirements. Everything in Phase 1 maps to one of them.

**Part A — Presentation, exactly these 13 sections:**

1. Title Slide — Project Title, Team Members (names & roll numbers), **Faculty In-charge**, Department, College, Academic Year
2. Abstract, Introduction / Background
3. Problem Statement
4. Objectives
5. Literature Survey
6. Proposed System / Methodology
7. Implementation Details
8. Results & Analysis
9. Challenges / Limitations
10. Big data tools used, and **impact comparison between big data tools and conventional data processing techniques**
11. Conclusion & Future Work
12. Conference paper status
13. References

**Part B — Live demonstration during the review. Each team must be prepared to demonstrate:**

| # | Requirement | Delivered by |
|---|---|---|
| 1 | Hadoop cluster with **minimum 2 DataNodes** | P0.2 (we run 3) |
| 2 | **Raw dataset in HDFS** | P1.2 |
| 3 | **Scala preprocessing code** | P1.3 |
| 4 | Preprocessing operations performed | P1.3 + P1.4 |
| 5 | Processed dataset | P1.3 |
| 6 | **Processed dataset stored back in HDFS** | P1.3 |
| 7 | **Evidence / screenshots of HDFS storage and execution** | P1.6 |

> Item 7 is highlighted in the review instructions. Screenshots are not optional decoration — they are a graded deliverable. Capture them continuously, not at the end.

Two things follow that shape the whole build: the preprocessing **must be written in Scala**, and the cluster **must be genuinely multi-node**. Do not substitute PySpark for the preprocessing job.

---

## 2. Non-negotiable rules for the agent

1. **Comment code generously.** Every non-trivial function gets a docstring/ScalaDoc saying what it does and why. Every cleaning rule carries an inline comment naming the real-world reason for it (e.g. `// fare_amount <= 0 means a voided or refunded trip, not a real ride`). A student has to defend every line of this in a viva.
2. **Notebooks must be presentation-grade.** A markdown heading before every code cell explaining the step in plain words first, then the technical detail. Numbered sections. No dead cells, no `Untitled`, no leftover debug prints.
3. **Explain in simple words first.** Any README, report text, or notebook markdown you write leads with a plain-language sentence or analogy, *then* the technical version.
4. **Never sample the data silently.** This project's academic claim is full-scale processing. If you need a small subset for a fast dev loop, put it behind an explicit `--sample` flag defaulting to OFF, printing a loud warning when on.
5. **No data leakage in the models.** See Appendix B — the single most likely way this project gets torn apart in a review. Read it before writing any ML code.
6. **Capture evidence as you go**, into `docs/evidence/<phase>/`.
7. **Everything must be re-runnable.** No manual step that exists only in someone's memory. If a human must click something, write it into `docs/runbook.md`.
8. **Commit after every completed task**, message naming the phase: `[P1.3] Scala cleaning job writes partitioned Parquet to HDFS`.

---

## 3. Environment and versions

| Component | Version | Why pinned |
|---|---|---|
| Docker Desktop (WSL2 backend) | latest | Runs the whole cluster |
| Hadoop | 3.2.1 (`bde2020` images) | Matches the course lab environment |
| Spark | 3.5.x, **Scala 2.12** build | Must match Hadoop 3.x; a Scala 2.13 build will break |
| Scala | 2.12.18 | Spark 3.5 default |
| sbt | 1.9.x | Scala build tool |
| Python | 3.10 | PySpark 3.5 compatibility |
| Kafka | 3.9.x (KRaft mode, no ZooKeeper) | Avoids a ZooKeeper container |
| Streamlit | latest | Dashboard |

**Language split required by the course:** Scala for the core data pipeline (ingest, clean, enrich, feature build); Python/PySpark for ML, streaming, and dashboard. Both must genuinely be used.

**Machine requirements:** 16 GB RAM strongly preferred (8 GB minimum, expect slow runs), 4+ cores, and **at least 70 GB of free disk** on the drive Docker uses.

---

## 4. Target repository structure

Create this in Phase 0 and keep it. Do not invent a different layout.

```
TaxiPulse/
├── README.md                       <- this file
├── docker/
│   ├── docker-compose.yml          <- full contents given in P0.2
│   └── spark-client/Dockerfile
├── data/                           <- local staging only, git-ignored, never committed
│   ├── raw/
│   └── reference/
├── scripts/
│   ├── 01_download_data.py
│   ├── 02_download_weather.py
│   ├── 03_upload_to_hdfs.ps1
│   └── 99_capture_evidence.ps1
├── scala-pipeline/                 <- PHASE 1 CORE DELIVERABLE
│   ├── build.sbt
│   └── src/main/scala/com/taxipulse/
│       ├── Config.scala
│       ├── SchemaUnifier.scala
│       ├── DataCleaner.scala
│       ├── ZoneEnricher.scala
│       ├── WeatherJoiner.scala
│       ├── FeatureBuilder.scala
│       ├── QualityReport.scala
│       └── PreprocessJob.scala      <- main entry point
├── mapreduce/
│   ├── mapper.py
│   ├── reducer.py
│   └── run_job.ps1
├── ml/
│   ├── notebooks/
│   │   ├── 01_eda.ipynb
│   │   ├── 02_demand_model.ipynb
│   │   ├── 03_fare_eta_model.ipynb
│   │   └── 04_zone_clustering.ipynb
│   └── src/
│       ├── demand_model.py
│       ├── fare_model.py
│       ├── eta_model.py
│       ├── zone_clustering.py
│       └── evaluate.py
├── streaming/
│   ├── kafka_producer.py
│   └── spark_streaming_job.py
├── dashboard/
│   └── app.py
├── benchmarks/
│   ├── run_benchmarks.py
│   └── pandas_vs_spark.py
├── models/                          <- saved Spark ML models, git-ignored
├── docs/
│   ├── runbook.md
│   ├── data_quality_report.md
│   ├── evidence/                    <- screenshots per phase
│   └── review1/                     <- PPT + demo script
└── .gitignore
```

---

## PHASE 0 — Environment setup (blocker for everything)

### ⚠️ P0.1 — Verify disk space before downloading anything

Docker Desktop on Windows stores all container and volume data in a WSL2 virtual disk **on the C: drive by default**. This project needs roughly:

| Item | Size |
|---|---|
| Raw dataset, local staging | ~23 GB |
| Same data in HDFS at replication 2 | ~46 GB |
| Processed Parquet output | ~15 GB |
| Container images and overhead | ~10 GB |
| **Working total** | **~70–90 GB** |

Check free space first: `Get-PSDrive -PSProvider FileSystem`.

If the Docker drive has under 90 GB free, relocate Docker's storage to a larger drive **before** downloading data: Docker Desktop → Settings → Resources → **Disk image location** → point to the larger drive → Apply & Restart.

If no drive has that much space, reduce scope by dropping FHVHV years (2023–2024 only ≈ 11 GB) and set `HDFS_CONF_dfs_replication: "1"` — but note in the report that the dataset is then ~14 GB, and check whether that still satisfies the course minimum before committing to it.

**DoD:** at least 90 GB free on the drive Docker uses, confirmed by command output pasted into `docs/runbook.md`.

### P0.2 — Bring up the Hadoop cluster

Create `docker/docker-compose.yml` with the following. This is a 3-DataNode Hadoop 3.2.1 cluster plus Spark, sized to satisfy demo requirement #1 with margin.

```yaml
version: "3"

services:

  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    hostname: namenode
    restart: unless-stopped
    ports: ["9870:9870", "9000:9000"]
    volumes: ["namenode:/hadoop/dfs/name"]
    environment: &hdfs_env
      CLUSTER_NAME: taxipulse
      CORE_CONF_fs_defaultFS: hdfs://namenode:9000
      CORE_CONF_hadoop_http_staticuser_user: root
      HDFS_CONF_dfs_webhdfs_enabled: "true"
      HDFS_CONF_dfs_permissions_enabled: "false"
      HDFS_CONF_dfs_replication: "2"
      HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check: "false"
    networks: [hadoop]

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    hostname: datanode1
    restart: unless-stopped
    depends_on: [namenode]
    ports: ["9864:9864"]
    volumes: ["datanode1:/hadoop/dfs/data"]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870"
    networks: [hadoop]

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    hostname: datanode2
    restart: unless-stopped
    depends_on: [namenode]
    ports: ["9865:9864"]
    volumes: ["datanode2:/hadoop/dfs/data"]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870"
    networks: [hadoop]

  datanode3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode3
    hostname: datanode3
    restart: unless-stopped
    depends_on: [namenode]
    ports: ["9866:9864"]
    volumes: ["datanode3:/hadoop/dfs/data"]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870"
    networks: [hadoop]

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    container_name: resourcemanager
    hostname: resourcemanager
    restart: unless-stopped
    depends_on: [namenode, datanode1, datanode2, datanode3]
    ports: ["8088:8088"]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870 datanode1:9864 datanode2:9864 datanode3:9864"
      YARN_CONF_yarn_resourcemanager_hostname: resourcemanager
      MAPRED_CONF_mapreduce_framework_name: yarn
    networks: [hadoop]

  nodemanager:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager
    hostname: nodemanager
    restart: unless-stopped
    depends_on: [resourcemanager]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870 resourcemanager:8088"
      YARN_CONF_yarn_resourcemanager_hostname: resourcemanager
      MAPRED_CONF_mapreduce_framework_name: yarn
    networks: [hadoop]

  historyserver:
    image: bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8
    container_name: historyserver
    hostname: historyserver
    restart: unless-stopped
    depends_on: [resourcemanager]
    ports: ["19888:19888"]
    volumes: ["historyserver:/hadoop/yarn/timeline"]
    environment:
      <<: *hdfs_env
      SERVICE_PRECONDITION: "namenode:9870 resourcemanager:8088"
      YARN_CONF_yarn_resourcemanager_hostname: resourcemanager
      MAPRED_CONF_mapreduce_framework_name: yarn
    networks: [hadoop]

  spark-client:
    build: ./spark-client
    container_name: spark-client
    hostname: spark-client
    depends_on: [namenode, resourcemanager]
    ports: ["4040:4040", "8501:8501"]
    volumes: ["../:/workspace"]
    environment:
      <<: *hdfs_env
      HADOOP_CONF_DIR: /etc/hadoop/conf
    command: tail -f /dev/null
    networks: [hadoop]

volumes:
  namenode:
  datanode1:
  datanode2:
  datanode3:
  historyserver:

networks:
  hadoop:
    driver: bridge
```

`docker/spark-client/Dockerfile` — build on `apache/spark:3.5.3-scala2.12-java11-python3-ubuntu`, add `sbt`, `curl`, `python3-pip`, then `pip install pyspark==3.5.3 pandas pyarrow streamlit plotly folium geopandas kafka-python requests`. Write `core-site.xml` into `/etc/hadoop/conf` pointing `fs.defaultFS` at `hdfs://namenode:9000` so Spark resolves HDFS by hostname.

**Note on replication:** set to `2`, not `1`. With replication 1 each block sits on a single DataNode and the cluster looks decorative. Replication 2 across 3 DataNodes demonstrates real distribution and lets you demo killing a DataNode while the file still reads — a 30-second moment that proves the cluster is genuine.

**DoD:**
- `docker compose up -d` brings all services healthy.
- NameNode UI `http://localhost:9870` → *Datanodes* tab lists **3 live DataNodes**.
- YARN UI `http://localhost:8088` shows 1 active NodeManager.
- Inside `spark-client`: `hdfs dfs -ls /` works and `spark-submit --version` prints Spark 3.5.x / Scala 2.12.
- Screenshots of both UIs in `docs/evidence/phase0/`.

### P0.3 — Repo skeleton and .gitignore

Create the structure from §4. `.gitignore` excludes `data/`, `models/`, `*.parquet`, `target/`, `.ipynb_checkpoints/`, `__pycache__/`, `*.crc`.

**DoD:** `git status` stays clean after a full data download — no data file is ever staged.

---

## PHASE 1 — FIRST REVIEW DELIVERABLES ⭐

### P1.1 — Download the raw data

Write `scripts/01_download_data.py` — resumable, verifies file size, retries on failure, logs each file.

**Source:** NYC TLC Trip Record Data, direct Parquet links:
`https://d37ci6vzurychx.cloudfront.net/trip-data/<type>_tripdata_<YYYY>-<MM>.parquet`

Download into `data/raw/`:

| Dataset | File prefix | Range | Approx size |
|---|---|---|---|
| High-Volume For-Hire (Uber/Lyft) | `fhvhv_tripdata_` | 2021-01 → 2024-12 (48 files) | ~20 GB |
| Yellow Taxi | `yellow_tripdata_` | 2021-01 → 2024-12 (48 files) | ~2.5 GB |
| Green Taxi | `green_tripdata_` | 2021-01 → 2024-12 (48 files) | ~0.5 GB |

**Total ≈ 23 GB compressed Parquet ≈ 120 GB uncompressed, ~1.5 billion trip records.** FHVHV is the volume driver — do not drop it.

Also into `data/reference/`:
- `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` — 265 zones (LocationID, Borough, Zone, service_zone)
- `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip` — shapefile; convert to GeoJSON for the dashboard map

**DoD:** all 144 Parquet files present, total size printed and ≥ 20 GB, manifest CSV listing filename + size + row count.

### P1.2 — Load raw data into HDFS (demo requirement #2)

`scripts/03_upload_to_hdfs.ps1` — copy into the container, then `hdfs dfs -put` with this layout:

```
/taxipulse/raw/fhvhv/year=YYYY/month=MM/*.parquet
/taxipulse/raw/yellow/year=YYYY/month=MM/*.parquet
/taxipulse/raw/green/year=YYYY/month=MM/*.parquet
/taxipulse/reference/taxi_zone_lookup.csv
/taxipulse/reference/weather.json
```

Set block size explicitly (`-D dfs.blocksize=134217728`) so block distribution is demonstrable.

**DoD:**
- `hdfs dfs -du -s -h /taxipulse/raw` prints ≥ 20 GB.
- `hdfs dfs -count /taxipulse/raw` shows the file count.
- `hdfs fsck /taxipulse/raw -files -blocks -locations` saved to `docs/evidence/phase1/fsck_raw.txt`. **This is the strongest single piece of evidence** for demo requirement #7 — it prints which DataNode holds each block, proving real distribution.

### P1.3 — Scala preprocessing pipeline ⭐ (demo requirements #3, #4, #5, #6)

The graded centrepiece. Must genuinely be Scala. Build with sbt, produce a fat JAR via `sbt-assembly`, run with `spark-submit`.

**`Config.scala`** — case class holding input/output HDFS paths, cleaning thresholds, and a `sample: Boolean` flag defaulting to false. Parse CLI args; no hardcoded paths inside logic.

**`SchemaUnifier.scala`** — the three datasets have *different* schemas. Normalise into one canonical schema:

| Canonical column | Yellow / Green source | FHVHV source |
|---|---|---|
| `pickup_ts` | `tpep_pickup_datetime` / `lpep_pickup_datetime` | `pickup_datetime` |
| `dropoff_ts` | `tpep_dropoff_datetime` / `lpep_dropoff_datetime` | `dropoff_datetime` |
| `pu_location_id` | `PULocationID` | `PULocationID` |
| `do_location_id` | `DOLocationID` | `DOLocationID` |
| `trip_distance_mi` | `trip_distance` | `trip_miles` |
| `fare_amount` | `fare_amount` | `base_passenger_fare` |
| `tip_amount` | `tip_amount` | `tips` |
| `total_amount` | `total_amount` | computed: fare + tolls + tax + surcharges |
| `passenger_count` | `passenger_count` | not available → `null` |
| `service_type` | literal `"yellow"` / `"green"` | literal `"fhvhv"` |

Cast types explicitly. Never rely on schema inference across 144 files — it is slow and produces merge conflicts.

**`DataCleaner.scala`** — each rule a separate named function, with a comment giving the real-world justification, and each **counting the rows it removes** (needed for the quality report):

| Rule | Condition removed | Real-world reason |
|---|---|---|
| Null keys | null pickup/dropoff timestamp or location | unusable record |
| Invalid zone | `pu/do_location_id` not in 1–263 | 264/265 mean "Unknown"; outside range is corrupt |
| Non-positive fare | `fare_amount <= 0` | voided or refunded trips, not real rides |
| Absurd fare | `fare_amount > 1000` | data entry error |
| Non-positive distance | `trip_distance_mi <= 0` | cancelled before moving |
| Absurd distance | `trip_distance_mi > 200` | GPS glitch — NYC is ~35 mi across |
| Time inversion | `dropoff_ts <= pickup_ts` | clock or meter error |
| Too short | duration < 60 s | accidental start/stop |
| Too long | duration > 6 h | meter left running |
| Impossible speed | avg speed > 90 mph or < 0.5 mph | GPS error / stuck meter |
| Out-of-period | pickup date outside the file's own month | known TLC defect — files contain stray 2002/2098 dates |
| Passenger count | `> 8` (keep nulls for FHVHV) | vehicle capacity |
| Duplicates | exact duplicate rows | double-submitted records |

**`ZoneEnricher.scala`** — broadcast-join the 265-row zone lookup using an explicit `broadcast()`. It is tiny, and this avoids a shuffle — a tuning point to cite later. Adds `pu_borough`, `pu_zone`, `do_borough`, `do_zone`, `service_zone`.

**`WeatherJoiner.scala`** — join hourly weather on `(date, hour)`; see P1.5.

**`FeatureBuilder.scala`** — derived columns: `trip_duration_min`, `avg_speed_mph`, `pickup_hour`, `pickup_dayofweek`, `pickup_month`, `pickup_year`, `is_weekend`, `is_night` (22:00–05:00), `is_airport_trip` (zones 132 JFK / 138 LaGuardia / 1 Newark), `is_holiday`, `fare_per_mile`.

**`QualityReport.scala`** — writes rows-in, rows-out, and rows removed per rule with percentages to `docs/data_quality_report.md` plus a Parquet copy in HDFS. This is your *Veracity* evidence, quantified — a strong Results & Analysis slide.

**`PreprocessJob.scala`** — main. Reads raw from HDFS → unify → clean → enrich → features → writes back to HDFS:

```
/taxipulse/processed/trips/year=YYYY/month=MM/   (Parquet + snappy, partitioned)
```

Coalesce toward ~128 MB files to avoid the small-files problem, and say so in the report.

**DoD:**
- `sbt assembly` produces a JAR with no errors.
- `spark-submit` runs end-to-end on the **full** dataset and completes.
- `hdfs dfs -du -s -h /taxipulse/processed/trips` shows the output.
- Row counts before and after printed and recorded.
- `docs/data_quality_report.md` exists with per-rule removal counts.

### P1.4 — Verification queries (Spark SQL)

A short Scala/`spark-shell` script proving the processed data is usable: trips per borough, trips per hour of day, average fare by service type, top 10 pickup zones, effect of rain on hourly demand. Save outputs to `docs/evidence/phase1/`. These become the Results & Analysis slide.

### P1.5 — Weather data (the Variety requirement)

`scripts/02_download_weather.py` — Open-Meteo historical archive API, free, no key:

```
https://archive-api.open-meteo.com/v1/archive
  ?latitude=40.7128&longitude=-74.0060
  &start_date=2021-01-01&end_date=2024-12-31
  &hourly=temperature_2m,precipitation,rain,snowfall,wind_speed_10m,relative_humidity_2m
  &timezone=America/New_York
```

Request **one year per call** and concatenate — long ranges time out. Save raw JSON to `data/reference/weather.json`, upload to HDFS unmodified.

Keep it as **JSON, not CSV**: this is your Variety evidence — structured Parquet trips plus semi-structured nested JSON weather. Parse the nested arrays in Scala with an explicit schema.

> Terminology, for the viva: weather and zone data are **semi-structured**, not unstructured. Unstructured means free text or images with no schema at all. JSON has a schema — just a nested, flexible one. Do not let this slip into any document.

**DoD:** ~35,000 hourly rows (4 years × 8,760 h) joining onto trips with under 1% null match rate.

### P1.6 — Evidence capture ⭐ (demo requirement #7 — explicitly highlighted)

`scripts/99_capture_evidence.ps1` automates what it can; keep a manual screenshot checklist in `docs/runbook.md`. Required into `docs/evidence/phase1/`, numbered `01_...` to `10_...` so they drop straight into the PPT in order:

1. NameNode UI overview — capacity used, live nodes
2. NameNode UI **Datanodes tab** — 3 live DataNodes
3. HDFS browser showing `/taxipulse/raw/` and `/taxipulse/processed/`
4. Terminal: `hdfs dfs -du -s -h` for raw and processed
5. `hdfs fsck` output showing block replication and locations
6. YARN UI `:8088` with the Spark job **SUCCEEDED**
7. Spark UI showing stages, tasks, executor count for the preprocessing job
8. Full `spark-submit` console log saved as text
9. Scala code in the IDE + the `sbt assembly` success line
10. Before/after row counts side by side

**DoD:** every item exists as a file.

### P1.7 — First Review PPT (the 13 sections from §1)

Build `docs/review1/TaxiPulse_First_Review.tex` in Beamer, Madrid theme, `aspectratio=169`, with this preamble so it matches the zeroth-review deck:

```latex
\documentclass[aspectratio=169]{beamer}
\usetheme{Madrid}
\usecolortheme{default}
\definecolor{pulseblue}{RGB}{20,60,110}
\definecolor{pulseyellow}{RGB}{245,180,0}
\setbeamercolor{palette primary}{bg=pulseblue,fg=white}
\setbeamercolor{palette secondary}{bg=pulseblue!80,fg=white}
\setbeamercolor{palette tertiary}{bg=pulseblue!60,fg=white}
\setbeamercolor{structure}{fg=pulseblue}
\setbeamercolor{title}{bg=pulseblue,fg=white}
\setbeamercolor{frametitle}{bg=pulseblue,fg=white}
\usepackage{tikz}\usetikzlibrary{arrows.meta,positioning}
\usepackage{booktabs}\usepackage{array}
\setbeamertemplate{navigation symbols}{}
\setbeamertemplate{itemize item}{\color{pulseblue}$\blacktriangleright$}
\setbeamertemplate{itemize subitem}{\color{pulseyellow}$\bullet$}
```

Title slide must carry Group-6, all four names with roll numbers, **Faculty In-charge**, Department of Artificial Intelligence and Data Science, college name, and AY 2026–27.

Content guidance for the sections that are new relative to the zeroth review:

- **§7 Implementation Details** — cluster topology diagram, Scala module list, the cleaning rules table, HDFS directory layout.
- **§8 Results & Analysis** — quality report numbers, P1.4 query outputs, evidence screenshots.
- **§9 Challenges / Limitations** — disk and memory constraints, schema drift across years, TLC data defects, single-machine containerised cluster rather than physical nodes.
- **§10 Big data tools vs conventional techniques** — needs **hard numbers**, not opinions. Produce them in Phase 2 before finalising.
- **§12 Conference paper status** — state honestly as "in preparation", name a target venue and timeline.

Wrap any TikZ diagram in `\resizebox{0.95\textwidth}{!}{...}` — without it the architecture diagram overflows the slide.

**DoD:** compiles with pdfLaTeX, ≤ 20 slides, every claim backed by a captured screenshot or a measured number.

### P1.8 — Demo rehearsal

Write `docs/review1/demo_script.md`: the exact command sequence, in order, with expected output for each, plus a fallback (pre-recorded terminal capture and screenshots) in case the cluster misbehaves on the day. Include the "kill a DataNode, show the file still reads" moment.

**DoD:** the full demo runs start-to-finish from a cold `docker compose up` in under 15 minutes.

---

## PHASE 2 — Conventional vs big data comparison (feeds PPT §10)

### P2.1 — MapReduce baseline

A Hadoop Streaming job — trips per (pickup zone, hour):
- `mapper.py` emits `PULocationID_hour \t 1`
- `reducer.py` sums per key
- `run_job.ps1` calls `hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar` with `-mapper` / `-reducer` / `-input` / `-output`

Raw data is Parquet and Streaming needs text, so first export one month to CSV via Spark into `/taxipulse/raw_csv/`. Document that conversion as a finding in itself: MapReduce Streaming cannot read columnar formats natively — a concrete reason the industry moved to Spark.

**DoD:** job SUCCEEDED in YARN UI, output in HDFS, wall-clock time recorded, screenshot captured.

### P2.2 — The comparison table for slide 10

`benchmarks/pandas_vs_spark.py` — the same aggregation, timed across approaches:

| Approach | Scope | Expected outcome |
|---|---|---|
| pandas, single machine | 1 month (~19 M rows) | Works, slow, high memory |
| pandas, single machine | full 4 years | **Fails with MemoryError** — capture the traceback, it is the best slide in the deck |
| Hadoop MapReduce | 1 month CSV | Works, slowest (disk I/O between stages) |
| Spark | 1 month | Fast |
| Spark | full 4 years | Works — the entire point of the project |

**DoD:** `benchmarks/results.csv` with wall-clock times, a bar chart PNG, and the pandas MemoryError traceback saved as evidence.

---

## PHASE 3 — Analytical feature engineering

### P3.1 — Demand aggregation table

From processed trips: `/taxipulse/features/demand/` → `(zone_id, date, hour) → trip_count`, plus that hour's weather, `dayofweek`, `is_weekend`, `is_holiday`, `borough`.

Add **lag features** via Spark window functions partitioned by zone, ordered by time: `lag_1h`, `lag_2h`, `lag_24h` (same hour yesterday), `lag_168h` (same hour last week), `rolling_mean_3h`, `rolling_mean_24h`, `rolling_mean_168h`.

> Plain words: to guess how busy a zone will be at 6 PM today, the strongest clues are how busy it was at 5 PM today, at 6 PM yesterday, and at 6 PM last Friday. Those three clues are the lag features.

### P3.2 — Trip-level feature table

`/taxipulse/features/trips/` for the fare and ETA models — containing only features **knowable at booking time** (Appendix B).

**DoD:** both tables in HDFS, partitioned, no nulls in required feature columns, row counts logged.

---

## PHASE 4 — Machine learning (PySpark MLlib)

### P4.1 — Splitting strategy

**Split by time, never randomly.** A random split lets the model see the future and then predict the past, inflating every metric.

- Train: 2021-01 → 2023-12
- Validation: 2024-01 → 2024-06
- Test: 2024-07 → 2024-12 (touch once, at the very end)

### P4.2 — Baselines first

Before any ML, compute naive baselines. A model that cannot beat these is worthless, and reviewers ask.

| Task | Baseline |
|---|---|
| Demand | historical mean trips for that (zone, hour, day-of-week) |
| Fare | `distance × average $/mile` |
| ETA | `distance ÷ average speed` |

### P4.3 — Models

Each task compares **at least three** algorithms: `LinearRegression`, `RandomForestRegressor`, `GBTRegressor`.

- **`demand_model.py`** → target `trip_count`; features: lags, rolling means, calendar, weather, zone cluster ID.
- **`fare_model.py`** → target `total_amount`.
- **`eta_model.py`** → target `trip_duration_min`.
- **`zone_clustering.py`** → K-Means over each zone's 24-hour normalised demand profile (a 24-dimensional vector per zone). Pick *k* using the elbow method **and** silhouette score, then hand-label clusters (airport / business district / nightlife / residential) by inspecting their profiles. Feed the cluster ID back into the demand model as a feature.

Use `StringIndexer` + `OneHotEncoder`, `VectorAssembler`, `StandardScaler`, all inside a `Pipeline`. Tune on the **validation** split with `TrainValidationSplit` respecting time order — do not use k-fold cross-validation across time.

### P4.4 — Evaluation

`evaluate.py` computes RMSE, MAE, R², MAPE for every model against its baseline, writing one comparison table plus plots: predicted-vs-actual scatter, residual distribution, GBT feature importances, and demand-over-time actual-vs-predicted for a few interesting zones (JFK, Midtown, a residential zone).

**DoD:** every model beats its baseline on the test set; results table in `docs/`; models saved to `/taxipulse/models/` via `model.write().overwrite().save(...)`.

---

## PHASE 5 — Real-time streaming layer

### P5.1 — Kafka

Add a `kafka` service (`apache/kafka:3.9.0`, KRaft mode) to the compose file on the `hadoop` network. Topic `taxi-trips`, 3 partitions keyed by pickup zone so a zone's events stay ordered.

### P5.2 — Producer

`streaming/kafka_producer.py` — reads processed trips from HDFS in `pickup_ts` order and replays them as JSON events with **time compression** (configurable, default 1 real second = 1 simulated minute). Print a clear banner stating this is a historical replay simulating live traffic, and say the same in the report. Every reviewer asks; being upfront is a strength.

### P5.3 — Structured Streaming consumer

`streaming/spark_streaming_job.py`:
- Reads Kafka with an explicit schema.
- **Windowed aggregation:** 10-minute tumbling windows per zone with `withWatermark("event_ts", "15 minutes")` for late events.
- Loads the trained demand model, compares live observed pace against forecast.
- **Anomaly alert** when `observed / forecast > threshold` (start at 2.0) sustained across two windows — e.g. a concert ending or a transit outage.
- Writes to `/taxipulse/streaming/live_zone_stats/` with `checkpointLocation` set.

**DoD:** producer and consumer run together; live zone counts update; an injected surge triggers an alert; streaming query progress screenshots captured.

---

## PHASE 6 — Dashboard

`dashboard/app.py` — Streamlit, three tabs:

1. **Driver Mode** — NYC choropleth (zone GeoJSON) coloured by predicted demand for the next hour, top-5 recommended zones, auto-refreshing from the streaming sink.
2. **Passenger Mode** — pick origin zone, destination zone, time, weather → predicted fare and ETA with a confidence range.
3. **Operator Mode** — live trips/minute, active alerts, zone cluster map, model health (recent RMSE).

Plotly/Folium for maps, `@st.cache_data` for caching. Read predictions from the sink — never recompute on page load.

**DoD:** runs with `streamlit run`, all three tabs functional, screen recording captured for the final demo.

---

## PHASE 7 — Performance tuning study

`benchmarks/run_benchmarks.py` — run the same job under each configuration, record wall-clock time, produce table + chart:

| Experiment | Variants |
|---|---|
| File format | CSV vs Parquet |
| Partitioning | unpartitioned vs partitioned by year/month, measured with a filtered query to show partition pruning |
| Caching | `.cache()` on a reused DataFrame vs not |
| Shuffle partitions | `spark.sql.shuffle.partitions` = 200 (default) vs tuned |
| Join strategy | broadcast vs sort-merge join for the zone lookup |
| Cluster size | 1 vs 2 vs 3 DataNodes (scale the compose down to demonstrate) |

Explain *why* each result happens — column pruning, predicate pushdown, avoided shuffle. Reviewers reward the explanation more than the number.

**DoD:** results table, charts, and written analysis in `docs/performance_study.md`.

---

## PHASE 8 — Final review and paper

1. Final report consolidating all phases.
2. Final Beamer deck extending the Phase 1 deck with ML, streaming, dashboard, and tuning results.
3. Conference paper draft — the novelty argument stated plainly:
   - **Unified triple prediction** — demand, fare, and ETA from one pipeline and one feature store, where the 2024–25 literature solves each in isolation.
   - **Full-scale processing** — 1.5 B records with no sampling, where comparable studies subsample to fit one machine.
   - **Batch + streaming hybrid** — trained offline, applied to a live stream with anomaly alerting, where prior work stops at offline evaluation.
   - **Open and reproducible** — public data, public code, one `docker compose up`.
4. Reproducibility check: wipe everything, clone fresh, follow `docs/runbook.md`, confirm it rebuilds.

---

## Appendix A — Known data traps

- **Schema drift.** TLC changed columns across years (`airport_fee` appears and gets renamed; FHVHV gained fields). Never use `mergeSchema` blindly — declare an explicit schema per dataset per era.
- **Junk timestamps.** Files contain stray dates from 2002, 2009, 2098. Filter to the file's own month.
- **Zones 264/265** mean "Unknown" / "Outside NYC" — exclude from zone-level analysis but count them in the quality report.
- **FHVHV has no `passenger_count`.** Keep the column nullable; do not impute a fake value.
- **`trip_time` in FHVHV is seconds**, while yellow/green have no duration column at all (compute it). Unit mismatches here silently corrupt the ETA model.
- **Timezone.** TLC timestamps are local New York time. Pull weather with `timezone=America/New_York`; a UTC mismatch shifts weather by 4–5 hours and quietly destroys the correlation.

## Appendix B — Preventing data leakage (read before Phase 4)

**Plain words:** the model may only be told things a real user would know *at the moment of booking*. Feed it facts that only exist after the trip finishes and it will look brilliant in testing and be useless in reality.

For **fare** and **ETA** models you may use: pickup zone, dropoff zone, estimated distance (haversine between zone centroids), hour, day of week, month, holiday flag, weather, passenger count, rate code, service type, zone cluster.

You must **not** use: `trip_duration_min` (that *is* the ETA target), `avg_speed_mph`, `tip_amount`, `tolls_amount`, `total_amount` when predicting `fare_amount`, or the meter-recorded `trip_distance_mi` (the *actual* distance driven, known only afterwards — use the zone-centroid estimate instead and document the substitution).

If a fare model reports R² above ~0.98, assume leakage and go find it.

## Appendix C — Quick command reference

```powershell
# Cluster
cd docker; docker compose up -d; docker compose ps
docker exec -it spark-client bash

# HDFS (inside spark-client)
hdfs dfsadmin -report
hdfs dfs -du -s -h /taxipulse/raw
hdfs fsck /taxipulse/raw -files -blocks -locations

# Build and run the Scala pipeline
cd /workspace/scala-pipeline && sbt assembly
spark-submit --master yarn --deploy-mode client \
  --class com.taxipulse.PreprocessJob \
  --conf spark.sql.shuffle.partitions=200 \
  target/scala-2.12/taxipulse-assembly-1.0.jar \
  --input hdfs://namenode:9000/taxipulse/raw \
  --output hdfs://namenode:9000/taxipulse/processed
```

**Web UIs:** NameNode `:9870` · YARN `:8088` · History `:19888` · Spark `:4040` · Streamlit `:8501`

---

## Execution order summary

```
P0 (setup — CHECK DISK FIRST)
  → P1 (FIRST REVIEW: data → HDFS → Scala clean → HDFS → evidence → PPT)   ⭐ deadline-driven
  → P2 (conventional vs big data comparison, feeds PPT §10)
  → P3 (features) → P4 (ML) → P5 (streaming) → P6 (dashboard)
  → P7 (tuning) → P8 (final report + paper)
```

**If time runs short, cut scope from Phases 5–7. Never from Phase 1.**
