# Formula 1 Lakehouse — End-to-End Data Engineering on Databricks

An end-to-end **batch data engineering pipeline** built on the **Databricks Lakehouse Platform**, using the Formula 1 racing dataset (seasons, races, drivers, constructors, results, sprints) to implement a production-style **Medallion Architecture (Bronze → Silver → Gold)** with **incremental (batch) loading**, **automated orchestration**, **scheduled execution**, **email alerting**, and **CI/CD via GitHub integration**.

The final Gold layer powers a set of interactive **Databricks SQL Dashboards** for Driver Championship standings, Constructor Championship standings, and historical "greatest of all time" analysis.

## Project Overview

This project simulates a real-world F1 data platform that ingests season data (circuits, races, constructors, drivers, results, and sprints) as it "arrives" in batches, and progressively refines it through three quality tiers until it's ready for business consumption:

- **Bronze** — raw, untouched data as delivered, with ingestion metadata attached.
- **Silver** — cleaned, standardised, deduplicated, and business-key validated data.
- **Gold** — a dimensional (star schema) model with facts and dimensions, ready for BI/analytics.

Rather than reprocessing the entire dataset on every run, the pipeline is built as a **true incremental system**: a control table tracks which data batches have landed, which are being processed, and which are complete — so every notebook processes **only the new batch_id**, and merges (upserts) the result into Bronze/Silver/Gold using Delta Lake `MERGE`.

The whole pipeline is:
- **Orchestrated** with Databricks Jobs (Lakeflow Jobs) as a multi-task DAG.
- **Scheduled** to run automatically every day at **7:00 AM** via a cron trigger.
- **Monitored** via automatic **email notifications** on job success and failure.
- **Version controlled** through a **Databricks ↔ GitHub Repo integration**, enabling a CI/CD-style workflow for notebook development.

## Architecture

The platform follows the classic **Medallion Architecture**, layered on top of an **incremental batch control system** that decides what data needs to be processed on each run.

<img width="1861" height="1013" alt="formula1-incremental-data-processing" src="https://github.com/user-attachments/assets/9891db78-8bc7-4f22-8b84-2fb1604756a0" />
<img width="1886" height="1023" alt="incremental-data-processing-medallion" src="https://github.com/user-attachments/assets/b4830edc-54bb-4516-bcb9-f8eb93af4cdf" />


**How incremental processing works, end to end:**

1. New batch folders (e.g. `/Volumes/formula1_incr/landing/files/3/`) land in the **Landing volume**, each containing that batch's `circuits.csv`, `races.csv`, `constructors.json`, `drivers.json`, `results/*.json`, and `sprints/*.json`.
2. **`01.Identify Next Batch`** compares the folders present in landing against a `control.batch_control` Delta table (which tracks `in_progress` / `completed` batches) and works out the **single earliest unprocessed batch**.
3. **`02.Create New Batch`** inserts a row into `batch_control` marking that batch as `in_progress`.
4. The **Bronze → Silver → Gold** notebooks all run **parameterised by `p_batch_id`**, so each run only touches the rows belonging to that batch:
   - Bronze writes with `overwrite` + `replaceWhere batch_id = '<id>'` (safe, idempotent re-runs per batch, partitioned by `batch_id`).
   - Silver and Gold use Delta Lake `MERGE` (`whenMatchedUpdate` / `whenNotMatchedInsertAll`), with an extra `s.batch_id >= t.batch_id` guard on Silver so an accidental re-run of an **older** batch can never overwrite newer data.
5. **`03.Complete Batch`** flips that batch's status to `completed` once every layer has finished successfully.
6. The orchestration job loops back to step 2 — if another unprocessed batch exists it is picked up immediately; if not, the run exits cleanly. This lets the pipeline safely "catch up" on multiple missed batches in a single scheduled run.

This design means the pipeline is **restartable, idempotent, and safe to re-run** without creating duplicate data or double-counting points/wins in the Gold layer.



## Technical Stack

| Layer | Technology |
|---|---|
| Compute & Orchestration | Databricks (Job Clusters), Databricks Jobs / Lakeflow Jobs |
| Processing Engine | Apache Spark 4.0 (PySpark DataFrame API, Spark SQL) |
| Storage Format | Delta Lake  |
| Data Governance | Unity Catalog (Catalogs, Schemas, External Locations, Volumes, Storage Credentials) |
| Cloud Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) via  |
| Alerting | Databricks Job email notifications (success / failure) |
| BI / Analytics | Databricks SQL, Databricks SQL Dashboards |
| Version Control / CI-CD | Git-backed Databricks Repos integrated with GitHub |
| Languages | Python (PySpark), SQL |

##  Dataset Overview & Entity Relationship Diagrams

The source data models Formula 1 championship history: **circuits**, **races**, **constructors**, **drivers**, **race results**, and **sprint results** — delivered as a mix of CSV and JSON files per batch (mirroring how a real sports-data API/vendor would drop periodic season updates).

### Bronze (Raw) Layer — as delivered

Data is ingested with its original field names (`circuitId`, `raceName`, `positionText`, etc.) and enforced schemas (`FAILFAST` mode — the job fails loudly on any schema drift rather than silently corrupting data).

<img width="1361" height="971" alt="formula1-raw-data-erd" src="https://github.com/user-attachments/assets/50be4138-b966-4d8c-8282-8e82e02bcf88" />


### Silver Layer — cleaned & standardised

Columns are renamed to `snake_case` and given more meaningful names (`lat`/`long` → `latitude`/`longitude`, `date` → `race_date`, `grid` → `grid_position`, etc.), business keys are null-checked, duplicates are dropped, and text fields are cast to Title Case. Every table also carries `ingestion_timestamp`, `source_file`, and `batch_id` for full lineage and traceability.

<img width="1401" height="1001" alt="formula1-silver-data-erd" src="https://github.com/user-attachments/assets/8034b699-c0ff-47fe-a6df-d46381db7a27" />

### Gold Layer — dimensional star schema

The Gold layer reshapes the cleaned Silver tables into a **star schema**: a central `fact_session_results` table (which unions **race** results and **sprint** results, tagged by `session_type`) surrounded by `dim_drivers`, `dim_constructors`, and `dim_races`. Derived analytical flags (`is_win`, `is_podium`, `has_points`) are computed once here, so every downstream dashboard/report stays consistent. A curated `ref_nationality_region` reference table also enriches drivers and constructors with a geographic **region** (Europe, Americas, Asia, Africa, Oceania).

<img width="1071" height="481" alt="formula1-gold-data-erd" src="https://github.com/user-attachments/assets/bc004863-8165-453a-86de-da3e2b8964c6" />

##  Lakehouse Job & Orchestration Architecture

Two Databricks Jobs work together to run the platform:

### 1. `job_formula1_lakehouse_full_refresh` — the core ETL DAG

This job encodes the medallion pipeline itself as a dependency graph of tasks — ingest each Bronze source, transform each into Silver, then build each Gold dimension/fact — matching the data lineage 1:1 (e.g. `01_ingest_circuits_file → 01_transform_circuits_data → 01_Build_Races_Dimension`). Every task accepts a `p_batch_id` parameter so the same job definition serves both **full historical loads** and **single-batch incremental runs**.
<img width="1920" height="1080" alt="LakeFlow Job 1" src="https://github.com/user-attachments/assets/b85c2b61-62e1-4141-8981-b7cfcf242ff5" />


### 2. `job_formula1_incremental_batch_orchestration` — the incremental control loop

This is the "brain" of the incremental system. It runs the **Identify → Conditional Branch → Create Batch → Run ETL Job → Complete Batch** control flow described in the [Architecture](#-architecture) section above, invoking `job_formula1_lakehouse_full_refresh` as a **child job run** for the identified `batch_id` before marking that batch complete.
<img width="1920" height="971" alt="Mater lake_flowjob" src="https://github.com/user-attachments/assets/8170867c-0b52-42d7-9f17-1691aca889dc" />

##  Scheduling, Alerting & CI/CD

- **Scheduling** — `job_formula1_incremental_batch_orchestration` runs automatically on a **daily cron trigger at 7:00 AM**, so any new data dropped into the landing volume the previous day is picked up and processed without manual intervention.
- **Email Notifications** — Databricks Job notification settings are configured to send an email on **every run outcome**: a ✅ success email confirming the run/job ID, duration, and status, and a ❌ failure email if any task in the DAG fails, so pipeline health can be monitored without needing to log into the workspace.

<img width="1920" height="1080" alt="Job Notification" src="https://github.com/user-attachments/assets/f71ded53-9f98-40a4-9d86-c4d0d70c773d" />


- **CI/CD via GitHub Integration** — The Databricks Workspace is linked to a **GitHub repository** through Databricks Repos (Git folders). All notebooks in this project (`00-common` through `06-orchestration`) are version-controlled `.py`/`.sql` source files rather than opaque notebook JSON, enabling standard Git workflows — feature branches, pull requests, and code review — before changes are pulled into the workspace and picked up by the scheduled Jobs.

---

##  Data Analytics — Dashboards

Three Databricks SQL Dashboard pages sit on top of the Gold layer views (`v_driver_standing`, `v_constructor_standing`) and the Gold fact/dimension tables, filterable by **season**.

### 1. Driver Championship Standings
Live standings table plus donut and bar visuals showing points distribution and total points scored by every driver in the selected season.

<img width="1920" height="1080" alt="Dashboard_1_Driver_Championship_ Standings" src="https://github.com/user-attachments/assets/ba23d916-d90c-4c88-80ee-da94e1a21451" />

### 2. Constructor Championship Standings
Mirrors the driver view at the team level — standings table, wins share by team, and total points by constructor.

<img width="1920" height="1080" alt="Dashboard_2_Constructor_Championship_standing" src="https://github.com/user-attachments/assets/f3ca995b-35a1-41a4-ab74-104ec5c5ec01" />


### 3. Dominant Drivers / Teams of All Time
A cross-season, all-time "greatness" analysis. A weighted **Greatness Score** — `(championships × 100) + (wins × 10) + (podiums × 3)` — ranks the most dominant drivers and constructors across F1 history, backed by the underlying SQL query and results shown below.

<img width="1920" height="1080" alt="Dashboard_3_Dominant_Drivers" src="https://github.com/user-attachments/assets/25d07e70-3fe8-4664-aa7a-0dfc247a7124" />

<img width="1920" height="1080" alt="Dashboard_Dominant_Teams " src="https://github.com/user-attachments/assets/a0f3edef-809e-4cd7-a730-2cab91a86d27" />

**Underlying SQL analysis (drivers):**

<img width="1920" height="1080" alt="Dominant Drivers Analysis" src="https://github.com/user-attachments/assets/6ed5b4a1-600f-46fd-9ec7-2360f8a8014e" />


**Underlying SQL analysis (constructors):**

<img width="1920" height="1080" alt="Dominant_Teams_Analysis" src="https://github.com/user-attachments/assets/7f6682a4-cb1a-4267-8bde-c63bc583146f" />

## 📁 Project Structure

```
Notebooks/
├── 00-common/                              # Shared config & reusable write helpers
│   ├── 01.environment-config.py            # Catalog/schema names, landing path
│   ├── 02.bronze-helpers.py                # add_ingestion_metadata(), write_to_bronze()
│   ├── 03.silver-helpers.py                # write_to_silver() — create-or-merge pattern
│   └── 04.gold-helpers.py                  # write_to_gold()   — create-or-merge pattern
│
├── 01-setup/                               # One-time environment setup
│   ├── 01.Setup Project Environment.sql    # External location, catalog, schemas, volume
│   └── 02.Setup Batch Events.sql           # Control schema & batch_events bootstrap
│
├── 02-bronze/                              # Raw ingestion (per source file)
│   ├── 01.Ingest Circuits File.py
│   ├── 02.Ingest Races File.py
│   ├── 03.Ingest Constructors File.py
│   ├── 04.Ingest Drivers File.py
│   ├── 05.Ingest Results File.py
│   └── 06.Ingest Sprints File.py
│
├── 03-silver/                              # Cleansing, standardisation, dedup, merge
│   ├── 01.Transform Circuits Data.py
│   ├── 02.Transform Races Data.py
│   ├── 03.Transform Constructors Data.py
│   ├── 04.Transform Drivers Data.py
│   ├── 05.Transform Results Data.py
│   └── 06.Transform Sprints Data.py
│
├── 04-gold/                                # Dimensional model (star schema)
│   ├── 01.Build Races Dimension.py
│   ├── 02.Build Constructors Dimension.py
│   ├── 03.Build Drivers Dimension.py
│   ├── 04.Build Results Fact.py            # Unions race + sprint results
│   └── 91.Build Nationality Region Reference.py
│
├── 05-analytics/                           # BI-facing SQL views
│   ├── 01.Build Driver Standings View.sql
│   └── 02.Build Constructor Standings View.sql
│
├── 06-orchestration/                       # Incremental batch control framework
│   ├── 00.Create Control Tables.py
│   ├── 01.Identify Next Batch.py
│   ├── 02.Create New Batch.py
│   └── 03.Complete Batch.py
```

##  Key Learnings

- **Designing for idempotency**: using `replaceWhere` on Bronze and a `batch_id >= target.batch_id` merge guard on Silver ensures re-running a batch (or accidentally re-running an old one) never corrupts downstream data — a critical property for any production incremental pipeline.
- **Separating "what changed" from "how to process it"**: the control-table pattern (`Identify → Create → Process → Complete`) decouples batch detection from the transformation logic, so the same Bronze/Silver/Gold notebooks work identically for a full historical backfill and a daily incremental trickle.
- **Delta Lake `MERGE` as the backbone of SCD-style upserts**: reusable `write_to_silver()` / `write_to_gold()` helper functions standardise the merge logic (insert-if-new, update-if-matched, preserve `created_timestamp`) across every table in the project.
- **Schema enforcement over inference**: explicitly defining schemas and using `mode('FAILFAST')` catches upstream data quality issues immediately instead of allowing silent `null`/mis-typed columns to flow downstream.
- **Modelling facts across multiple session types**: unioning `results` and `sprints` into a single `fact_session_results` table (tagged by `session_type`) avoids duplicating dimensional logic and keeps standings queries simple.
- **Operationalising a pipeline, not just building it**: adding a cron schedule, success/failure email alerts, and a job-level conditional (has a new batch arrived? yes/no) turns a notebook exercise into something that behaves like a real production data platform.
- **Governance from day one**: using Unity Catalog external locations, managed schema locations, and a dedicated storage credential keeps cloud storage access auditable and centrally governed rather than hard-coding storage keys into notebooks.
- **Git-backed development on Databricks**: working out of a GitHub-linked Repo (rather than the classic notebook workspace) makes the whole project reviewable, diffable, and CI/CD-ready.

---

##  Future Enhancements

- Add automated data quality tests (e.g. Great Expectations / Delta Live Tables expectations) as a gating step before promoting Bronze → Silver.
- Extend the GitHub integration into a full CI/CD pipeline using GitHub Actions to run notebook tests and deploy job definitions via the Databricks Asset Bundles (DABs) framework.
- Add a driver/constructor **season-over-season trend** dashboard using window functions (`LAG`/`LEAD`).
- Capture and surface job/task-level runtime metrics (e.g. via system tables) to track pipeline performance over time.





















































