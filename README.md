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

## 🏗 Architecture

The platform follows the classic **Medallion Architecture**, layered on top of an **incremental batch control system** that decides what data needs to be processed on each run.

<img width="1861" height="1013" alt="formula1-incremental-data-processing" src="https://github.com/user-attachments/assets/9891db78-8bc7-4f22-8b84-2fb1604756a0" />
<img width="1886" height="1023" alt="incremental-data-processing-medallion" src="https://github.com/user-attachments/assets/b4830edc-54bb-4516-bcb9-f8eb93af4cdf" />
