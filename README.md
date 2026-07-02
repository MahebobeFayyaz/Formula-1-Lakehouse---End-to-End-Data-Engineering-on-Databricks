# Formula 1 Lakehouse — End-to-End Data Engineering on Databricks

An end-to-end **batch data engineering pipeline** built on the **Databricks Lakehouse Platform**, using the Formula 1 racing dataset (seasons, races, drivers, constructors, results, sprints) to implement a production-style **Medallion Architecture (Bronze → Silver → Gold)** with **incremental (batch) loading**, **automated orchestration**, **scheduled execution**, **email alerting**, and **CI/CD via GitHub integration**.

The final Gold layer powers a set of interactive **Databricks SQL Dashboards** for Driver Championship standings, Constructor Championship standings, and historical "greatest of all time" analysis.
