# ShipmentOps-ETL

**An orchestrated batch ETL pipeline for logistics data** : orders, warehouse inventory, delivery events, and partner payments, built on Apache Airflow, landing in a quarantine-aware validation layer, and loaded into a star-schema PostgreSQL data warehouse for analytics.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Warehouse-PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Runtime-Docker%20Compose-2496ED?logo=docker&logoColor=white)

### Highlights

- **Real orchestration, not a script that runs top to bottom** : a 6-task Airflow DAG (`generate → extract → validate → transform → load → quality_checks`) with retries, execution timeouts, and XCom-passed state between tasks, scheduled daily.
- **Quarantine, not silent failure** : records that fail schema or null checks don't get dropped; they're written to a `quarantine/` zone with the specific reason and timestamp attached, so a failed row is always traceable back to *why* it failed.
- **Star-schema warehouse** : `fact_deliveries` joined against `dim_warehouse`, `dim_partner`, `dim_route`, and `dim_date`, with surrogate keys and upsert (SCD Type 1) logic on the dimension loads.
- **Incremental loading** : the fact table load reads a watermark from an `etl_metadata` table and only processes orders newer than the last successful run, instead of reprocessing the full history every day.
- **A real, pluggable data-quality framework** : `DataQualityRule` / `DataQualityValidator` classes with composable checks (`not_null`, `unique_values`, `between`, `non_negative`, `in_set`, `referential_integrity`) rather than one-off `if` statements scattered through the code.
- **Fully containerized** : `docker-compose up` brings up Airflow (webserver + scheduler), a metadata Postgres, and a separate warehouse Postgres, with the star schema auto-created on first boot.

---

## Why this project

A lot of ETL portfolio projects are a single script: read a file, transform it, write it somewhere. That doesn't reflect how batch pipelines actually run in a company with a real data platform team, and it skips the parts that make a pipeline *operable* rather than just functional:

1. **Orchestration** : tasks have dependencies, need retries, need to be scheduled, and need their intermediate state passed between them safely (Airflow's XCom) instead of relying on global variables or re-reading files by convention.
2. **Data quality as a first-class step**, not an afterthought, bad records need a *reason* attached and a place to land, and quality checks need to run as a distinct, auditable stage after load, not be buried inline inside transform logic.
3. **A warehouse schema built for analytics**, not just "a table with all the columns", a star schema with proper dimension/fact separation is what makes the analytical queries in `sql/analytical_queries.sql` fast and readable instead of a wall of nested joins over raw columns.

This project is built to demonstrate that reasoning end to end: messy synthetic data in, a DAG that has to actually handle the mess, and a warehouse schema and query set on the other side that a BI tool or analyst could plug into directly.

---

## Architecture

```
                         ┌───────────────────────────────────────────┐
                         │              ShipmentOps-ETL                │
                         └───────────────────────────────────────────┘

  generate_synthetic_data          extract              validate                transform              load                  quality_checks
  ────────────────────────        ─────────            ──────────              ─────────────          ──────                ─────────────────
  writes messy CSVs to     ──▶   reads CSVs,     ──▶   schema + null    ──▶   standardizes      ──▶   upserts dims,   ──▶   runs DataQuality
  data/landing/                  tags with               checks;               dates, currency,        loads fact via          Validator rules +
  (orders, inventory,            _extracted_at,          quarantines           casing, computes         incremental             referential
  delivery_events,               writes Parquet          failing rows          days_late /              watermark load          integrity checks
  partner_payments)              to data/processed/      to data/                is_on_time                                     against the
                                                          quarantine/                                                            warehouse
                                                                                                              │
                                                                                                              ▼
                                                                                          PostgreSQL star-schema warehouse
                                                                                    (dim_warehouse, dim_partner, dim_route,
                                                                                      dim_date, fact_deliveries)
                                                                                                              │
                                                                                                              ▼
                                                                                        sql/analytical_queries.sql
```

**Landing (`data/landing/`)** : raw CSVs as they'd arrive from source systems, generated with intentional messiness: inconsistent date formats (`%Y-%m-%d`, `%m/%d/%Y`, `%d-%m-%Y` all mixed in the same column), inconsistent status casing (`pending` vs `Pending` vs `DELIVERED`), nulls in optional fields, duplicate order rows, and a few physically invalid values (negative delivery durations).

**Extract** : reads each landing CSV as all-strings (to avoid pandas guessing types wrong on messy input), tags every row with `_extracted_at` / `_source_file` / `_execution_date`, and writes Parquet to `data/processed/`.

**Validate** : checks required columns are present and non-null (excluding explicitly nullable fields per source), and checks uniqueness on each source's natural key. Rows that fail either check are written to `data/quarantine/` with the specific failure reason attached.

**Transform** : standardizes dates (multiple input formats → ISO), lowercases/strips status and event-type strings, converts all currency amounts to USD off a fixed exchange-rate table, and computes derived fields (`is_on_time`, `days_late`, `needs_reorder`).

**Load** : upserts the four dimension tables (SCD Type 1- latest values win), then loads `fact_deliveries` incrementally: pulls the last watermark from `etl_metadata`, only processes orders newer than that watermark, joins in the dimension surrogate keys, and updates the watermark to the new max order date on success.

**Quality checks** : runs the composable rule framework per dataset (null checks, uniqueness, value-range checks, valid-set checks) plus a referential-integrity check against the warehouse (orphaned `warehouse_key` values in the fact table), and logs a pass/fail summary. Currently logs failures rather than hard-failing the DAG.

---

## Project structure

```
ShipmentOps-ETL/
├── dags/
│   └── shipmentops_etl_dag.py     # the 6-task Airflow DAG definition
├── plugins/shipmentops/
│   ├── extract.py                 # landing CSV -> tagged Parquet, + synthetic data generator
│   ├── validate.py                # schema/null/uniqueness checks, quarantine writer
│   ├── transform.py                # date/currency/casing standardization, derived fields
│   ├── load.py                    # dimension upserts + incremental fact load (watermark-based)
│   └── quality_checks.py          # composable DataQualityRule / DataQualityValidator framework
├── scripts/
│   └── init_warehouse.sql         # star schema DDL, run automatically on first container boot
├── sql/
│   └── analytical_queries.sql     # 5 analytical queries against the warehouse
├── docker-compose.yaml            # Airflow (webserver+scheduler) + 2x Postgres (metadata, warehouse)
├── dockerfile                     # custom Airflow image with project dependencies
├── requirements.txt
└── .env.example                   # Airflow UID + default admin credentials
```

---

## How to run

Requires Docker and Docker Compose.

```bash
git clone <your-repo-url>
cd ShipmentOps-ETL
cp .env.example .env
docker-compose up -d
```


1. Open **http://localhost:8080** : Airflow UI. Log in with `airflow` / `airflow`.
2. Find the `shipmentops_etl` DAG in the list, un-pause it, and trigger it manually, or wait for its daily schedule.
3. Watch the 6 tasks run in the Graph view: `start → generate_synthetic_data → extract → validate → transform → load → quality_checks → end`.
4. Once it finishes, connect to the warehouse directly to run the analytical queries : it's exposed on your host at `localhost:5433`:
   ```bash
   psql -h localhost -p 5433 -U warehouse -d shipmentops
   # password: warehouse

To stop everything:
```bash
docker-compose down        # stop containers, keep data
docker-compose down -v     # stop containers and wipe the Postgres volumes (fresh start)
```

---

## Tech stack

- **Orchestration**: Apache Airflow 2.8.1 (LocalExecutor), Python 3.11
- **Transform**: pandas, `python-dateutil` for robust multi-format date parsing
- **Warehouse**: PostgreSQL, star schema (dimension + fact tables, surrogate keys, indexed foreign keys)
- **Data quality**: a small rule-based validator framework built on top of pandas boolean masks
- **Runtime**: Docker Compose  4 containers (Airflow webserver, Airflow scheduler, metadata Postgres, warehouse Postgres)

---


## Notes on the synthetic data

Data is generated by `generate_synthetic_data()` in `extract.py` for 1000 orders, 10 warehouses, 20 partners, 50 routes, and 30 days of history, with intentional quality issues baked in: mixed date formats, inconsistent status casing, ~5% null partner IDs, ~10% missing actual delivery dates, duplicate order rows, and a handful of physically invalid (negative) delivery durations , so the validate/transform stages have real problems to solve, not clean input pretending to need cleaning.
