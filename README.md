# 🏭 FMCG Medallion Pipeline on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621.svg?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8.svg?style=for-the-badge&logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C.svg?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E.svg?style=for-the-badge&logo=amazons3&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-003B57?style=for-the-badge&logo=postgresql&logoColor=white)

## 📌 Project Overview
A production-style **Medallion (Bronze/Silver/Gold) lakehouse pipeline** built on Databricks and Delta Lake, simulating a real post-acquisition scenario: consolidating a newly acquired subsidiary's daily sales data into a parent company's monthly FMCG reporting model.

**Business Goal:** After an acquisition, leadership needed one consolidated, trustworthy sales report — but the two companies used different product IDs, customer IDs, and reporting grains (daily vs. monthly). This pipeline automates that consolidation so finance and sales ops get accurate monthly numbers without manual reconciliation.

---

## 🏗️ Architecture

```text
Daily CSV files (S3 landing zone)
        │
        ▼
   Bronze  →  raw ingestion, file metadata, audit history
        │
        ▼
   Silver  →  cleaned, deduplicated, IDs mapped to parent product codes
        │
        ▼
Gold (Child)   →  daily subsidiary fact table
        │
        ▼
Gold (Parent)  →  monthly consolidated fact table (Delta MERGE, month-aware recalculation)
        │
        ▼
BI View  →  fmcg.gold.vw_fact_orders_enriched (joined with date/customer/product/price dims)
```

![Star Schema](./star_schema.png)

---

## 📊 Data Modeling

| Layer | Table | Grain | Purpose |
|---|---|---|---|
| Bronze | `fmcg.bronze.orders` | Raw row | Append-only raw history |
| Silver | `fmcg.silver.orders` | Clean daily order | Deduplicated, standardized, IDs resolved |
| Gold (Child) | `fmcg.gold.sb_fact_orders` | Daily | Subsidiary sales fact |
| Gold (Parent) | `fmcg.gold.fact_orders` | Monthly | Consolidated parent-company fact |
| Gold View | `vw_fact_orders_enriched` | Monthly | BI-ready star schema view |

---

## ⚙️ Key Engineering Decisions

- **Delta MERGE upserts** — Silver and Gold writes use `MERGE` instead of blind appends, making the pipeline idempotent and safe to rerun on duplicate files or late corrections.
- **Staging tables for incremental isolation** — each layer writes the current batch to a staging table so incremental runs only touch new data, not full history.
- **Month-aware recalculation** — the subsidiary sends daily data, but the parent reports monthly. Each incremental run detects which calendar months were touched and re-aggregates only those months, avoiding a full historical refresh.
- **Data quality handling** — multiple date formats (including weekday-prefixed strings), invalid/non-numeric customer IDs, missing quantities, and duplicate rows are all resolved in Silver rather than silently dropped where possible (invalid IDs are mapped to a visible sentinel value instead of discarded).
- **Change Data Feed enabled** on all Delta tables for future incremental/streaming consumers.

---

## 📈 Results

| Metric | Full Load | Incremental Load |
|---|---:|---:|
| Raw rows ingested | 51,810 | 9,967 |
| Rows after cleaning | ~40,811 | 7,834 |
| Monthly periods affected | Jul–Nov 2025 (3,060 rows) | Dec 2025 only |
| Full history reprocessed? | N/A | No |

---

## 🗂️ Repository Structure

```text
.
├── README.md
├── notebooks
│   ├── 1_setup                  # catalog, schema, date dimension setup
│   ├── 2_dimensional_modeling   # customers, products, pricing dims
│   ├── 3_fact_modeling          # full load + incremental fact pipeline
│   └── denormalized.sql         # BI-ready gold view
```

---

## 🚀 How to Run

1. **Setup:** run `notebooks/1_setup/setup_catalogs.py` and `dim_date_table_creation.py`
2. **Dimensions:** run the three notebooks in `2_dimensional_modeling/`
3. **Facts:** run `1_full_load_fact.ipynb` once, then `2_incremental_load_fact.ipynb` for each new batch

Requires a Databricks workspace with Unity Catalog enabled and an S3-style landing zone for source CSVs.

---

## 💡 What This Demonstrates

End-to-end data engineering ownership: medallion architecture, Delta Lake upserts, dimensional modeling, incremental/idempotent batch processing, data quality remediation, and BI-ready delivery — the kind of pipeline needed to support finance, sales ops, and executive reporting after a real-world business event like an acquisition.
