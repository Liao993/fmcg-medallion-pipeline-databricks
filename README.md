# FMCG Medallion Pipeline — Databricks & Delta Lake

> Consolidating a post-acquisition subsidiary's daily sales data into a parent company's monthly reporting layer using a production-grade medallion lakehouse on Databricks.

**Stack:** Python · PySpark · Delta Lake · Amazon S3 · Databricks · SQL · Databricks Genie

---

## The Problem

A large FMCG parent company acquired a smaller retail chain (SportsBar). Both companies run separate OLTP systems — different date formats, customer ID schemas, and product code conventions. Leadership needed one consolidated view of monthly sales volumes across all customers and products from Day 1 of the acquisition.

The challenge wasn't just moving data. The acquired company's files landed as daily CSVs with real-world data quality issues: invalid customer IDs, four different date format conventions, NULL quantities, and product codes that only resolved through a reference join. And the parent's gold table aggregates at **monthly grain** — meaning every new daily batch potentially requires recalculating that month's totals, not just appending rows.

---

## Architecture

```
S3 Landing Zone (daily CSV drops)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE LAYER                                       │
│  Append raw rows + capture file metadata            │
│  fmcg.bronze.orders          ← append (full history)│
│  fmcg.bronze.staging_orders  ← overwrite (batch only)│
└──────────────────────┬──────────────────────────────┘
                       │  read from staging
                       ▼
┌─────────────────────────────────────────────────────┐
│  SILVER LAYER  (5 transformations applied)          │
│  fmcg.silver.orders          ← Delta MERGE (upsert) │
│  fmcg.silver.staging_orders  ← overwrite (batch only)│
└──────────────────────┬──────────────────────────────┘
                       │  read from staging
                       ▼
┌─────────────────────────────────────────────────────┐
│  GOLD LAYER — CHILD (SportsBar)                     │
│  fmcg.gold.sb_fact_orders    ← Delta MERGE, daily   │
└──────────────────────┬──────────────────────────────┘
                       │  month-aware re-aggregation
                       ▼
┌─────────────────────────────────────────────────────┐
│  GOLD LAYER — PARENT (Consolidated)                 │
│  fmcg.gold.fact_orders       ← Delta MERGE, monthly │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
              BI Dashboard / Databricks Genie
```

### Data Layer Reference

| Layer | Table | Grain | Write Mode |
|---|---|---|---|
| Bronze | `fmcg.bronze.orders` | Raw row, append-only | Append + Change Data Feed |
| Bronze Staging | `fmcg.bronze.staging_orders` | Current batch only | Overwrite |
| Silver | `fmcg.silver.orders` | Cleaned daily row | Delta MERGE (upsert) |
| Silver Staging | `fmcg.silver.staging_orders` | Current batch only | Overwrite |
| Gold Child | `fmcg.gold.sb_fact_orders` | Daily, denormalized | Delta MERGE (upsert) |
| Gold Parent | `fmcg.gold.fact_orders` | Monthly aggregated | Delta MERGE (upsert) |

---

## Engineering Decisions

### 1. Staging tables for incremental isolation
Every layer writes an **overwrite-mode staging table** containing only the current batch. All downstream steps read from staging rather than the full historical table. This keeps processing costs proportional to batch size, not accumulated history — a meaningful difference when the bronze table grows to millions of rows across months of daily files.

### 2. Delta MERGE for idempotent upserts
Silver and gold writes use `whenMatchedUpdateAll / whenNotMatchedInsertAll` keyed on composite business keys. The pipeline is safe to re-run: duplicate file drops or late-arriving corrections update existing rows instead of creating duplicates. The if-else table existence check (`spark.catalog.tableExists`) means both notebooks work in a fresh environment with no manual setup.

### 3. Month-aware incremental rollup to the parent table
This was the trickiest design decision. The parent stores data at **monthly grain**, but the child receives **daily data**. Simply appending new daily rows would undercount the month if that month already had rows. The solution:
1. Read silver staging to identify which calendar months the current batch touches
2. Pull **all daily gold rows for those months** from the child gold table
3. Re-aggregate from scratch to monthly totals
4. Merge corrected monthly figures into the parent

This avoids both stale aggregates and full-history recomputes — only affected months are recalculated.

### 4. Change Data Feed enabled on all Delta tables
`delta.enableChangeDataFeed = true` is set at table creation on every layer. This enables downstream consumers to read incremental changes without full scans — important for future Structured Streaming integrations or CDC-based pipelines built on top of this lakehouse.

---

## Silver Transformations

The subsidiary's raw data required five cleaning steps before it could be joined to the parent's dimension tables:

| # | Issue | Fix Applied |
|---|---|---|
| 1 | Missing `order_qty` | Filter — rows with NULL quantity dropped |
| 2 | Non-numeric customer IDs (`INVALID`, `ABC987`) | Sentinel value `999999` — revenue preserved, flagged for review |
| 3 | Date includes weekday name (`"Tuesday, July 01, 2025"`) | Regex strip of leading `Weekday, ` prefix |
| 4 | Four different date formats across files | `F.coalesce(F.try_to_date(...))` across all four patterns |
| 5 | Exact duplicate rows | `dropDuplicates` on composite key |

After cleaning: inner join to `fmcg.silver.products` resolves `product_id` → `product_code` (SHA-256 hash used as the parent's canonical product identifier).

---

## Notebooks

```
consolidated_pipeline/
├── 1_setup/
│   └── utilities              # %run target — defines bronze/silver/gold schema names
├── dimensions/
│   ├── 1_full_load_customers
│   ├── 2_incremental_load_customers
│   ├── 1_full_load_products
│   ├── 2_incremental_load_products
│   ├── 1_full_load_prices
│   └── 2_incremental_load_prices
└── facts/
    ├── 1_full_load_fact       # Bootstrap Jul–Nov 2025 history
    └── 2_incremental_load_fact # Scheduled incremental runs
```

Each entity has two notebooks: `1_full_load` creates and populates tables from historical data; `2_incremental_load` processes new batches and merges into the same tables. The incremental notebook includes a table existence check so it also works as a standalone bootstrap in a fresh workspace.

---

## Pipeline Run Results

**Full Load (Jul–Nov 2025)**
- Raw rows ingested: 51,810
- After silver cleaning: ~40,811 clean rows
- Monthly aggregates in parent gold: 3,060 rows (5 months × ~600 customer-product combinations)

**Incremental Load (December 2025 batch)**
- Raw rows ingested: 8,834
- After silver cleaning: 6,947 clean rows
- Monthly aggregates recalculated: 612 rows (December only — July–November untouched)
- Data quality flags: ~16% drop rate (NULL qty + unresolvable product joins); ~1.5% customer ID sanitization

---

## How to Run

### Prerequisites
- Databricks workspace (Free Edition works)
- S3 bucket with `landing/` and `processed/` prefixes per data source
- Unity Catalog with `fmcg` catalog and `bronze`, `silver`, `gold` schemas

### Setup
1. Upload all notebooks to `/Workspace/consolidated_pipeline/`
2. Configure the `utilities` notebook with your schema names
3. Mount or configure S3 access credentials in Databricks secrets

### Execution Order
```
# First time (or full reload):
1_full_load_customers → 1_full_load_products → 1_full_load_prices → 1_full_load_fact

# Each subsequent batch (scheduled or triggered):
2_incremental_load_customers → 2_incremental_load_products → 2_incremental_load_prices → 2_incremental_load_fact
```

### Widgets
Each notebook accepts two widgets:
- `catalog` — default `fmcg`
- `data_source` — default `orders` (or `customers`, `products`, `prices` for dimensions)

---

## Key Learnings

**Medallion staging pattern scales better than it looks.** Writing a staging table adds one extra Delta write per layer, but it unlocks clean incremental isolation without Auto Loader or Structured Streaming. For batch pipelines where files land on a schedule, this is a pragmatic middle ground between full-table reprocessing and stream-based CDC.

**Monthly rollup requires re-aggregation, not just append.** This was the non-obvious design challenge. Append works fine if the target grain matches the source grain. When it doesn't — daily data feeding a monthly table — you need to identify affected periods and recalculate. The incremental months temp view pattern handles this cleanly.

**Delta MERGE composite keys need to be chosen carefully.** Using only `order_id` as the merge key would fail when the same order appears for multiple products. The composite key `(order_id, date, product_code, customer_id)` ensures one row per order line, matching how the source OLTP system treats order data.