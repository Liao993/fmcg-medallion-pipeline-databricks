# FMCG Medallion Pipeline on Databricks

Portfolio project: a production-style lakehouse pipeline that consolidates a newly acquired subsidiary's daily sales data into the parent company's monthly FMCG reporting model.

The project demonstrates end-to-end data engineering work: raw file ingestion, medallion architecture, data quality handling, Delta Lake upserts, dimensional modeling, monthly fact aggregation, and BI-ready gold views.

---

## Project Summary

A parent FMCG company acquired a retail chain called SportsBar. Both companies had different operational data formats, product identifiers, customer identifiers, and reporting grains. Leadership needed consolidated monthly sales reporting immediately after the acquisition.

I built a Databricks and Delta Lake pipeline that:

- Ingests daily CSV order files from an S3-style landing zone.
- Preserves raw source data in a Bronze layer with file metadata.
- Cleans and standardizes messy subsidiary data in Silver.
- Resolves subsidiary product IDs to the parent company's product codes.
- Builds a daily subsidiary fact table.
- Re-aggregates only affected months into the parent company's monthly gold fact table.
- Produces a BI-ready enriched view for dashboarding and Genie-style analytics.

---

## Skills Demonstrated

| Area | What I Built |
|---|---|
| Data Engineering | Batch ETL pipeline using PySpark notebooks in Databricks |
| Lakehouse Architecture | Bronze, Silver, and Gold layers with Delta tables |
| Incremental Processing | Staging tables plus Delta MERGE for repeatable batch loads |
| Data Modeling | Dimension tables, child fact table, parent monthly fact table, enriched BI view |
| Data Quality | Date parsing, duplicate removal, null filtering, invalid ID handling |
| Business Logic | Daily subsidiary orders rolled up into monthly parent reporting grain |
| Cloud Data Platform | S3-style landing and processed zones, Unity Catalog schemas |
| Analytics Enablement | SQL view joining facts with date, customer, product, and price dimensions |
| Version Control | Git-based project structure ready for GitHub portfolio sharing |

---

## Business Problem

The acquired company delivered daily order files, while the parent company reported sales at a monthly grain. The source data also contained real-world quality issues:

- Multiple date formats, including weekday-prefixed strings such as `Tuesday, July 01, 2025`
- Non-numeric customer IDs such as `INVALID` and `ABC987`
- Missing order quantities
- Duplicate order lines
- Product IDs that needed to be mapped into parent-company product codes

The hardest part was not only cleaning the data. The pipeline also had to update monthly totals correctly when new daily files arrived. A simple append would create inaccurate monthly reporting, so the pipeline identifies the affected month and recalculates that month only.

---

## Architecture

```text
Daily CSV files
      |
      v
Bronze
  - fmcg.bronze.orders
  - fmcg.bronze.staging_orders
      |
      v
Silver
  - fmcg.silver.orders
  - fmcg.silver.staging_orders
      |
      v
Gold child fact
  - fmcg.gold.sb_fact_orders
  - daily SportsBar sales grain
      |
      v
Gold parent fact
  - fmcg.gold.fact_orders
  - monthly consolidated reporting grain
      |
      v
BI / Analytics
  - fmcg.gold.vw_fact_orders_enriched
```

---

## Data Layers

| Layer | Table | Grain | Purpose |
|---|---|---|---|
| Bronze | `fmcg.bronze.orders` | Raw order row | Append-only historical landing table |
| Bronze Staging | `fmcg.bronze.staging_orders` | Current batch | Isolates each incremental file batch |
| Silver | `fmcg.silver.orders` | Clean daily order row | Standardized, deduplicated order data |
| Silver Staging | `fmcg.silver.staging_orders` | Current batch | Feeds downstream incremental processing |
| Gold Child | `fmcg.gold.sb_fact_orders` | Daily customer/product sales | Subsidiary fact table |
| Gold Parent | `fmcg.gold.fact_orders` | Monthly customer/product sales | Consolidated parent-company reporting |
| Gold View | `fmcg.gold.vw_fact_orders_enriched` | Monthly enriched sales | BI-ready semantic view |

Delta Change Data Feed is enabled when tables are created so future downstream consumers can process changes incrementally.

---

## Pipeline Logic

### 1. Bronze ingestion

The Bronze layer reads raw CSV files and stores the original order data with ingestion metadata:

- `read_timestamp`
- `file_name`
- `file_size`

This creates an auditable raw history while a staging table keeps the current batch isolated for incremental processing.

### 2. Silver cleaning and standardization

The Silver layer applies five main transformations:

| Issue | Solution |
|---|---|
| Missing `order_qty` | Drop rows where quantity is null |
| Invalid customer IDs | Convert non-numeric IDs to sentinel value `999999` |
| Weekday-prefixed dates | Strip weekday prefix with regex |
| Multiple date formats | Parse with `try_to_date` across supported patterns |
| Duplicate rows | Drop duplicates on the order-line business key |

The cleaned order data is then joined to the product dimension so subsidiary product IDs resolve to the parent company's hashed `product_code`.

### 3. Delta MERGE upserts

Silver and Gold writes use Delta Lake `MERGE` logic so the pipeline can be rerun safely. Existing order lines are updated and new order lines are inserted.

This makes the pipeline idempotent, which is important for:

- Duplicate file deliveries
- Late-arriving corrections
- Notebook reruns during development
- Scheduled batch jobs

### 4. Month-aware incremental aggregation

The parent table reports at monthly grain, but the subsidiary sends daily data. For each incremental load, the pipeline:

1. Reads the Silver staging table.
2. Identifies the calendar months touched by the current batch.
3. Pulls all daily child fact rows for those months.
4. Re-aggregates monthly totals from the daily fact table.
5. Merges the corrected monthly totals into `fmcg.gold.fact_orders`.

This avoids full-history recomputation while keeping monthly reporting accurate.

---

## Results

### Historical full load

| Metric | Result |
|---|---:|
| Raw order rows ingested | 51,810 |
| Clean rows after Silver processing | About 40,811 |
| Parent monthly fact rows created | 3,060 |
| Historical coverage | July 2025 to November 2025 |

### Incremental load

| Metric | Result |
|---|---:|
| Raw December rows ingested | 9,967 |
| Daily child fact rows after processing | 7,834 |
| Monthly periods recalculated | December 2025 only |
| Prior months touched | No |

---

## Repository Structure

```text
.
├── README.md
├── notebooks
│   ├── 1_setup
│   │   ├── setup_catalogs.py
│   │   ├── utilities.py
│   │   └── dim_date_table_creation.py
│   ├── 2_dimensional_modeling
│   │   ├── 1_customer_data_processing.ipynb
│   │   ├── 2_products_data_processing.ipynb
│   │   └── 3_pricing_data_processing.ipynb
│   ├── 3_fact_modeling
│   │   ├── 1_full_load_fact.ipynb
│   │   └── 2_incremental_load_fact.ipynb
│   ├── denormalized.sql
│   └── Sales_BI_Insights
├── data
└── doc
```

---

## How to Run

### Prerequisites

- Databricks workspace
- Unity Catalog enabled
- Catalog named `fmcg`
- Schemas named `bronze`, `silver`, and `gold`
- Cloud storage landing zone for source CSV files

### Setup

Run the setup scripts first:

```text
notebooks/1_setup/setup_catalogs.py
notebooks/1_setup/dim_date_table_creation.py
```

Then run the dimension notebooks:

```text
notebooks/2_dimensional_modeling/1_customer_data_processing.ipynb
notebooks/2_dimensional_modeling/2_products_data_processing.ipynb
notebooks/2_dimensional_modeling/3_pricing_data_processing.ipynb
```

Finally run the fact notebooks:

```text
notebooks/3_fact_modeling/1_full_load_fact.ipynb
notebooks/3_fact_modeling/2_incremental_load_fact.ipynb
```

---

## BI-Ready View

The SQL file `notebooks/denormalized.sql` creates:

```sql
fmcg.gold.vw_fact_orders_enriched
```

This view joins the monthly fact table with:

- Date dimension
- Customer dimension
- Product dimension
- Gross price dimension

It also calculates `total_amount_inr` as:

```sql
sold_quantity * price_inr
```

The result is ready for dashboarding, business analysis, and natural-language BI exploration.

---

## Key Design Decisions

### Staging tables for incremental isolation

Each layer writes the current batch to a staging table. Downstream notebooks read from staging instead of scanning the full historical table. This keeps incremental processing focused and easier to debug.

### Delta MERGE for reliable reruns

The pipeline uses Delta MERGE instead of blind appends for curated tables. This prevents duplicates and supports late updates.

### Month-aware recalculation

Because daily subsidiary data feeds a monthly parent table, the pipeline recalculates only the affected months. This keeps monthly reporting correct without a full refresh.

### Sentinel IDs for invalid customers

Invalid customer IDs are converted to `999999` instead of dropping the rows. This preserves sales volume while making the data quality issue visible for review.

---

## What This Project Shows

This project shows that I can design and implement a practical data pipeline, not just write isolated Spark transformations. I handled source-system mismatch, data quality problems, incremental loading, dimensional joins, fact table grain differences, and BI delivery in one complete workflow.

It is designed as a realistic example of the type of work needed in data engineering roles that support analytics, finance, supply chain, sales operations, and executive reporting.
