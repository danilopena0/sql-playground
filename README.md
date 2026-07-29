# sql-playground

A hands-on SQL notebook built around a synthetic **healthcare claims**
dataset, going from basics to expert-level querying. It's modeled after the
kind of schema and question types that show up in data analytics / data
science interviews at healthcare payers: member enrollment, provider /
physician networks, claim adjudication, and fraud triage.

Everything runs on [DuckDB](https://duckdb.org/), which lets you write plain
SQL directly against in-memory pandas DataFrames — no database server to
stand up.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
jupyter lab claims_sql_playground.ipynb
```

Run the setup cell at the top of the notebook, then work through it top to
bottom — "Run All" also works, every cell is idempotent.

## What's in here

- **`data_gen.py`** — generates the synthetic dataset (members, providers,
  physicians, diagnoses, procedures, claims, claim_lines) as related pandas
  DataFrames, seeded for reproducibility. Can also be run standalone
  (`python data_gen.py`) to print row counts.
- **`claims_sql_playground.ipynb`** — the actual learning notebook. Ten
  sections, basics through expert:
  1. SQL Basics (`SELECT`, `WHERE`, `ORDER BY`, `DISTINCT`, NULLs)
  2. Aggregation (`GROUP BY`, `HAVING`, `CASE WHEN`)
  3. Joins (inner, left/anti-join, self-join)
  4. Grain & Fan-Out Traps — a deliberate one-to-many table
     (`claim_lines`) so you can reproduce and fix the classic
     join-inflates-your-totals bug
  5. Subqueries & CTEs (correlated subqueries, `IN` vs `EXISTS`, `WITH`)
  6. Date & String Functions
  7. Window Functions (`ROW_NUMBER`/`RANK`, running totals, `LAG`/`LEAD`,
     `NTILE`, the "latest record per group" pattern)
  8. Expert Techniques (recursive CTEs, `PIVOT`, set operations, `QUALIFY`,
     reading an `EXPLAIN` plan)
  9. Fraud & Anomaly Detection — a progression of increasingly refined
     outlier-flagging queries (fixed-multiplier -> z-score -> peer-group
     comparison -> percentile -> a multi-signal composite score)
  10. SQL vs. Python — where each language actually shines, demonstrated
      side-by-side on the same data (including handing off to
      `scikit-learn` for multivariate anomaly detection)

Every query is preceded by a short explanation and followed by **Pros /
Cons**, so the notebook teaches not just *how* to write each query but
*when* to reach for it.
