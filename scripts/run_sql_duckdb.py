"""Run sql/flux_assessment.sql locally on DuckDB to prove the queries work.

The SQL is written for BigQuery. Each `-- @name:` block is transpiled with
sqlglot (BigQuery -> DuckDB) and executed against the pipeline's outputs.
SELECT results are saved to output/sql_results/<name>.csv.

Local-only shims (BigQuery behaviour is unchanged):
  * SAFE.PARSE_DATE / SAFE.PARSE_TIMESTAMP -> DuckDB macros over TRY_STRPTIME
    (sqlglot does not translate the SAFE. prefix on these functions).
  * PARTITION BY / CLUSTER BY / OPTIONS on the DDL are dropped: DuckDB has no
    equivalent, and they affect cost, not results.

Usage:  python scripts/run_sql_duckdb.py   (run the pipeline first)
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import sqlglot

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from flux_pipeline.config import Config, load_config  # noqa: E402
from flux_pipeline.io import discover_batches, read_batch  # noqa: E402

SQL_FILE = ROOT / "sql" / "flux_assessment.sql"
BLOCK_RE = re.compile(r"^-- @name:\s*(\w+)\s*$", re.MULTILINE)
SHIMS = {r"\bSAFE\.PARSE_DATE\(": "bq_safe_parse_date(",
         r"\bSAFE\.PARSE_TIMESTAMP\(": "bq_safe_parse_timestamp("}


def load_tables(con: duckdb.DuckDBPyConnection, cfg: Config) -> None:
    stg = cfg.output_dir / "stg_bookings.csv"
    if not stg.exists():
        sys.exit("output/stg_bookings.csv not found: run `python -m flux_pipeline` first")

    raw = pd.concat([read_batch(b, cfg.encoding) for b in discover_batches(cfg)],
                    ignore_index=True)
    staging = pd.read_csv(stg, parse_dates=["check_in_date", "check_out_date", "created_at",
                                            "updated_at", "_batch_date"])
    staging["check_in_date"] = staging["check_in_date"].dt.date
    staging["check_out_date"] = staging["check_out_date"].dt.date
    # Microsecond precision (BigQuery's) on every pandas version; pandas 2.x
    # defaults to ns, which DuckDB won't compare with the DDL's TIMESTAMP columns.
    for col in ("created_at", "updated_at", "_batch_date"):
        staging[col] = staging[col].astype("datetime64[us]")
    raw["_batch_date"] = raw["_batch_date"].astype("datetime64[us]")
    fx = pd.read_csv(cfg.fx_rates_file, parse_dates=["effective_from"])
    fx["effective_from"] = fx["effective_from"].dt.date
    props = pd.read_csv(cfg.properties_file)
    props["country"] = props["country"].str.strip().replace(cfg.country_map)

    con.execute("CREATE SCHEMA IF NOT EXISTS flux")
    for name, df in {"raw_bookings": raw, "stg_bookings": staging,
                     "fx_rates": fx, "properties": props}.items():
        con.register(f"_{name}", df)
        con.execute(f"CREATE OR REPLACE TABLE flux.{name} AS SELECT * FROM _{name}")
    con.execute("CREATE OR REPLACE MACRO bq_safe_parse_date(fmt, s) AS "
                "CAST(TRY_STRPTIME(s, fmt) AS DATE)")
    con.execute("CREATE OR REPLACE MACRO bq_safe_parse_timestamp(fmt, s) AS "
                "TRY_STRPTIME(s, fmt)")


def blocks(sql: str) -> list[tuple[str, str]]:
    parts = BLOCK_RE.split(sql)
    return [(parts[i], parts[i + 1].strip().rstrip(";")) for i in range(1, len(parts), 2)]


def to_duckdb(bq_sql: str) -> str:
    for pattern, repl in SHIMS.items():
        bq_sql = re.sub(pattern, repl, bq_sql)
    return sqlglot.transpile(bq_sql, read="bigquery", write="duckdb")[0]


def run_all(cfg: Config) -> tuple[duckdb.DuckDBPyConnection, dict[str, pd.DataFrame]]:
    """Load tables, execute every block in order; return the connection + query results."""
    logging.getLogger("sqlglot").setLevel(logging.ERROR)   # silence dropped-DDL notices
    con = duckdb.connect()
    load_tables(con, cfg)
    results = {}
    for name, bq_sql in blocks(SQL_FILE.read_text(encoding="utf-8")):
        con.execute(to_duckdb(bq_sql))
        if bq_sql.lstrip().upper().startswith(("SELECT", "WITH")):
            results[name] = con.fetchdf()
    return con, results


def main() -> None:
    cfg = load_config()
    out_dir = cfg.output_dir / "sql_results"
    out_dir.mkdir(parents=True, exist_ok=True)
    _, results = run_all(cfg)
    for name, result in results.items():
        result.to_csv(out_dir / f"{name}.csv", index=False)
        print(f"\n=== {name} ({len(result)} rows) ===")
        print(result.to_string(index=False))


if __name__ == "__main__":
    main()
