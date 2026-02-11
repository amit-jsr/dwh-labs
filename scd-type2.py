"""
SCD Type 2 demo using in-memory DuckDB.

How it works:
- Loads an initial full load from `data/source/source_customers.csv` into a type-2 target
- Loads CDC from `data/cdc/cdc_customers.csv` into staging table
- Applies CDC where operations I/U/D occur:
  - For updates (U): expires the current row (sets effective_to and is_current=false) and inserts a new row
  - For inserts (I): inserts a new current row
  - For deletes (D): expires the current row
- Drops staging table after merge

Notes: We use set-based SQL (UPDATE/INSERT) to implement SCD2. DuckDB is used in-memory.
"""

import duckdb
import os
from datetime import datetime
from database import scd2_ddl, scd2_stage_ddl


def run_scd2():
    here = os.path.dirname(__file__)
    data_dir = os.path.join(here, "data")
    source_csv = os.path.join(data_dir, "source", "source_customers.csv")
    cdc_csv = os.path.join(data_dir, "cdc", "cdc_customers.csv")

    con = duckdb.connect(database=':memory:')

    # Create SCD2 target table using DDL from database.py
    con.execute(scd2_ddl())

    # Load initial source and insert as current records
    con.execute(f"CREATE TEMP TABLE src AS SELECT * FROM read_csv_auto('{source_csv}')")
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    con.execute(
        """
        INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current)
        SELECT customer_id, name, email, city, TIMESTAMP '2023-07-01 00:00:00', NULL, true FROM src
        """
    )

    print("After initial SCD2 load:")
    print(con.execute("SELECT * FROM scd2_target ORDER BY customer_id, effective_from").fetchdf())

    # Create CDC staging table and load CDC data
    con.execute(scd2_stage_ddl())
    con.execute(f"INSERT INTO scd2_stage SELECT * FROM read_csv_auto('{cdc_csv}')")

    print("\nCDC Stage table contents:")
    print(con.execute("SELECT * FROM scd2_stage").fetchdf())

    # Expire current rows for updates where values actually changed
    con.execute(
        """
        UPDATE scd2_target
        SET effective_to = stg.change_ts, is_current = false
        FROM (SELECT customer_id, name AS new_name, email AS new_email, city AS new_city, CAST(change_ts AS TIMESTAMP) as change_ts FROM scd2_stage WHERE op = 'U') stg
        WHERE scd2_target.customer_id = stg.customer_id
          AND scd2_target.is_current
          AND (scd2_target.name IS DISTINCT FROM stg.new_name OR scd2_target.email IS DISTINCT FROM stg.new_email OR scd2_target.city IS DISTINCT FROM stg.new_city)
        """
    )

    # Insert new versions for updates and inserts
    con.execute(
        """
        INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current)
        SELECT s.customer_id, s.name, s.email, s.city, CAST(s.change_ts AS TIMESTAMP), NULL, true
        FROM scd2_stage s
        LEFT JOIN scd2_target t ON t.customer_id = s.customer_id AND t.is_current
        WHERE s.op IN ('I','U')
          AND (
                t.customer_id IS NULL
                OR (t.customer_id IS NOT NULL AND (t.name IS DISTINCT FROM s.name OR t.email IS DISTINCT FROM s.email OR t.city IS DISTINCT FROM s.city))
              )
        """
    )

    # Handle deletes: expire current rows
    con.execute(
        """
        UPDATE scd2_target
        SET effective_to = stg.change_ts, is_current = false
        FROM (SELECT customer_id, CAST(change_ts AS TIMESTAMP) as change_ts FROM scd2_stage WHERE op = 'D') stg
        WHERE scd2_target.customer_id = stg.customer_id
          AND scd2_target.is_current
        """
    )

    print('\nAfter applying CDC (SCD2 semantics):')
    print(con.execute("SELECT * FROM scd2_target ORDER BY customer_id, effective_from").fetchdf())

    # Drop staging table after merge
    con.execute("DROP TABLE IF EXISTS scd2_stage")
    print("\nStaging table dropped after merge.")


if __name__ == "__main__":
    run_scd2()
