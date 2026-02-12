"""
SCD Type 2 using persistent DuckDB and set-based SQL.

How it works:
- Connects to persistent database.
- Loads CDC from `data/cdc/customers_cdc*.csv` into staging table
- Applies CDC where operations I/U/D occur:
  - For updates (U): expires the current row (sets effective_to and is_current=false) and inserts a new row
  - For inserts (I): inserts a new current row
  - For deletes (D): expires the current row
- Drops staging table after merge

Notes: 
    We use set-based SQL (UPDATE/INSERT) to implement SCD2. 
    Initial data load is done by database.py.
"""

import duckdb
import os
from database import (
    create_stage_tables,
    load_cdc_to_stage,
    drop_stage_tables,
)


def run_scd2():
    here = os.path.dirname(__file__)
    data_dir = os.path.join(here, "data")
    cdc_folder = os.path.join(data_dir, "cdc")

    # Connect to persistent database (already initialized with source data by database.py)
    con = duckdb.connect(database='data/warehouse.duckdb')

    # Create staging table for CDC
    create_stage_tables(con)

    print("Before CDC (SCD2 target):")
    print(con.execute("SELECT * FROM scd2_target ORDER BY customer_id, effective_from").fetchdf())

    # Load all CDC files into staging
    load_cdc_to_stage(con, cdc_folder)

    print("\nCDC Stage table contents:")
    print(con.execute("SELECT * FROM cdc_stage ORDER BY change_ts").fetchdf())

    # Expire current rows for updates where values actually changed
    con.execute(
        """
        UPDATE scd2_target
        SET effective_to = stg.change_ts, is_current = false, updated_at = stg.change_ts
        FROM (
            SELECT customer_id, name AS new_name, email AS new_email, city AS new_city, 
                   CAST(change_ts AS TIMESTAMP) as change_ts 
            FROM cdc_stage WHERE op = 'U'
        ) stg
        WHERE scd2_target.customer_id = stg.customer_id
          AND scd2_target.is_current = TRUE
          AND (scd2_target.name IS DISTINCT FROM stg.new_name 
               OR scd2_target.email IS DISTINCT FROM stg.new_email 
               OR scd2_target.city IS DISTINCT FROM stg.new_city)
        """
    )

    # Insert new versions for updates (where row was expired)
    con.execute(
        """
        INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current, created_at, updated_at)
        SELECT s.customer_id, s.name, s.email, s.city, 
               CAST(s.change_ts AS TIMESTAMP), NULL, TRUE, s.created_at, s.updated_at
        FROM cdc_stage s
        WHERE s.op = 'U'
          AND NOT EXISTS (
              SELECT 1 FROM scd2_target t 
              WHERE t.customer_id = s.customer_id AND t.is_current = TRUE
          )
        """
    )

    # Handle inserts: insert new current rows
    con.execute(
        """
        INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current, created_at, updated_at)
        SELECT s.customer_id, s.name, s.email, s.city, 
               CAST(s.change_ts AS TIMESTAMP), NULL, TRUE, s.created_at, s.updated_at
        FROM cdc_stage s
        WHERE s.op = 'I'
        """
    )

    # Handle deletes: expire current rows
    con.execute(
        """
        UPDATE scd2_target
        SET effective_to = stg.change_ts, is_current = false, updated_at = stg.change_ts
        FROM (
            SELECT customer_id, CAST(change_ts AS TIMESTAMP) as change_ts 
            FROM cdc_stage WHERE op = 'D'
        ) stg
        WHERE scd2_target.customer_id = stg.customer_id
          AND scd2_target.is_current = TRUE
        """
    )

    print('\nAfter applying CDC (SCD2 semantics):')
    print(con.execute("SELECT * FROM scd2_target ORDER BY customer_id, effective_from").fetchdf())

    # Drop staging table after merge
    drop_stage_tables(con)
    print("\nStaging table dropped.")

    con.close()
    print("SCD2 processing complete.")


if __name__ == "__main__":
    run_scd2()
