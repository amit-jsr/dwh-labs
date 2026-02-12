"""
SCD Type 1 using persistent DuckDB and MERGE command.

How it works:
- Connects to persistent database.
- Loads CDC from `data/cdc/cdc_*.csv` into staging table
- Applies changes using MERGE (inserts/updates) and DELETE
- Drops staging table after merge

Notes: 
    We use MERGE for set-based operations. 
    Initial data load is done by database.py.
"""

import duckdb
import os
from database import (
    create_stage_tables,
    load_cdc_to_stage,
    drop_stage_tables,
)


def run_scd1():
    here = os.path.dirname(__file__)
    data_dir = os.path.join(here, "data")
    cdc_folder = os.path.join(data_dir, "cdc")

    # Connect to persistent database (already initialized with source data by database.py)
    con = duckdb.connect(database='data/warehouse.duckdb')

    # Create staging table for CDC
    create_stage_tables(con)

    print("Before CDC (SCD1 target):")
    print(con.execute("SELECT * FROM scd1_target ORDER BY customer_id").fetchdf())

    # Load all CDC files into staging
    load_cdc_to_stage(con, cdc_folder)

    print("\nCDC Stage table contents:")
    print(con.execute("SELECT * FROM cdc_stage ORDER BY change_ts").fetchdf())

    # Handle deletes from staging
    con.execute(
        """
        DELETE FROM scd1_target
        WHERE customer_id IN (SELECT customer_id FROM cdc_stage WHERE op = 'D')
        """
    )

    # For SCD1, use MERGE to apply inserts/updates from staging
    con.execute(
        """
        MERGE INTO scd1_target t
        USING (
            SELECT customer_id, name, email, city, created_at, updated_at
            FROM cdc_stage 
            WHERE op IN ('I','U')
        ) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN
            UPDATE SET 
                name = s.name, 
                email = s.email, 
                city = s.city,
                updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, city, created_at, updated_at) 
            VALUES (s.customer_id, s.name, s.email, s.city, s.created_at, s.updated_at)
        """
    )

    print('\nAfter applying CDC (SCD1 semantics - overwrite):')
    print(con.execute("SELECT * FROM scd1_target ORDER BY customer_id").fetchdf())

    # Drop staging table after merge
    drop_stage_tables(con)
    print("\nStaging table dropped.")

    con.close()
    print("SCD1 processing complete.")


if __name__ == "__main__":
    run_scd1()
