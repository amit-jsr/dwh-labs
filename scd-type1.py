"""
Simple SCD Type 1 demo using in-memory DuckDB and MERGE semantics.

How it works:
- Loads `data/source/source_customers.csv` as the source (full load)
- Creates target table and performs initial load
- Loads CDC from `data/cdc/cdc_*.csv` into staging table
- Applies changes using MERGE (inserts/updates) and DELETE
- Drops staging table after merge

Notes: SCD1 semantics means we overwrite existing values on updates. We use MERGE for set-based operations. DuckDB is used in-memory."""

import duckdb
import os
from database import (
    create_tables,
    create_stage_tables,
    load_source_to_target,
    load_cdc_to_stage,
    drop_stage_tables,
)


def run_scd1():
    here = os.path.dirname(__file__)
    data_dir = os.path.join(here, "data")
    source_csv = os.path.join(data_dir, "source", "source_customers.csv")
    cdc_folder = os.path.join(data_dir, "cdc")

    con = duckdb.connect(database=':memory:')

    # Create target and staging tables
    create_tables(con)
    create_stage_tables(con)

    # Load the initial source data into target
    load_source_to_target(con, source_csv)

    print("After initial load (SCD1 target):")
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
    print("\nStaging table dropped after merge.")

    return con


if __name__ == "__main__":
    run_scd1()
