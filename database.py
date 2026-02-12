"""DDL helpers for SCD target and staging tables."""

import glob
from typing import Any


def scd1_target_ddl() -> str:
    """DDL for SCD1 target table."""
    return """
        CREATE TABLE IF NOT EXISTS scd1_target (
            customer_id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            city VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """


def scd2_target_ddl() -> str:
    """DDL for SCD2 target table."""
    return """
        CREATE TABLE IF NOT EXISTS scd2_target (
            surrogate_id INTEGER PRIMARY KEY DEFAULT nextval('scd2_seq'),
            customer_id INTEGER,
            name VARCHAR,
            email VARCHAR,
            city VARCHAR,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """


def cdc_stage_ddl() -> str:
    """DDL for CDC staging table."""
    return """
        CREATE TABLE IF NOT EXISTS cdc_stage (
            customer_id INTEGER,
            name VARCHAR,
            email VARCHAR,
            city VARCHAR,
            op VARCHAR,
            change_ts TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """


def create_tables(conn: Any) -> None:
    """Create SCD1 and SCD2 target tables."""
    conn.execute(scd1_target_ddl())
    conn.execute("CREATE SEQUENCE IF NOT EXISTS scd2_seq START 1")
    conn.execute(scd2_target_ddl())


def create_stage_tables(conn: Any) -> None:
    """Create CDC staging table."""
    conn.execute(cdc_stage_ddl())


def load_cdc_to_stage(conn: Any, cdc_folder: str = "data/cdc") -> int:
    """Load all CDC CSV files from folder into staging table.

    Reads all customers_cdc*.csv files and inserts into cdc_stage.
    Keeps only latest record per customer by change_ts.
    Returns the number of records loaded.
    """
    cdc_files = sorted(glob.glob(f"{cdc_folder}/customers_cdc*.csv"))

    if not cdc_files:
        return 0

    # Build UNION ALL of all CDC files
    file_queries = [f"SELECT * FROM read_csv_auto('{f}')" for f in cdc_files]
    union_query = " UNION ALL ".join(file_queries)

    # Load with deduplication - keep only latest record per customer
    conn.execute(f"""
        INSERT INTO cdc_stage
        SELECT customer_id, name, email, city, op, change_ts, created_at, updated_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_ts DESC) as rn
            FROM ({union_query})
        )
        WHERE rn = 1
    """)

    total_records = conn.execute("SELECT COUNT(*) FROM cdc_stage").fetchone()[0]
    print(f"Loaded {total_records} CDC records")

    return total_records


def load_source_to_target(conn: Any, source_file: str = "data/source/customers.csv") -> int:
    """Load initial source data into target tables.

    Returns the number of records loaded.
    """
    # Load into SCD1 target
    conn.execute(f"""
        INSERT INTO scd1_target (customer_id, name, email, city, created_at, updated_at)
        SELECT customer_id, name, email, city, created_at, updated_at
        FROM read_csv_auto('{source_file}')
    """)

    # Load into SCD2 target with initial effective dates
    conn.execute(f"""
        INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current, created_at, updated_at)
        SELECT customer_id, name, email, city, created_at, NULL, TRUE, created_at, updated_at
        FROM read_csv_auto('{source_file}')
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{source_file}')").fetchone()[0]
    print(f"Loaded {count} source records into target tables")
    return count


def drop_stage_tables(conn: Any) -> None:
    """Drop staging table."""
    conn.execute("DROP TABLE IF EXISTS cdc_stage")


def drop_all_tables(conn: Any) -> None:
    """Drop all tables and sequences."""
    conn.execute("DROP TABLE IF EXISTS cdc_stage")
    conn.execute("DROP TABLE IF EXISTS scd1_target")
    conn.execute("DROP TABLE IF EXISTS scd2_target")
    conn.execute("DROP SEQUENCE IF EXISTS scd2_seq")


if __name__ == "__main__":
    import duckdb

    con = duckdb.connect(database='data/warehouse.duckdb')

    # Drop existing tables for clean start
    drop_all_tables(con)
    print("Dropped existing tables")

    # Create target and staging tables
    create_tables(con)
    create_stage_tables(con)
    print("Created target and staging tables")

    # Load initial source data
    source_csv = "data/source/customers.csv"
    load_source_to_target(con, source_csv)

    # Show record counts
    scd1_count = con.execute("SELECT COUNT(*) FROM scd1_target").fetchone()[0]
    scd2_count = con.execute("SELECT COUNT(*) FROM scd2_target").fetchone()[0]
    print(f"SCD1 target: {scd1_count} records")
    print(f"SCD2 target: {scd2_count} records")

    con.close()
    print("Database initialization complete.")