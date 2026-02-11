"""
Minimal test suite for SCD Type 1 and SCD Type 2 implementations.
"""

import duckdb
import os
import pytest
from database import (
    create_tables,
    create_stage_tables,
    load_source_to_target,
    load_cdc_to_stage,
    drop_stage_tables,
    drop_all_tables,
)


@pytest.fixture
def conn():
    """Fresh in-memory DuckDB connection."""
    con = duckdb.connect(database=':memory:')
    yield con
    con.close()


@pytest.fixture
def data_dir():
    """Data directory path."""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def setup_all(conn, data_dir):
    """Setup tables and load all data."""
    create_tables(conn)
    create_stage_tables(conn)
    load_source_to_target(conn, os.path.join(data_dir, "source", "source_customers.csv"))
    load_cdc_to_stage(conn, os.path.join(data_dir, "cdc"))
    return conn


class TestSetup:
    """Test database setup functions."""

    def test_create_and_drop_tables(self, conn):
        create_tables(conn)
        create_stage_tables(conn)
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        assert "scd1_target" in tables
        assert "scd2_target" in tables
        assert "cdc_stage" in tables
        
        drop_all_tables(conn)
        assert conn.execute("SHOW TABLES").fetchall() == []


class TestDataLoading:
    """Test data loading functions."""

    def test_load_source_and_cdc(self, conn, data_dir):
        create_tables(conn)
        create_stage_tables(conn)
        
        src_count = load_source_to_target(conn, os.path.join(data_dir, "source", "source_customers.csv"))
        cdc_count = load_cdc_to_stage(conn, os.path.join(data_dir, "cdc"))
        
        assert src_count == 20
        assert cdc_count == 14


class TestSCD1:
    """Test SCD Type 1 operations."""

    def test_scd1_full_merge(self, setup_all):
        conn = setup_all
        
        # Delete
        conn.execute("DELETE FROM scd1_target WHERE customer_id IN (SELECT customer_id FROM cdc_stage WHERE op = 'D')")
        
        # Insert/Update
        conn.execute("""
            MERGE INTO scd1_target t
            USING (SELECT customer_id, name, email, city, created_at, updated_at FROM cdc_stage WHERE op IN ('I', 'U')) s
            ON t.customer_id = s.customer_id
            WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email, city = s.city, updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.name, s.email, s.city, s.created_at, s.updated_at)
        """)
        
        # 20 - 3 deletes + 5 inserts = 22
        assert conn.execute("SELECT COUNT(*) FROM scd1_target").fetchone()[0] == 22
        # Verify update (customer 2: Delhi -> Gurgaon)
        assert conn.execute("SELECT city FROM scd1_target WHERE customer_id = 2").fetchone()[0] == "Gurgaon"
        # Verify delete (customer 3 removed)
        assert conn.execute("SELECT COUNT(*) FROM scd1_target WHERE customer_id = 3").fetchone()[0] == 0


class TestSCD2:
    """Test SCD Type 2 operations."""

    def test_scd2_full_merge(self, setup_all):
        conn = setup_all
        
        # Soft delete
        conn.execute("""
            UPDATE scd2_target SET effective_to = s.change_ts, is_current = FALSE
            FROM (SELECT customer_id, change_ts FROM cdc_stage WHERE op = 'D') s
            WHERE scd2_target.customer_id = s.customer_id AND scd2_target.is_current = TRUE
        """)
        
        # Close for updates
        conn.execute("""
            UPDATE scd2_target SET effective_to = s.change_ts, is_current = FALSE
            FROM (SELECT customer_id, MIN(change_ts) as change_ts FROM cdc_stage WHERE op = 'U' GROUP BY customer_id) s
            WHERE scd2_target.customer_id = s.customer_id AND scd2_target.is_current = TRUE
        """)
        
        # Insert new versions (updates + inserts)
        conn.execute("""
            INSERT INTO scd2_target (customer_id, name, email, city, effective_from, effective_to, is_current, created_at, updated_at)
            SELECT customer_id, name, email, city, change_ts, NULL, TRUE, created_at, updated_at
            FROM cdc_stage WHERE op IN ('I', 'U')
        """)
        
        # 20 + 6 updates + 5 inserts = 31 total
        assert conn.execute("SELECT COUNT(*) FROM scd2_target").fetchone()[0] == 31
        # Customer 2 has 2 records (history preserved)
        assert conn.execute("SELECT COUNT(*) FROM scd2_target WHERE customer_id = 2").fetchone()[0] == 2
        # Deleted customer 3 still exists but closed
        assert conn.execute("SELECT is_current FROM scd2_target WHERE customer_id = 3").fetchone()[0] == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
