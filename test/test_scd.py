import duckdb

# Connect to the persistent database
con = duckdb.connect(database='data/warehouse.duckdb')

# Query SCD1 target table
print("=== SCD1 Target Table ===")
scd1_results = con.execute("SELECT * FROM scd1_target ORDER BY customer_id").fetchdf()
print(scd1_results)

print("\n=== SCD2 Target Table ===")
# Query SCD2 target table
scd2_results = con.execute("SELECT * FROM scd2_target ORDER BY customer_id, effective_from").fetchdf()
print(scd2_results)

# Query only current records in SCD2
print("\n=== SCD2 Current Records Only ===")
scd2_current = con.execute("""
    SELECT * FROM scd2_target 
    WHERE is_current = TRUE
    ORDER BY customer_id
""").fetchdf()
print(scd2_current)

# Compare record counts
print("\n=== Record Counts ===")
scd1_count = con.execute("SELECT COUNT(*) FROM scd1_target").fetchone()[0]
scd2_total = con.execute("SELECT COUNT(*) FROM scd2_target").fetchone()[0]
scd2_current_count = con.execute("SELECT COUNT(*) FROM scd2_target WHERE is_current = TRUE").fetchone()[0]

print(f"SCD1 records: {scd1_count}")
print(f"SCD2 total records: {scd2_total}")
print(f"SCD2 current records: {scd2_current_count}")

con.close()