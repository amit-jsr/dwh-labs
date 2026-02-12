# Data Warehouse Labs

Practical implementations of **SCD Type-1 & Type-2** patterns using DuckDB.

## Stack

SQL • Python • DuckDB

## Project Structure

```
dwh-labs/
├── data/
│   ├── source/customers.csv
│   └── cdc/customers_cdc1.csv, customers_cdc2.csv
├── database.py          # DDL and data loading
├── scd-type1.py         # SCD Type 1 (overwrite)
├── scd-type2.py         # SCD Type 2 (history tracking)
├── test/test_scd.py     # Test suite
└── requirements.txt
```

## Setup

```bash
python3 -m venv env && source env/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python scd-type1.py   # Overwrite updates, hard deletes
python scd-type2.py   # History tracking with effective dates
```

## Note
- **SCD1:** Overwrites data, no history preserved
- **SCD2:** Preserves history by closing old records and inserting new versions

*Originally developed in Jan 2022*

## Related Articles
- [Data Warehouse Architecture Overview](https://www.techrxiv.org/users/791577/articles/1072655-data-warehouse-architecture-overview)
- [Implementing SCD2 in Snowflake](https://medium.com/@amit-jsr/implementing-scd2-in-snowflake-slowly-changing-dimension-type-2-7ff793647150)
- [Implementing SCD1 in Snowflake](https://medium.com/@amit-jsr/implementing-slowly-changing-dimension-type-1-scd1-in-snowflake-d2206ec3cc01)