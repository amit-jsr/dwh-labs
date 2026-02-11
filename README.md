# Data Warehouse - Lab

Practical implementations of **SCD Type-1 & Type-2** patterns using DuckDB.

## Stack

SQL • Python • DuckDB

## Project Structure

```
dwh-labs/
├── data/
│   ├── source/source_customers.csv
│   └── cdc/cdc_1.csv, cdc_2.csv
├── database.py          # DDL and data loading
├── scd-type1.py         # SCD Type 1 (overwrite)
├── scd-type2.py         # SCD Type 2 (history tracking)
├── test_scd.py          # Test suite
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
pytest test_scd.py -v # Run tests
```

## SCD Type 1 vs Type 2

| Aspect | Type 1 | Type 2 |
|--------|--------|--------|
| History | ❌ | ✅ |
| Updates | Overwrite | Close old + insert new |
| Deletes | Hard delete | Soft close |

## Author

Amit Jaiswar