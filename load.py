import duckdb


def load_data():
    # Connect to a persistent DuckDB file
    # If the file doesn't exist, this creates it
    con = duckdb.connect("coffeeshop.duckdb")

    print("🚀 Starting ELT Process...")

    # 1. create a schema for raw data
    con.sql("CREATE SCHEMA IF NOT EXISTS raw;")

    # 2. Load Sales Data (CSV)
    # read_csv_auto is a DuckDB magic function that infers types
    print("... Loading Sales Data")
    con.sql(
        """
        CREATE OR REPLACE TABLE raw.source_sales AS 
        SELECT * FROM read_csv_auto('data/100000 Sales Records.csv');
    """
    )

    # 3. Load Budget Data (CSV/Excel converted to CSV for simplicity)
    print("... Loading Budget Data")
    con.sql(
        """
        CREATE OR REPLACE TABLE raw.source_budget AS 
        SELECT * FROM read_csv_auto('data/Capital_Improvement_Program__CIP__Project_Status.csv');
    """
    )

    # Validation check - to see if data is loaded into database successfully
    count = con.sql("SELECT count(*) FROM raw.source_sales").fetchone()
    print(f"✅ Loaded {count[0]} sales records.")

    con.close()


if __name__ == "__main__":
    load_data()
