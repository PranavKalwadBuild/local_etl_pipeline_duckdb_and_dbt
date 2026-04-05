import duckdb
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


def create_spark_session():
    """Create a Spark session configured with Delta Lake support."""
    builder = (
        SparkSession.builder.appName("DeltaToDuckDB")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.port", "4060")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def load_delta_to_duckdb(spark, delta_path, duckdb_path):
    """Load data from Delta Lake tables into DuckDB."""
    con = duckdb.connect(duckdb_path)

    print("🚀 Loading Delta Lake data into DuckDB...")

    try:
        # Create schema for delta data
        con.sql("CREATE SCHEMA IF NOT EXISTS delta;")

        # Load sales data from Delta
        sales_delta_path = os.path.join(delta_path, "sales")
        if os.path.exists(sales_delta_path):
            print("... Loading sales data from Delta Lake")
            sales_df = spark.read.format("delta").load(sales_delta_path)
            sales_pandas = sales_df.toPandas()

            # Create table in DuckDB
            con.sql("CREATE OR REPLACE TABLE delta.sales AS SELECT * FROM sales_pandas")
            print(f"✅ Loaded {len(sales_pandas)} sales records into DuckDB")

        # Load budget data from Delta
        budget_delta_path = os.path.join(delta_path, "budget")
        if os.path.exists(budget_delta_path):
            print("... Loading budget data from Delta Lake")
            budget_df = spark.read.format("delta").load(budget_delta_path)
            budget_pandas = budget_df.toPandas()

            # Create table in DuckDB
            con.sql(
                "CREATE OR REPLACE TABLE delta.budget AS SELECT * FROM budget_pandas"
            )
            print(f"✅ Loaded {len(budget_pandas)} budget records into DuckDB")

        # Validation
        sales_count = con.sql("SELECT COUNT(*) FROM delta.sales").fetchone()[0]
        budget_count = con.sql("SELECT COUNT(*) FROM delta.budget").fetchone()[0]

        print(f"\n📊 Total records in DuckDB:")
        print(f"   Sales: {sales_count}")
        print(f"   Budget: {budget_count}")

    except Exception as e:
        print(f"❌ Error loading data to DuckDB: {e}")
        raise
    finally:
        con.close()


def main():
    """Main function to load Delta data into DuckDB."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    delta_path = os.path.join(base_path, "delta_lake")
    duckdb_path = os.path.join(base_path, "coffeeshop.duckdb")

    # Check if Delta Lake data exists
    if not os.path.exists(delta_path):
        print(f"❌ Delta Lake path not found: {delta_path}")
        print("Please run ingest_delta.py first to create Delta Lake tables.")
        return

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data from Delta to DuckDB
        load_delta_to_duckdb(spark, delta_path, duckdb_path)

        print("\n🎉 Data successfully loaded from Delta Lake to DuckDB!")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
