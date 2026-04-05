from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


def create_spark_session():
    """Create a Spark session configured with Delta Lake support."""
    builder = (
        SparkSession.builder.appName("DeltaLakeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
        .config(
            "spark.databricks.delta.properties.defaults.columnMapping.enabled", "true"
        )
        .config("spark.ui.port", "4060")
    )

    # Configure for local development
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def ingest_sales_data(spark, data_path, delta_path):
    """Ingest sales data from CSV to Delta Lake format."""
    print("🚀 Starting Delta Lake ingestion for sales data...")

    # Read CSV file
    sales_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
    )

    print(f"Loaded {sales_df.count()} sales records")

    # Write to Delta Lake
    sales_delta_path = os.path.join(delta_path, "sales")
    sales_df.write.format("delta").mode("overwrite").save(sales_delta_path)

    print(f"✅ Sales data saved to Delta Lake at: {sales_delta_path}")
    return sales_delta_path


def ingest_budget_data(spark, data_path, delta_path):
    """Ingest budget data from CSV to Delta Lake format."""
    print("🚀 Starting Delta Lake ingestion for budget data...")

    # Read CSV file
    budget_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
    )

    print(f"Loaded {budget_df.count()} budget records")

    # Write to Delta Lake
    budget_delta_path = os.path.join(delta_path, "budget")
    budget_df.write.format("delta").mode("overwrite").save(budget_delta_path)

    print(f"✅ Budget data saved to Delta Lake at: {budget_delta_path}")
    return budget_delta_path


def main():
    """Main ingestion function."""
    # Define paths
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_path, "data")
    delta_lake_path = os.path.join(base_path, "delta_lake")

    # Create Delta Lake directory if it doesn't exist
    os.makedirs(delta_lake_path, exist_ok=True)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Ingest sales data
        sales_csv = os.path.join(data_path, "100000 Sales Records.csv")
        sales_delta = ingest_sales_data(spark, sales_csv, delta_lake_path)

        # Ingest budget data
        budget_csv = os.path.join(
            data_path, "Capital_Improvement_Program__CIP__Project_Status.csv"
        )
        budget_delta = ingest_budget_data(spark, budget_csv, delta_lake_path)

        print("\n🎉 Delta Lake ingestion completed successfully!")
        print(f"Sales Delta table: {sales_delta}")
        print(f"Budget Delta table: {budget_delta}")

        # Show sample data
        print("\n📊 Sample sales data:")
        sales_df = spark.read.format("delta").load(sales_delta)
        sales_df.show(5)

        print("\n📊 Sample budget data:")
        budget_df = spark.read.format("delta").load(budget_delta)
        budget_df.show(5)

    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
