# Local ETL Pipeline with DuckDB, DBT, and Delta Lake

This project demonstrates a local ETL pipeline using DuckDB for data warehousing, DBT for transformations, and Delta Lake + PySpark for data ingestion.

## Architecture

1. **Data Ingestion**: PySpark + Delta Lake reads CSV files and stores them in Delta format
2. **Data Loading**: Data is transferred from Delta Lake to DuckDB for analysis
3. **Data Transformation**: DBT models transform the data in DuckDB
4. **Analysis**: Query the transformed data using DuckDB

## Setup

### Prerequisites

- Python 3.8+
- Java 8+ (required for PySpark)

### Installation

1. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### 1. Data Ingestion with Delta Lake

Run the PySpark ingestion script to load CSV data into Delta Lake format:

```bash
python ingest_delta.py
```

This will:
- Read CSV files from the `data/` directory
- Process them using PySpark
- Save as Delta Lake tables in `delta_lake/` directory

### 2. Load Delta Data into DuckDB

Transfer data from Delta Lake to DuckDB:

```bash
python delta_to_duckdb.py
```

This loads the Delta tables into DuckDB's `delta` schema.

### 3. Original DuckDB Loading (Alternative)

You can still use the original DuckDB loading script:

```bash
python load.py
```

### 4. DBT Transformations

Navigate to the DBT project and run transformations:

```bash
cd coffee_analytics
dbt run
```

### 4.1 Run DBT with Engine Selection

Use the wrapper script to choose between the normal dbt target and the fusion target:

```bash
python run_dbt.py --engine normal
python run_dbt.py --engine fusion
```

If you want to pass additional dbt flags, provide them after the engine option:

```bash
python run_dbt.py --engine fusion test --models my_model
```

### 5. Minimal MCP Server

A minimal MCP server is provided by `mcp_server.py`.

Start it with:

```bash
python mcp_server.py
```

Then verify the health endpoint:

```bash
curl http://localhost:8080/health
```

Send a minimal MCP request to the `/mcp` endpoint:

```bash
curl -X POST http://localhost:8080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"input": "hello"}'
```

### 6. Query Data

Use the interactive query script:

```bash
python query_duckdb.py
```

## Delta Lake Benefits

- **ACID Transactions**: Reliable data ingestion with transactional guarantees
- **Schema Enforcement**: Automatic schema validation
- **Time Travel**: Query historical versions of data
- **Scalability**: Can scale to distributed systems later
- **Performance**: Optimized for analytical workloads

## File Structure

```
├── data/                          # Raw CSV data files
├── delta_lake/                    # Delta Lake tables
│   ├── sales/                     # Sales data in Delta format
│   └── budget/                    # Budget data in Delta format
├── coffee_analytics/              # DBT project
├── load.py                        # Original DuckDB loading script
├── ingest_delta.py                # PySpark Delta Lake ingestion
├── delta_to_duckdb.py             # Transfer Delta to DuckDB
├── query_duckdb.py                # Interactive querying
├── coffeeshop.duckdb              # DuckDB database file
└── requirements.txt               # Python dependencies
```

## Configuration

### Spark Configuration

The PySpark session is configured for local development. For production use, you can modify the `create_spark_session()` function in the scripts to:

- Add cluster configuration
- Set memory and CPU limits
- Configure S3/ADLS access for cloud storage

### Delta Lake Configuration

Delta Lake tables are stored locally in the `delta_lake/` directory. You can change this path in the scripts or configure it for cloud storage (S3, ADLS, GCS).

## Troubleshooting

### Java Not Found
If you get Java-related errors, ensure Java 8+ is installed:
```bash
java -version
```

### Memory Issues
For large datasets, increase Spark memory:
```python
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

### Port Conflicts
If port 4040 is in use (Spark UI), change it:
```python
.config("spark.ui.port", "4041")
```