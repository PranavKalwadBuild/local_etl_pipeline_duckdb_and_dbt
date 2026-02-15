import duckdb

db_path = "coffeeshop.duckdb"


def main():
    con = duckdb.connect(db_path)
    try:
        while True:
            query = input("Enter SQL query (or 'exit' to quit): ")
            if query.strip().lower() == "exit":
                break
            try:
                result = con.execute(query).fetchdf()
                print(result)
            except Exception as e:
                print(f"Error: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
