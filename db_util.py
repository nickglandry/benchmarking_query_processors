import duckdb
import os
import time

# Creating a duckdb file using tpch
DB_FILE = 'db_files/tpch_sf1.duckdb'
SCALE_FACTOR = 1


def create_db_files():
    # Create a file if it does not exist
    if not os.path.exists(DB_FILE):
        print(f"Database file {DB_FILE} not found, creating and loading TPC-H (SF={SCALE_FACTOR})...")
        con = duckdb.connect(DB_FILE)
        con.sql("INSTALL tpch; LOAD tpch;")
        con.sql(f"CALL dbgen(sf = {SCALE_FACTOR});")
        print("TPC-H data generation complete.")
        con.close()
    else:
        print(f"Found existing database: {DB_FILE}")

def run_query():
    query_to_run = """
    SELECT l_orderkey, o_orderdate, o_shippriority
    FROM customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON l_orderkey = o_orderkey
    WHERE c_mktsegment = 'AUTOMOBILE'
    LIMIT 100;
    """

    run_config = {
    'threads': 4,
    'memory_limit': '1GB'
    }

    try:
        con = duckdb.connect(DB_FILE, read_only=True, config=run_config)

        # --- This is the timing for one run ---
        start_time = time.time()
        
        # Execute the query. .fetchall() ensures the query is fully processed.
        con.sql(query_to_run).fetchall()
        
        end_time = time.time()
        # --- End of timing ---
        
        elapsed_time = end_time - start_time
        
        print(f"Query executed successfully.")
        print(f"Wall-clock time: {elapsed_time:.4f} seconds")

        # In your full script, you would save this 'elapsed_time'
        # results_list.append(elapsed_time)
        
    except duckdb.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Always close the connection
        if 'con' in locals():
            con.close()

run_query()