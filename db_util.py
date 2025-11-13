import duckdb
import os
import time
import csv
import numpy as np

'''
Plan:
DB Tool Choice: DuckDB
Experimental Dimension: Number of Joins
Reported Statistics: mean +- standard deviaition +- mean absolute deviation
Configuration Options: memory_limit, row_group_size, (backup) USING COMPRESSION in table definition

System Info:
CPU Model: Apple M2 8 Core (4 performance, 4 efficiency) CPU
Memory: 16 GB LPDDR5 RAM
OS: Mac OS Tahoe 26.1
Database Version: DuckDB v1.4.1 (Andium) b390a7c376
TODO: Check Activity Monitory before running tests to ensure relative consistency, kill all bad background tasks

Data Generation:
TPC-H with scale factor 1 and/or 10
'''

# Creating a duckdb file using tpch
DB_FILE = 'db_files/tpch_sf10.duckdb'
SCALE_FACTOR = 10

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

def execute_test(num_joins_query, num_threads, memory_limit):
    results = [] # Array of wall times for last 10 runs (13 runs, 10 returned)
    run_config = {
        'threads': num_threads,
        'memory_limit': memory_limit
    }
    con = duckdb.connect(DB_FILE, read_only=True, config=run_config)
    for i in range(0, 13):
        # run query, get results
        start_time = time.time()
        con.sql(num_joins_query).fetchall()
        end_time = time.time()
        elapsed_time = end_time - start_time
        if i > 2:
            results.append(elapsed_time)
    con.close()
    return results

def save_result(threads, memory, joins, results):
    # Calculate statistics
    results_np = np.array(results)
    mean = np.mean(results_np)
    median = np.median(results_np)
    abs_deviations = np.abs(results_np - median)
    mad = np.median(abs_deviations)
    std_dev = np.std(results_np, ddof = 1) # Using Sample Standard Deviation

    # A row in the csv follows the format: threads,memory,joins,mean,std_dev,mad,r1,r2,r3,r4,r5,r6,r7,r8,r9,r10
    csv_file = f'results/results_sf{SCALE_FACTOR}.csv'
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        row_arr = [threads, memory, joins, mean, std_dev, mad] + results
        writer.writerow(row_arr)

# def run_query():
#     query_to_run = """
#     SELECT l_orderkey, o_orderdate, o_shippriority
#     FROM customer
#     JOIN orders ON c_custkey = o_custkey
#     JOIN lineitem ON l_orderkey = o_orderkey
#     WHERE c_mktsegment = 'AUTOMOBILE'
#     LIMIT 100;
#     """

#     run_config = {
#     'threads': 4,
#     'memory_limit': '1GB'
#     }

#     try:
#         con = duckdb.connect(DB_FILE, read_only=True, config=run_config)

#         # --- This is the timing for one run ---
#         start_time = time.time()
        
#         # Execute the query. .fetchall() ensures the query is fully processed.
#         con.sql(query_to_run).fetchall()
        
#         end_time = time.time()
#         # --- End of timing ---
        
#         elapsed_time = end_time - start_time
        
#         print(f"Query executed successfully.")
#         print(f"Wall-clock time: {elapsed_time:.4f} seconds")

#         # In your full script, you would save this 'elapsed_time'
#         # results_list.append(elapsed_time)
        
#     except duckdb.Error as e:
#         print(f"An error occurred: {e}")

#     finally:
#         # Always close the connection
#         if 'con' in locals():
#             con.close()

def run_tests():
    join_1_query = """
    SELECT COUNT(*)
    FROM customer
    JOIN orders ON c_custkey = o_custkey
    WHERE c_mktsegment = 'BUILDING';
    """

    join_2_query = """
    SELECT COUNT(*)
    FROM customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey
    WHERE c_mktsegment = 'BUILDING';
    """

    join_3_query = """
    SELECT COUNT(*)
    FROM customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey
    JOIN supplier ON l_suppkey = s_suppkey
    WHERE c_mktsegment = 'BUILDING';
    """

    join_5_query = """
    SELECT COUNT(*)
    FROM customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey
    JOIN supplier ON l_suppkey = s_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
    WHERE c_mktsegment = 'BUILDING' AND r_name = 'AMERICA';
    """

    join_10_query = """
    SELECT COUNT(*)
    FROM region r
    JOIN nation n ON r.r_regionkey = n.n_regionkey
    JOIN customer c ON n.n_nationkey = c.c_nationkey
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l1 ON o.o_orderkey = l1.l_orderkey
    JOIN supplier s ON s.s_suppkey = l1.l_suppkey
    JOIN part p ON p.p_partkey = l1.l_partkey
    JOIN partsupp ps ON p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey
    JOIN lineitem l2 ON l1.l_orderkey = l2.l_orderkey AND l1.l_partkey = l2.l_partkey
    JOIN lineitem l3 ON l2.l_orderkey = l3.l_orderkey AND l2.l_suppkey = l3.l_suppkey
    JOIN lineitem l4 ON l3.l_orderkey = l4.l_orderkey
    WHERE c.c_mktsegment = 'FURNITURE' AND r.r_name = 'ASIA';
    """

    queries = [join_1_query, join_2_query, join_3_query, join_5_query, join_10_query]
    query_join_amounts = [1, 2, 3, 5, 10]
    num_threads = [1, 2, 4, 8, 16]
    num_memory = ['1GB', '2GB', '4GB', '8GB', '16GB']

    for thread in num_threads:
        print(f'Tests with {thread} threads:')
        for memory in num_memory:
            print(f'Tests with {memory} of memory')
            for i in range(len(queries)):
                results = execute_test(queries[i], thread, memory)
                save_result(thread, memory, query_join_amounts[i], results)
                print(f'Results for {query_join_amounts[i]}: {results}')

create_db_files()
run_tests()
