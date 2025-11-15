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
CPU Model: Intel Core i5-14400F (2.50 GHz)
Memory: 32 GB RAM
OS: Windows 11 Home (25H2)
Database Version: DuckDB v1.4.1 (Andium) b390a7c376

Data Generation:
TPC-H with scale factor 100
'''

# Creating a duckdb file using tpch
SCALE_FACTOR = 30
DB_FILE = os.path.join('db_files', f'tpch_sf{SCALE_FACTOR}.duckdb')

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
    # run_config = {
    #     'threads': num_threads,
    #     'memory_limit': memory_limit
    # }
    con = duckdb.connect(database=DB_FILE, read_only=True)
    if num_threads != None:
        con.execute(f"PRAGMA threads={num_threads};")
    if memory_limit != None:
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")

    for i in range(0, 13):
        # run query, get results
        start_time = time.time()
        result = con.sql(num_joins_query).fetchall()
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
    csv_file = os.path.join('results', f'results_sf{SCALE_FACTOR}.csv')
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        row_arr = [threads, memory, joins, mean, std_dev, mad] + results
        writer.writerow(row_arr)

def run_tests():
    join_1_query = """
    SELECT COUNT(*)
    FROM customer c
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    """

    join_2_query = """
    SELECT COUNT(*)
    FROM customer c
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    JOIN orders o ON c.c_custkey = o.o_custkey;
    """

    join_3_query = """
    SELECT COUNT(*)
    FROM customer c
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    """

    join_5_query = """
    SELECT COUNT(*)
    FROM customer c
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN part p ON l.l_partkey = p.p_partkey
    JOIN partsupp ps 
    ON p.p_partkey = ps.ps_partkey
    AND l.l_suppkey = ps.ps_suppkey;
    """

    join_10_query = """
    SELECT COUNT(*)
    FROM region r
    JOIN nation n1 ON r.r_regionkey = n1.n_regionkey
    JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey
    JOIN customer c ON n2.n_nationkey = c.c_nationkey
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN supplier s ON l.l_suppkey = s.s_suppkey
    JOIN nation n3 ON s.s_nationkey = n3.n_nationkey
    JOIN part p ON l.l_partkey = p.p_partkey
    JOIN partsupp ps 
    ON p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    JOIN region r2 ON n3.n_regionkey = r2.r_regionkey;
    """

    queries = [join_1_query, join_2_query, join_3_query, join_5_query, join_10_query]
    query_join_amounts = [1, 2, 3, 5, 10]
    num_threads = [16, 1, 2, 4, 8, 16]
    # num_memory = ['128MB', '256MB', '512MB', '1GB', '2GB', '3GB']
    # num_memory = ['1GB', '2GB', '4GB', '8GB', '16GB']
    num_memory = ['1GB', '2GB', '3GB', '4GB', '5GB', '6GB', '7GB', '8GB', '16GB']


    for thread in num_threads:
        print(f'Tests with {thread} threads:')
        for i in range(len(queries)):
            results = execute_test(queries[i], thread, None)
            save_result(thread, 'N/A', query_join_amounts[i], results)
            print(f'Results for {query_join_amounts[i]} join(s): {results}')
    
    # for memory in num_memory:
    #     print(f'Tests with {memory} memory:')
    #     for i in range(len(queries)):
    #         results = execute_test(queries[i], None, memory)
    #         save_result('N/A', memory, query_join_amounts[i], results)
    #         print(f'Results for {query_join_amounts[i]} join(s): {results}')

    # for thread in num_threads:
    #     print(f'Tests with {thread} threads:')
    #     for memory in num_memory:
    #         print(f'Tests with {memory} of memory')
    #         for i in range(len(queries)):
    #             results = execute_test(queries[i], thread, memory)
    #             save_result(thread, memory, query_join_amounts[i], results)
    #             print(f'Results for {query_join_amounts[i]}: {results}')

create_db_files()
run_tests()
