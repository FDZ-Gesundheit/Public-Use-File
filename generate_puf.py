import random
import sys
import argparse
from pathlib import Path

import csv

import pandas as pd
from helpers import (connect_to_database, get_data_types, get_pseudo_variables, get_constant_variables, clean_data,
                     get_pseudo_mapping, get_secondary_pools_dm3)
from functions import force_k, generate_pseudonym, shuffle_column
import warnings
from datetime import datetime
from multiprocessing import Pool, cpu_count

warnings.simplefilter(action='ignore', category=UserWarning)
K = 3



def get_prefix(table: str, data_model: int) -> str:

    """
    Returns the correct table prefix for the specified "Satzart" as available in the database
    Parameters:
        table (str): The name of the current table (Satzart)
        data_model (int): The data model of the current table
    Returns:
        prefix (str): Prefix of the table
    """
    return "BJ" if data_model == 3 else "V" if table == 'SA131' else "VBJ"


def generate_pool_of_ids(col_name: str, table: str, data_model: int) -> list:
    """
    Generates a pool of new ids

    Parameters:
        col_name (str): The name of the column
        table (str): The name of the current table (Satzart)
        data_model: The data model of the current table

    Returns:
        id_pool (list): A list of new randomly generated pseudonyms
    """
    prefix = get_prefix(table, data_model=data_model)
    table_name = f"{prefix}{args.year}{table}"
    cur.execute(f"SELECT {col_name} from {table_name}")
    data = pd.Series([entry[0] for entry in cur.fetchall()])
    id_pool = [generate_pseudonym(len(str(val))) for val in set(data)]

    return id_pool


def get_columns(table_name: str, args: argparse.Namespace) -> list:
    """
    Fetches the names of all columns for the specific table (Satzart).

    Parameters:
        table_name (str): The name of the current table
        args (argparse.Namespace): Dictionary containing command line arguments

    Returns:
        columns (list): A list of column names from the specified table
    """

    connection, cursor = connect_to_database(data_model=args.dm)
    if args.dsn == "sqlite":
        info_query = f"PRAGMA table_info({table_name})"
        df_info = pd.read_sql_query(info_query, con=connection)
        columns = list(df_info.name)
    elif args.dsn == "oracle":
        info_query = f"SELECT * FROM {table_name} FETCH FIRST 2 ROWS ONLY"
        cursor.execute(info_query)
        columns = [col[0] for col in cursor.description if col[0] != 'index']
    else:
        sys.exit("SQL dialect not supported. Choose from sqlite or oracle")
    connection.close()

    return columns


def process_data(arguments):
    """
    Fetches original data from database, processes it by applying random shuffling and k-anonymity
    and writes it column-wise into csv files.

    Parameters:
        arguments (tuple): Contains the table name to process (str) and the command line arguments
        (argparse.Namespace), including year and dsn connection
    """

    table, mapping, args = arguments
    dtypes = get_data_types(data_model=args.dm)
    prefix = get_prefix(table, args.dm)

    table_name = f"{prefix}{args.year}{table}"
    columns = get_columns(table_name, args)

    # create output directory, if it doesn't exist
    out_dir_path_name: str = "output_csv"
    out_dir: Path = Path(out_dir_path_name)
    out_dir.mkdir(exist_ok=True)

    # get single column and process it
    # this is needed due to memory issues
    # whole tables cannot be loaded and stored in a pandas dataframe
    for e, col in enumerate(columns):
        connection, cursor = connect_to_database(data_model=args.dm)
        if col in get_constant_variables(data_model=args.dm):
            cursor.execute(f"SELECT {col} from {table_name} LIMIT 1")
            data = pd.Series([entry[0] for entry in cursor.fetchall()])
            cursor.execute(f"SELECT COUNT(*) from {table_name}")
            n = cursor.fetchall()[0][0]
            data = pd.Series([data[0]] * n)
            connection.close()

        elif col in get_pseudo_variables(data_model=args.dm):
            cursor.execute(f"SELECT COUNT(*) from {table_name}")
            n = cursor.fetchall()[0][0]
            key = col[col.find("_") + 1:]
            if table in ['SA151', 'SA152', 'SA751', 'SA131', 'VERS']:
                data = pd.Series(mapping[key]
                                 + [random.choice(mapping[key]) for _ in
                                    range(n - len(mapping[key]))])
            else:
                data = pd.Series([random.choice(mapping[key]) for _ in range(n)])

        else:  # get data
            begin = datetime.now()
            cursor.execute(f"SELECT {col} from {table_name}")
            data = pd.Series([entry[0] for entry in cursor.fetchall()])
            connection.close()
            print(f"Fetching {col} data from {table_name} took {datetime.now() - begin}")

            # 0) clean column
            begin = datetime.now()
            data_type = dtypes[col]
            data = clean_data(data, data_type)
            print(f"Cleaning {col} took {datetime.now() - begin}.")

            # 1) randomly shuffle the column
            begin = datetime.now()
            data = shuffle_column(data)  # overwrite data in columns new order
            print(f"Data shuffling for {col} took {datetime.now() - begin}.")

            # 2) apply k-anonymity
            begin = datetime.now()
            data = force_k(data, data_type, k=K)
            print(f"K-anonymity for {col} took {datetime.now() - begin}.")

        # 3) write data into csv files
        
        csv_file_out: Path = out_dir / f"{table}_{e}.csv"
        csv_file_in: Path = out_dir / f"{table}_{e-1}.csv"
        csv_final: Path = out_dir / f"{table}.csv"

        if e == 0:
            with csv_file_out.open("w", newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow([col])
                for data_row in data:
                    writer.writerow([data_row])
        else:
            with csv_file_in.open("r") as f_in, csv_file_out.open("w", newline="") as f_out:
                csv_reader: csv.reader = csv.reader(f_in, delimiter=",")
                writer: csv.writer = csv.writer(f_out, delimiter=",")
                writer.writerow(next(csv_reader) + [col])
                for csv_row, data_row in zip(csv_reader, data):
                    csv_row.append(data_row)
                    writer.writerow(csv_row)
            csv_file_in.unlink(missing_ok=True)
    
    # rename fails on windows systems if the target file exists
    if csv_final.exists():
        csv_final.unlink()
    
    csv_file_out.rename(csv_final)
    
    return None


def write_to_database(table: str):
    """
    Loads data from CSV files and writes them to the SQLite database.

    Parameters:
        table (str): The name of the table to process (str).
    """
    table_name = f"{get_prefix(table, args.dm)}{args.year}{table}_puf"
    cnxn, cur = connect_to_database(data_model=args.dm)
    with (open(f"output_csv/{table}.csv", "r") as f):
        reader = csv.reader(f, delimiter=",")
        for i, row in enumerate(reader):
            if i == 0:
                cols = row
            else:
                insert_query = f"INSERT INTO {table_name} {tuple(cols)} values {tuple(['?' for i in range(len(cols))])}"
                insert_query = insert_query.replace("'", "")
                cur.execute(insert_query, row)
    cnxn.commit()
    cnxn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Script to generate the public use file for DM1/2")
    parser.add_argument("--dsn", default="sqlite", help="Data source name for ODBC data source, default: sqlite")
    parser.add_argument("--username", default="fdz", help="Username to connect to database, default: fdz")
    parser.add_argument("--password", default="fdz", help="Password to connect to database, default: fdz")
    parser.add_argument("--year", default=2016, type=int, help="Year for data creation, default: 2016")
    parser.add_argument("--multi_threading", default=False, help="Whether to parallelize the code in multiple "
                                                                 "threads, default: False")

    args = parser.parse_args()
    # get data model
    args.dm = 2 if args.year <= 2018 else 3

    begin = datetime.now()
    # connect to database:
    cnxn, cur = connect_to_database(data_model=args.dm)

    # get pool for all person ids:

    if args.dm == 2:
        psid_pool = generate_pool_of_ids("SA151_PSID", "SA151", args.dm)
        vsid_pool = generate_pool_of_ids("SA151_VSID", "SA151", args.dm)
        id_pool_mapping = {"PSID": psid_pool, "VSID": vsid_pool}
    else:
        pseudo_mapping = get_pseudo_mapping(data_model=args.dm)
        id_pool_mapping = {key: [] for key in pseudo_mapping.keys()}
        for col in id_pool_mapping.keys():
            if col not in get_secondary_pools_dm3().keys():
                id_pool_mapping[col] = generate_pool_of_ids(col, pseudo_mapping[col], args.dm)
        for key, value in get_secondary_pools_dm3().items():
            id_pool_mapping[key] = id_pool_mapping[value]


    # drop all tables and create new ones - needed for testing purposes
    if args.dm == 2:
        all_tables = ["SA151", "SA131", "SA152", "SA153", "SA551", "SA651", "SA751", "SA951", "SA451"]
        create_path = "create_puf_tables.sql"
    else:
        all_tables = ["VERS", "VERSQ", "VERSQDMP", "REZ", "AMBFALL", "KHFALL", "ZAHNFALL",
                      "AMBDIAG", "AMBOPS", "AMBLEIST", "KHFA", "KHENTG", "KHDIAG", "KHPROZ",
                      "ZAHNLEIST", "ZAHNBEF", "EZD"]
        create_path = "create_puf_tables_dm3.sql"
    if args.dm == 3:
        drop_all = " ".join([f"DROP TABLE IF EXISTS BJ{args.year}{table}_puf;" for table in all_tables])
    else:
        drop_all = " ".join(
            [f"DROP TABLE IF EXISTS {get_prefix(table, args.dm)}{args.year}{table}_puf;" for table in all_tables])
    cur.executescript(drop_all)

    # generate new tables
    with open(create_path, "r") as f_tables:
        sql_create_tables = f_tables.read().format(prefix="BJ" if args.dm == 3 else "VBJ", receiving_year=args.year,
                                                   clearing_year=int(args.year) - 1,
                                                   schema="puf")
    cur.executescript(sql_create_tables)
    cnxn.close()

    if args.multi_threading:
        num_processes = min(len(all_tables), cpu_count())
        print(f"Multi-threading is used with {num_processes} processes.")
        with Pool(num_processes) as pool:
            pool.map(process_data, [(table, id_pool_mapping, args) for table in all_tables])
    else:
        for table in all_tables:
            process_data((table, id_pool_mapping, args))


    # load csv files and insert data into database line by line
    # this is needed when working with the real data because the tables cannot be loaded into the memory at once
    for table in all_tables:
        print(f"Writing table {table} to database.")
        write_to_database(table)

    print(f"The whole process took {datetime.now() - begin}.")
