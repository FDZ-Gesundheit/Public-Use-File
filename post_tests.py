import unittest
import sys
import pandas as pd
from helpers import connect_to_database


def get_prefix(table: str) -> str:
    """
    Returns the correct table prefix for the specified "Satzart" as available in the database
    Parameters:
        table (str): The name of the current table (Satzart)
    Returns:
        prefix (str): Prefix of the table
    """
    return "V" if table == 'SA131' else "VBJ"


class TestOutputData(unittest.TestCase):
    YEAR = 2016
    DATA_MODEL = 2 if YEAR <= 2018 else 3
    ALL_TABLES = ["SA151", "SA152", "SA153", "SA451", "SA551", "SA651", "SA751", "SA951", "SA131"] if (
                 DATA_MODEL == 2) else ["VERS", "VERSQ", "VERSQDMP", "REZ", "AMBFALL", "KHFALL", "ZAHNFALL",
                      "AMBDIAG", "AMBOPS", "AMBLEIST", "KHFA", "KHENTG", "KHDIAG", "KHPROZ",
                      "ZAHNLEIST", "ZAHNBEF", "EZD", "VS"]

    def test_same_datatypes(self):
        print("\nTest whether the Public Use File contains the same datatypes as the original data.")
        con, cursor = connect_to_database(data_model=self.DATA_MODEL)
        for table in self.ALL_TABLES:
            print(f"\nTable {table}")
            prefix = get_prefix(table)
            table_name_puf = f"{prefix}{self.YEAR}{table}_puf"
            table_name_original = f"{prefix}{self.YEAR}{table}"
            df_original = pd.read_sql_query(f"PRAGMA table_info({table_name_original})", con=con)
            d_original_type = {key: value for key, value in zip(df_original.name, df_original.type)}
            df_puf = pd.read_sql_query(f"PRAGMA table_info({table_name_puf})", con=con)
            d_puf_type = {key: value for key, value in zip(df_puf.name, df_puf.type)}
            print(f"Original data types:\t {d_original_type}")
            print(f"Puf data types:\t\t\t {d_puf_type}")
            self.assertEqual(d_original_type, d_puf_type)
        con.close()

    def test_completeness(self):
        print("Test whether the Public Use File is complete, hence contains the same columns and number of rows for "
              "each table as the original data.")
        con, cursor = connect_to_database(data_model=self.DATA_MODEL)
        for table in self.ALL_TABLES:
            print(f"Table {table}")
            prefix = get_prefix(table)
            table_name_puf = f"{prefix}{self.YEAR}{table}_puf"
            table_name_original = f"{prefix}{self.YEAR}{table}"

            # check columns
            info_query_original = f"PRAGMA table_info({table_name_original})"
            df_info_original = pd.read_sql_query(info_query_original, con=con)
            columns_original = set(df_info_original.name)
            info_query_puf = f"PRAGMA table_info({table_name_puf})"
            df_info_puf = pd.read_sql_query(info_query_puf, con=con)
            columns_puf = set(df_info_puf.name)
            print("Original columns:\t", columns_original)
            print("PUF columns:\t\t", columns_puf, "\n")

            # check amount of rows
            df_original = pd.read_sql_query(f"SELECT count(*) as count FROM {table_name_original}", con=con)
            df_puf = pd.read_sql_query(f"SELECT count(*) as count FROM {table_name_puf}", con=con)
            n_rows_original = df_original["count"].values.tolist()[0]
            n_rows_puf = df_puf["count"].values.tolist()[0]
            print("Amount of original rows:\t", n_rows_original)
            print("Amount of PUF rows:\t", n_rows_puf, "\n")

            self.assertEqual(columns_original, columns_puf)
            self.assertEqual(n_rows_original, n_rows_puf)
        con.close()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestOutputData.YEAR = sys.argv.pop()
    unittest.main()
    # unittest.main(argv=['', '-v'])
