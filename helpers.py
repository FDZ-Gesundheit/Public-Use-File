import pyodbc
import sqlite3
import pandas as pd
import numpy as np


def connect_to_database(dsn="sqlite", username="fdz", password="fdz"):
    if dsn == "oracle":
        # get oracle connection with test database
        connect_string = f"DSN={dsn};UID={username};PWD={password}"
        try:
            cnxn = pyodbc.connect(connect_string)
            cursor = cnxn.cursor()
        except ConnectionError:
            return False
    elif dsn == "sqlite":
        connect_string = f"test_data/fdz_generated_data_dm12.db"
        try:
            cnxn = sqlite3.connect(connect_string)
            cursor = cnxn.cursor()
        except ConnectionError:
            return False
    else:
        print(f"Database {dsn} not supported. Try Sqlite or Oracle.")
        return False
    return cnxn, cursor


def get_constant_variables():
    const = ['SA151_AUSGLEICHSJAHR', 'SA152_AUSGLEICHSJAHR', 'SA153_AUSGLEICHSJAHR', 'SA451_AUSGLEICHSJAHR',
             'SA551_AUSGLEICHSJAHR', 'SA651_AUSGLEICHSJAHR', 'SA751_AUSGLEICHSJAHR', 'SA951_AUSGLEICHSJAHR',
             'SA999_AUSGLEICHSJAHR', 'SA131_AUSGLEICHSJAHR', 'SA151_BERICHTSJAHR', 'SA152_BERICHTSJAHR',
             'SA153_BERICHTSJAHR', 'SA451_BERICHTSJAHR', 'SA551_BERICHTSJAHR', 'SA651_BERICHTSJAHR',
             'SA751_BERICHTSJAHR', 'SA999_BERICHTSJAHR', 'SA131_BERICHTSJAHR', 'SA151_SATZART', 'SA152_SATZART',
             'SA153_SATZART', 'SA451_SATZART', 'SA551_SATZART', 'SA651_SATZART', 'SA751_SATZART', 'SA999_SATZART',
             'SA131_SATZART']
    return const


def get_pseudo_variables():
    pseudos = ['SA151_VSID', 'SA151_PSID', 'SA152_VSID', 'SA152_PSID', 'SA153_VSID', 'SA153_PSID', 'SA451_VSID',
               'SA451_PSID', 'SA551_VSID', 'SA551_PSID', 'SA651_VSID', 'SA651_PSID', 'SA751_VSID', 'SA751_PSID',
               'SA951_VSID', 'SA951_PSID', 'SA999_PSID', 'SA131_PSID', 'SA131_VSID']
    return pseudos


def get_data_types():
    data_types = pd.read_csv("data_types.csv")
    return dict(zip(data_types.Variable, data_types.Type))


def clean_data(column_data, dt):
    if dt == "category":
        column_data = column_data.astype('category')
    elif dt == "date":
        column_data = pd.to_datetime(column_data, format='%Y%m%d').dt.date
        column_data.replace({np.nan: None}, inplace=True)
    elif dt == "year":
        column_data = pd.to_datetime(column_data, format='%Y').dt.year
    elif dt == "integer":
        column_data = pd.to_numeric(column_data, errors='coerce').astype('Int64')
    return column_data
