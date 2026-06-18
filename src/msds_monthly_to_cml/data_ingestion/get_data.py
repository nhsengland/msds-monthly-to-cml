"""Contains functions used to aquire the data from external sources"""

import zipfile
import shutil
import os
import io
from pathlib import Path
import sys
import pandas as pd
import pyodbc


def get_sql_connection(server, database):
    """Establishes and returns a connection to the SQL Server database.

    Modify the connection string variables below to match your environment.
    """

    conn_str = (
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        raise


def run_sql_query(query: str, conn) -> pd.DataFrame:
    """Connects to the database, executes a SQL query, returns the data as a

    Pandas DataFrame, and safely closes the connection.
    """
    try:
        df = pd.read_sql(query, conn)
        return df

    except Exception as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        raise

    finally:
        if conn:
            conn.close()
