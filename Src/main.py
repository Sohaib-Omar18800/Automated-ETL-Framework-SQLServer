# ******************************************
#               IMPORT LIBARIRIES
# ******************************************
import sys
import io
from auto_increment import auto_increment
from create_bronze_table import create_bronze_table
from sql_service_var import sql_server_var
from start_bronze import start_bronze
import pyodbc
import connectorx
import polars as pl
from math import floor
from sqlalchemy import create_engine
from pathlib import Path
# ******************************************
#       ENSURING DATA IS READ AS UTF-08
# ******************************************
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

user_info = start_bronze()

# ***********************************
#       SELECT QUERY TO TEST
# ***********************************

df = pl.read_database_uri(
    f"""SELECT TOP(10) * FROM bronze.crm_cust_info;""", uri=user_info[1], engine="connectorx")

print(df)
