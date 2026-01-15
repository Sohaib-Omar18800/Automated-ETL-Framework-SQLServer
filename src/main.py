# ******************************************
#               IMPORT LIBARIRIES
# ******************************************
import sys
import io
from engine.auto_increment import auto_increment
from config.create_bronze_table import create_bronze_table
from config.sql_service_var import sql_server_var
from config.create_gold_table import create_gold
from database.start_gold import start_gold
from database.start_bronze import start_bronze
from database.start_silver import start_silver
import pyodbc
import connectorx
import polars as pl
from math import floor
from sqlalchemy import create_engine
from pathlib import Path
from config.schemas import schema
from config.tables import table
from config.soda_config import run_soda_scan
from soda.scan import Scan
from datetime import datetime
import yaml
import duckdb as dk
# ******************************************
#      ENSURING DATA IS READ AS UTF-08
# ******************************************
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def main_app():
    while True:
        print('What Layer Would You Like To Auto Increment')
        user_answer = input(
            'Layers are [bronze,silver,gold]: ').strip().lower()
        if user_answer in ['bronze', 'b', 'bronze layer']:
            print("Starting Bronze Auto Increment Load")
            start_bronze()
        elif user_answer in ['silver', 's', 'silver layer']:
            print("Starting Silver Auto Increment Load")
            start_silver()
        elif user_answer in ['gold', 'g', 'gold layer']:
            print("Starting Golden Load")
            gathered = start_gold()
            database = gathered[0]
            tables = gathered[1]
            con = dk.connect(f"{database}.duckdb")
            for table_name in tables:
                print(f"{'='*150}")
                print("\t"*7, table_name)
                print(f"{'='*150}")
                query = f""" SELECT * FROM {table_name} LIMIT 5 """
                print(con.execute(query=query).df())
            con.close()

        else:
            print(
                'Invalid Input Please Pick From This List [bronze,silver,gold]')
            print('Would You Like To Retry (y,n)')
            user_retry = input('>> ').strip().lower()
            if user_retry in ['y', 'yes', 'retry']:
                continue
            else:
                print('Closing Application')
                print('Thanks, For Using The App')
                return None
        print('Finished Auto Increment')
        print('Would You Like To Close The App (y,n)')
        user_close = input('>> ').strip().lower()
        if user_close in ['n', 'no', 'exit', 'quit', 'q']:
            pass
        else:
            print('Thanks For Using The Application')
            print('Closing Application')
            break


main_app()
