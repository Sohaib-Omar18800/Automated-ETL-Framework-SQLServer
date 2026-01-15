from engine.auto_increment import auto_increment
from config.create_silver_table import create_silver_tables
from config.sql_service_var import sql_server_var
from config.tables import table
import pyodbc
import connectorx
import polars as pl
from database.start_bronze import start_bronze
from config.soda_config import run_soda_scan


def start_silver():
    user_info = sql_server_var()
    conn_string_windows = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={user_info[3]};"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )
    check_database = f"""SELECT DB_ID('{user_info[0]}')"""
    use_database_query = f"""USE [{user_info[0]}]"""
    create_silver_schema = """
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    BEGIN
        EXEC('CREATE SCHEMA silver')
    END
    """
    table_name = [tables[0] for tables in table().values()]

    # userinfo = [database, connection_url, connection_url_alchemy, servername]
    try:
        # Note: MAKE SURE TO USE autocommit=True FOR DDL COMMAND
        connector = pyodbc.connect(conn_string_windows, autocommit=True)
        cursor = connector.cursor()
        cursor.execute(check_database)
        db_exists = cursor.fetchone()[0]
        if db_exists is None:
            print(
                f"Database Not Found {user_info[0]}.\nStarting Bronze Layer First.")
            start_bronze()
        cursor.execute(use_database_query)
        check_bronze = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = 'bronze'
        """
        check_silver = """SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'silver'"""
        while True:
            cursor.execute(check_bronze)
            existing_bronze = [row[0] for row in cursor.fetchall()]
            if len(existing_bronze) < 6:
                print("Bronze tables are missing. Synchronizing Bronze first...".title())
                start_bronze()
            else:
                print('Bronze Layer is complete. Proceeding...'.title())
                break
        cursor.execute(create_silver_schema)
        while True:
            cursor.execute(check_silver)
            existing_silver = [row[0] for row in cursor.fetchall()]
            if len(existing_silver) < 6:
                print("Creating Silver table structures...".title())
                create_silver_tables(cursor=cursor)
            else:
                print('Silver Layer is complete. Proceeding...'.title())
                break

        auto_increment('silver', user_info[1], user_info[2], user_info[0])
        user_ans = input(
            'Would You Like To Run Inetegrity Scan (y,n)? >> ').upper()
        if user_ans in ['Y', 'YES']:
            run_soda_scan(user_info=user_info)
        else:
            pass
    except pyodbc.Error as e:
        print(f"Error occured: {e}")
    return user_info
