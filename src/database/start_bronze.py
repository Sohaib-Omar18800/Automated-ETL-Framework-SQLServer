
def start_bronze():
    user_info = sql_server_var()
    conn_string_windows = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={user_info[3]};"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )
    create_datawarehouse_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{user_info[0]}')
    BEGIN
        CREATE DATABASE [{user_info[0]}]
    END
    ELSE
    BEGIN
        SELECT 1
    END
    """
    use_database_query = f"""USE [{user_info[0]}]"""
    create_bronze_schema = """
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    BEGIN
        EXEC('CREATE SCHEMA bronze')
    END
    """
    # userinfo = [database, connection_url, connection_url_alchemy, servername]
    try:
        # Note: MAKE SURE TO USE autocommit=True FOR DDL COMMAND
        connector = pyodbc.connect(conn_string_windows, autocommit=True)
        cursor = connector.cursor()
        cursor.execute(create_datawarehouse_query)
        res = cursor.fetchone()
        if res and res[0] == 1:
            print('The Database Does Exist')
        else:
            print('The Database Was Successfully Created')
        cursor.execute(use_database_query)
        cursor.execute(create_bronze_schema)
        create_bronze_table(cursor=cursor)
        auto_increment('bronze', user_info[1], user_info[2], user_info[0])
    except pyodbc.Error as e:
        print(f"Error occured: {e}")
    return user_info
