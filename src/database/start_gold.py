def start_gold():
    # [database, connection_url, connection_url_alchemy, servername]
    user_info = sql_server_var()
    master_conn = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={user_info[3]};"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(master_conn, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DB_ID('{user_info[0]}')")
        if cursor.fetchone()[0] is None:
            print(
                f"Database '{user_info[0]}' does not exist. Starting from Bronze...")
            start_bronze()
            start_silver()

        con = dk.connect(f'{user_info[0]}.duckdb')
        tables = table()
        for table_names in tables.values():
            names = table_names[0]
            df_query = f"""
    SELECT * FROM silver.{names}
    """
            df = pl.read_database_uri(df_query, uri=user_info[1])
            con.register(f'temp_{names}', df)
        list_fact = create_gold(con=con)
        return [user_info[0], list_fact]

    except Exception as e:
        print(f"Error in Gold Layer: {e}")
