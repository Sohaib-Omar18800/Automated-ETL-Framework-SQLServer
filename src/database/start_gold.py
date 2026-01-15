def start_gold():
    # [database, connection_url, connection_url_alchemy, servername]
    user_info = sql_server_var()
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
