def sql_server_var():
    """
*********************************************
    USES THE USER INPUT database,username,password,servername
    TO ESTABLESH CONNECTION TO THE SQL SERVER
*********************************************
-----------------------------------------------------
    Note THE connection_url FORMAT AS FOLLOWING:
    connection_url = sqlprovider://username:password@servername/database
    connection_url_alchemy = f"mssql+pyodbc://{username}:{password}@{servername}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
----------------------------------------------------
    """
    servername = input("Please Enter The Server Name: ")
    database = input("Please Enter The Desired Database Name: ")
    username = input("Please Enter Your Username of SQL Provider: ")
    password = input("Please Enter Your Password of SQL Provider: ")
    connection_url = (
        f"mssql://{username}:{password}@{servername}/{database}"
    )
    connection_url_alchemy = f"mssql+pyodbc://{username}:{password}@{servername}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    return [database, connection_url, connection_url_alchemy, servername]
