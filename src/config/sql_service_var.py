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
    Returns The List [database, connection_url, connection_url_alchemy, servername]
    """
    servername = input("Please Enter The Server Name: ")
    database = input("Please Enter The Desired Database Name: ")
    print("\nChoose Authentication Method:")
    print("1- Windows Authentication (No Password)")
    print("2- SQL Server Authentication (Username/Password)")
    auth_choice = input(">> ").strip()
    if auth_choice == '1':
        connection_url = f"mssql://@{servername}/{database}?trusted_connection=true"
        connection_url_alchemy = (
            f"mssql+pyodbc://{servername}/{database}?"
            "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
    else:
        username = input("Please Enter Your Username of SQL Provider: ")
        password = input("Please Enter Your Password of SQL Provider: ")
        connection_url = (
            f"mssql://{username}:{password}@{servername}/{database}"
        )
        connection_url_alchemy = f"mssql+pyodbc://{username}:{password}@{servername}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    return [database, connection_url, connection_url_alchemy, servername]
