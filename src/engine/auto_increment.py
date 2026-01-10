def auto_increment(schema, connection_url, connection_url_alchemy):

    engine_sql = create_engine(connection_url_alchemy)

    tables = table()
    i = 0

    try:
        number_files = input(
            'Please Enter How Many Files Would You Insert: ')
        files = floor(abs(int(number_files)))
    except ValueError as e:
        print(f"Error Please Enter A Number: {e}")
        return None
    while i < files:
        passed = False
        if schema == 'bronze':
            PATH = input(
                'Please Enter The File Path As Follow D:/test/file.csv:\n').strip()
            check_path = Path(PATH)
            if check_path.exists():
                passed = True
            else:
                print('Sorry, Invalid Path')
                print('Would You Like To Retry (y,n)')
                retry = input('>> ').strip().lower()
                if retry in ['y', 'yes', 'retry']:
                    pass
                else:
                    i = files
                    print('Closing Application')
                    return None
        if schema == 'silver':
            # placeholder
            pass
        if schema == 'gold':
            # placeholder
            pass

        while passed:
            table_number = input('Please Enter The Number Corresponding To Table Name in The SQL Database From The Following\n'
                                 '1- crm_cust_info               2- crm_prd_info \n'
                                 '3- crm_sale_details            4- erp_cust_az12 \n'
                                 '5- erp_loc_a101                6- erp_px_cat_g1v2\n'
                                 '7- exit\n:>> ').strip()
            if table_number.lower() in ['quit', 'exit', 'q', '7']:
                i = files
                passed = False
                return 'closing application'.title()
            if tables.get(table_number):
                i += 1
                table_info = tables.get(table_number)
                try:
                    select_table_exist = f"""
                                SELECT {table_info[1]}
                                FROM {schema}.[{table_info[0]}]
                            """
                    read_db = pl.read_database_uri(
                        query=select_table_exist, uri=connection_url, engine='connectorx')
                    # print(
                    #     f"DEBUG: Found {len(read_db)} rows in SQL Server for table {table_info[0]}")
                    if read_db.is_empty():
                        read_db = pl.DataFrame({table_info[1]: []}, schema={
                            table_info[1]: table_info[2].get(table_info[1])})
                except Exception as e:
                    print(
                        f"DEBUG: Catching error and creating empty DF: {e}")
                    read_db = pl.DataFrame({table_info[1]: []}, schema={
                        table_info[1]: table_info[2].get(table_info[1])})
                read_csv = pl.read_csv(source=PATH,
                                       separator=',', schema=table_info[2])
                new_df = read_csv.join(
                    read_db, on=table_info[1], how='anti')
                if len(new_df) > 0:
                    # print('The new rows are:')
                    # print(new_df)
                    new_df.write_database(table_name=f'{schema}.{table_info[0]}',
                                          connection=connection_url_alchemy, if_table_exists='append', engine='sqlalchemy')
                    print(
                        f"Done! {len(new_df)} New Rows Added")
                else:
                    print('Nothing New To Add')
                break
            else:
                print(
                    'Invalid Input Please Put Numbers From [1 To 7] Only')
