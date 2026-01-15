from pathlib import Path
import connectorx
import polars as pl
from math import floor
from config.schemas import schema
from config.tables import table
from sqlalchemy import create_engine
from config.etl_bronze_to_silver import etl_to_silver
from datetime import datetime
from config.create_gold_table import create_gold


def auto_increment(layer, connection_url, connection_url_alchemy, database):

    engine_sql = create_engine(connection_url_alchemy)

    tables = table()
    i = 0

    try:
        number_files = input(
            'Please Enter How Many Operations Would You Perform: ')
        files = floor(abs(int(number_files)))
    except ValueError as e:
        print(f"Error Please Enter A Number: {e}")
        return None
    while i < files:
        if layer == 'bronze':
            b_passed = False
            PATH = input(
                'Please Enter The File Path As Follow D:/test/file.csv:\n').strip()
            check_path = Path(PATH)
            if check_path.exists():
                b_passed = True
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

            while b_passed:
                table_number = input('Please Enter The Number Corresponding To Table Name in The SQL Database From The Following\n'
                                     '1- crm_cust_info               2- crm_prd_info \n'
                                     '3- crm_sale_details            4- erp_cust_az12 \n'
                                     '5- erp_loc_a101                6- erp_px_cat_g1v2\n'
                                     '7- exit\n:>> ').strip()
                if table_number.lower() in ['quit', 'exit', 'q', '7']:
                    i = files
                    b_passed = False
                    return 'closing application'.title()
                if tables.get(table_number):
                    i += 1
                    table_info = tables.get(table_number)
                    try:
                        select_table_exist = f"""
                                    SELECT {table_info[1]}
                                    FROM {layer}.[{table_info[0]}]
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
                        new_df.write_database(table_name=f'{layer}.{table_info[0]}',
                                              connection=connection_url_alchemy, if_table_exists='append', engine='sqlalchemy')
                        print(
                            f"Done! {len(new_df)} New Rows Added To Bronze")
                    else:
                        print('Nothing New To Add')
                    break
                else:
                    print(
                        'Invalid Input Please Put Numbers From [1 To 7] Only')

# ====================================================================================
        if layer == 'silver':
            s_passed = True
            while s_passed:
                table_number = input('Please Enter The Number Corresponding To Table Name in The SQL Database From The Following\n'
                                     '1- crm_cust_info               2- crm_prd_info \n'
                                     '3- crm_sale_details            4- erp_cust_az12 \n'
                                     '5- erp_loc_a101                6- erp_px_cat_g1v2\n'
                                     '7- Exit                        8- All Tables\n:>> ').strip()
                if table_number.lower() in ['quit', 'exit', 'q', '7']:
                    i = files
                    s_passed = False
                    return 'closing application'.title()
                work_list = []
                if table_number.lower() in ['8', 'all', 'all tables']:
                    work_list = [tables.get(str(n)) for n in range(1, 7)]
                    i = files
                    s_passed = False
                elif tables.get(table_number):
                    work_list = [tables.get(table_number)]
                    i += 1
                    s_passed = False
                else:
                    print('Invalid Input [1 To 8] Only')
                    continue
                for table_info in work_list:
                    try:
                        print('='*60)
                        print(f"--- Processing Table: {table_info[0]} ---")
                        print('='*60)
                        max_inserted = pl.read_database_uri(
                            f"SELECT MAX(bronze_inserted_at) as max_bi FROM {layer}.{table_info[0]}", uri=connection_url)
                        max_val = max_inserted["max_bi"][0]
                        if max_val is None:
                            max_val = datetime(1900, 1, 1, 0, 0, 0)
                        read_bronze = etl_to_silver(
                            table_name=table_info[0], connection_url=connection_url)
                        new_bronze = read_bronze.filter(
                            pl.col("bronze_inserted_at") > pl.lit(max_val))
                        if len(new_bronze) > 0:
                            new_bronze.write_database(
                                f"{layer}.{table_info[0]}", connection=connection_url_alchemy, if_table_exists='append', engine='sqlalchemy')
                            print(
                                f"Done! {len(new_bronze)} New Rows Added To Silver {table_info[0]}")
                        else:
                            print(f'Nothing New To Add For {table_info[0]}')

                    except Exception as e:
                        print(f"Error Occured: {e}")
