from pathlib import Path
import connectorx
import polars as pl
from math import floor
from sqlalchemy import create_engine


def auto_increment(database, connection_url, connection_url_alchemy):

    engine_sql = create_engine(connection_url_alchemy)

    crm_cst_schema = {'cst_id': pl.Int32, 'cst_key': pl.String, 'cst_firstname': pl.String, 'cst_lastname': pl.String,
                      'cst_material_status': pl.String, 'cst_gndr': pl.String, 'cst_create_date': pl.Date}
    crm_prd_info_schema = {'prd_id': pl.Int32, 'prd_key': pl.String, 'prd_nm': pl.String, 'prd_cost': pl.Float32,
                           'prd_line': pl.String, 'prd_start_dt': pl.Date, 'prd_end_dt': pl.Date}
    crm_sale_details_schema = {'sls_ord_num': pl.String, 'sls_prd_key': pl.String, 'sls_cust_id': pl.Int32, 'sls_order_dt': pl.Int32,
                               'sls_ship_dt': pl.Int32, 'sls_due_dt': pl.Int32, 'sls_sales': pl.Float32, 'sls_quantity': pl.Int32, 'sls_price': pl.Float32}
    erp_cust_az12_schema = {'cid': pl.String,
                            'bdate': pl.Date, 'gen': pl.String}
    erp_loc_a101_schema = {'cid': pl.String, 'cntry': pl.String}
    erp_px_cat_g1v2_schema = {'id': pl.String, 'cat': pl.String,
                              'subcat': pl.String, 'maintenance': pl.String}
    ACTIVE = True
    tables = {'1': ['crm_cust_info', 'cst_key', crm_cst_schema], '2': ['crm_prd_info', 'prd_id', crm_prd_info_schema],
              '3': ['crm_sale_details', 'sls_ord_num', crm_sale_details_schema],
              '4': ['erp_cust_az12', 'cid', erp_cust_az12_schema], '5': ['erp_loc_a101', 'cid', erp_loc_a101_schema],
              '6': ['erp_px_cat_g1v2', 'id', erp_px_cat_g1v2_schema]}
    i = 0
    while ACTIVE:
        schema = input(
            'Please Enter Schmea Name (bronze,silver,gold): ').lower().strip()
        if schema in ['q', 'quit', 'exit']:
            print('closing the application'.title())
            return None
        elif schema in ['bronze', 'silver', 'gold']:
            try:
                number_files = input(
                    'Please Enter How Many Files Would You Insert: ')
                files = floor(abs(int(number_files)))
            except ValueError as e:
                print(f"Error Please Enter A Number: {e}")
                return e
            while i < files:
                PATH = input(
                    'Please Enter The File Path As Follow D:/test/file.ext (where ext can be csv,xml,xslx,...):\n').strip()
                check_path = Path(PATH)
                if check_path.exists():
                    while True:
                        table_number = input('Please Enter The Number Corresponding To Table Name in The SQL Database From The Following\n'
                                             '1- crm_cust_info               2- crm_prd_info \n'
                                             '3- crm_sale_details            4- erp_cust_az12 \n'
                                             '5- erp_loc_a101                6- erp_px_cat_g1v2\n'
                                             '7- exit\n:>> ').strip()
                        if table_number.lower() in ['quit', 'exit', 'q', '7']:
                            i = files
                            ACTIVE = False
                            break
                        elif tables.get(table_number):
                            i += 1
                            table_info = tables.get(table_number)
                            try:
                                select_table_exist = f"""
                                        SELECT {table_info[1]}
                                        FROM {schema}.[{table_info[0]}]
                                    """
                                read_db = pl.read_database_uri(
                                    query=select_table_exist, uri=connection_url, engine='connectorx')
                                print(
                                    f"DEBUG: Found {len(read_db)} rows in SQL Server for table {table_info[0]}")
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
                                print('The new rows are:')
                                print(new_df)
                                new_df.write_database(table_name=f'{schema}.{table_info[0]}',
                                                      connection=connection_url_alchemy, if_table_exists='append', engine='sqlalchemy')
                                print(
                                    f"Done! {len(new_df)} New Rows Added")
                                break
                            else:
                                print('Nothing New To Add')
                                break
                        else:
                            print(
                                'Invalid Input Please Put Numbers From [1 To 7] Only')
                else:
                    print("The Provided Path Doesn't Exist")
                    answer = input("Would You Like To Retry (yes,no)?")
                    if answer.upper() in ['NO', 'N', 'Q', 'Quit', 'EXIT']:
                        print('Thanks For Using The Service')
                        i = files
                        ACTIVE = False
                        return None
                    if answer.upper() in ['YES', 'Y']:
                        pass
                    else:
                        print('Please Use Valid Answer (yes,no)')
        else:
            print(
                'invalid schema name as (bronze,silver,gold) are the only valid schema names'.title())
        close_app = input(
            'Auto_Incremental Just Finished.\nClose The App (yes,no): >> ').lower().strip()
        if close_app in ['n', 'no']:
            print('The Application Will Run Again')
            i = 0
        else:
            print('The Application Is Closing')
            ACTIVE = False
            print('Thanks For Using The Service')
            return None
