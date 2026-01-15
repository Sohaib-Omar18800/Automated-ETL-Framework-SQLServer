from soda.scan import Scan
from datetime import datetime
import yaml


def run_soda_scan(user_info):
    config_dict = {
        "data_source my_silver_ds": {
            "type": "sqlserver",
            "host": user_info[3],
            "database": user_info[0],
            "trusted_connection": "yes",
            "driver": "ODBC Driver 17 for SQL Server",
            "schema": "silver"
        },
    }
    soda_config_str = yaml.dump(config_dict)
    scan = Scan()
    today_time = datetime.now().strftime('%Y-%m-%d')
    scan.add_variables({"today": today_time})
    scan.set_verbose(True)
    scan.set_data_source_name("my_silver_ds")
    scan.add_configuration_yaml_str(soda_config_str)
    print("What Table Would You Like To Check:".title())
    print('Please Enter The Number Corresponding To Table Name in The SQL Database From The Following\n'
          '1- crm_cust_info               2- crm_prd_info \n'
          '3- crm_sale_details            4- erp_cust_az12 \n'
          '5- erp_loc_a101                6- erp_px_cat_g1v2\n'
          '7- full_check                  8- exit\n:>> '.title())
    user_input = input(">> ").lower()
    if user_input in ['1', '2', '3', '4', '5', '6', '7', 'crm_cust_info',
                      'crm_prd_info', 'crm_sale_details', 'erp_cust_az12',
                      'erp_loc_a101', 'erp_px_cat_g1v2', 'full_check']:
        print(f"{'*'*35}")
        print("\tStarting Soda\t")
        print(f"{'*'*35}")
        if user_input in ['1', 'crm_cust_info']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/crm_cust_soda_silver_test.yml')
        if user_input in ['2', 'crm_prd_info']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/crm_prd_soda_silver_test.yml')
        if user_input in ['3', 'crm_sale_details']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/crm_sale_soda_silver_test.yml')
        if user_input in ['4', 'erp_cust_az12']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/erp_cust_az12_soda_silver_test.yml')
        if user_input in ['5', 'erp_loc_a101']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/erp_loc_a101_soda_silver_test.yml')
        if user_input in ['6', 'erp_px_cat_g1v2']:
            scan.add_sodacl_yaml_file(
                file_path='test/soda_check/erp_px_cat_g1v2_soda_silver_test.yml')
        if user_input in ['7', 'full_check']:
            scan.add_sodacl_yaml_files(
                path='test/soda_check', suffixes=['.yml', '.yaml'])
        else:
            return None

    exit_code = scan.execute()
    print(f"scan finished with exit code: {exit_code}")
    print(f"\n{'*'*25}")
    print("soda check result".title())
    print(f"\n{'*'*25}")
    print(scan.get_logs_text())
    print(f"\n{'*'*25}")
    if exit_code != 0:
        print(f"\n\t{'-'*8}|Soda Report|{'-'*8}\n")
        print(scan.get_error_logs_text())

    return exit_code
