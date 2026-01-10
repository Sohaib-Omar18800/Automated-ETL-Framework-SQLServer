import polars as pl
from config.schemas import schema

# default table and schema please notice changing the names of the table will require you to also change it
# in schema script(must) and in auto_increment.py script(to prevent confusion)
def table():
    tables = {'1': ['crm_cust_info', 'cst_key', schema().get('crm_cst_schema')], '2': ['crm_prd_info', 'prd_id', schema().get('crm_prd_info_schema')],
              '3': ['crm_sale_details', 'sls_ord_num', schema().get('crm_sale_details_schema')],
              '4': ['erp_cust_az12', 'cid', schema().get('erp_cust_az12_schema')], '5': ['erp_loc_a101', 'cid', schema().get('erp_loc_a101_schema')],
              '6': ['erp_px_cat_g1v2', 'id', schema().get('erp_px_cat_g1v2_schema')]}
    return tables

