import polars as pl
from config.sql_service_var import sql_server_var

crm_cst_info_q = """
SELECT TOP(2) * FROM bronze.crm_cust_info
"""
crm_prd_info_q = """
SELECT TOP(2) * FROM bronze.crm_prd_info
"""
crm_sale_details_q = """
SELECT TOP(2) * FROM bronze.crm_sale_details
"""
erp_cust_az12_q = """
SELECT TOP(2) * FROM bronze.erp_cust_az12
"""
erp_loc_a101_q = """
SELECT TOP(2) * FROM bronze.erp_loc_a101
"""
erp_px_cat_g1v2_q = """
SELECT TOP(2) * FROM bronze.erp_px_cat_g1v2
"""

user_info = sql_server_var()

crm_cst_df = pl.read_database_uri(crm_cst_info_q, uri=user_info[1])
crm_prd_df = pl.read_database_uri(crm_prd_info_q, uri=user_info[1])
crm_sales_df = pl.read_database_uri(crm_sale_details_q, uri=user_info[1])
erp_cust_df = pl.read_database_uri(erp_cust_az12_q, uri=user_info[1])
erp_loc_df = pl.read_database_uri(erp_loc_a101_q, uri=user_info[1])
erp_cat_df = pl.read_database_uri(erp_px_cat_g1v2_q, uri=user_info[1])
dictinary = {"crm_cst_df": crm_cst_df, "crm_prd_df": crm_prd_df, "crm_sales_df": crm_sales_df,
             "erp_cust_df": erp_cust_df, "erp_loc_df": erp_loc_df, "erp_cat_df": erp_cat_df}
for k, v in dictinary.items():
    print(f"Table Name: {k}")
    print(v)


# ┌───────────────────────────────────────────┐
# │                                           │
# │ erp_cust_az12---     -------erp_loc_a101  │
# │                 |   |                     │
# │                 ↓   ↓                     │
# │        crm_cust_info ----------           │
# │                                ↓          │
# │       crm_prd_info ----->crm_sale_details │
# │                   ↑                       │
# │                   |                       │
# │  erp_px_cat_g1v2 -                        │
# │                                           │
# └───────────────────────────────────────────┘
# ┌────────────────┬────────────┬────────────────┐
# │ name           ┆ join_key   ┆shape           │
# │ ---            ┆ ---        ┆ ---            │
# ╞════════════════╪════════════╪════════════════╡
# │erp_px_cat_g1v2 ┆ id         ┆ AC_BR          │
# │ erp_cust_az12  ┆ cid        ┆NASAW00011000   │
# │ erp_loc_a101   ┆ cid        ┆ AW-00011000    |
# │ crm_cst_info   ┆ cst_key    ┆ AW00011000     |
# │ crm_prd_info   ┆ prd_key    ┆CO-RF-FR-R92B-58|
# │crm_sale_details┆sls_prd_id  ┆ TI-M267        |
# │                ┆sls_cust_id ┆ 14312          |
# └────────────────┴────────────┴────────────────┘
