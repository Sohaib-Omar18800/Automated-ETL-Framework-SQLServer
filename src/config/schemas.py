import polars as pl


def schema():
    schemas = {'crm_cst_schema': {'cst_id': pl.Int32,
                                  'cst_key': pl.String,
                                  'cst_firstname': pl.String,
                                  'cst_lastname': pl.String,
                                  'cst_material_status': pl.String,
                                  'cst_gndr': pl.String,
                                  'cst_create_date': pl.Date},
               'crm_prd_info_schema': {'prd_id': pl.Int32,
                                       'prd_key': pl.String,
                                       'prd_nm': pl.String,
                                       'prd_cost': pl.Float32,
                                       'prd_line': pl.String,
                                       'prd_start_dt': pl.Date,
                                       'prd_end_dt': pl.Date},
               'crm_sale_details_schema': {'sls_ord_num': pl.String,
                                           'sls_prd_key': pl.String,
                                           'sls_cust_id': pl.Int32,
                                           'sls_order_dt': pl.Int32,
                                           'sls_ship_dt': pl.Int32,
                                           'sls_due_dt': pl.Int32,
                                           'sls_sales': pl.Float32,
                                           'sls_quantity': pl.Int32,
                                           'sls_price': pl.Float32},
               'erp_cust_az12_schema': {'cid': pl.String,
                                        'bdate': pl.Date,
                                        'gen': pl.String},
               'erp_loc_a101_schema': {'cid': pl.String,
                                       'cntry': pl.String},
               'erp_px_cat_g1v2_schema': {'id': pl.String,
                                          'cat': pl.String,
                                          'subcat': pl.String,
                                          'maintenance': pl.String}
               }
    return schemas
