import polars as pl
from datetime import datetime


def etl_to_silver(table_name, connection_url):
    if table_name == "crm_cust_info":
        crm_cust_info_query = """
        SELECT * FROM bronze.crm_cust_info
        """
        df = pl.read_database_uri(
            query=crm_cust_info_query, uri=connection_url)
        fixed_cst_table = df.drop_nulls(subset=pl.col(['cst_id'])).\
            sort(by=pl.col('cst_create_date'), descending=True).\
            unique(pl.col('cst_key'), keep='first').\
            select([pl.col('cst_id'), pl.col('cst_key'),
                    pl.col('cst_firstname').str.strip_chars(), pl.col(
                'cst_lastname').str.strip_chars(),
                pl.when(pl.col('cst_material_status').str.to_uppercase().str.strip_chars() == 'M').then(pl.lit('Married')).
                when(pl.col('cst_material_status').str.to_uppercase().str.strip_chars() == 'S').then(pl.lit('Single')).
                otherwise(pl.lit('n/a')).alias('cst_material_status'),
                pl.when(pl.col('cst_gndr').str.to_uppercase().str.strip_chars() == 'M').then(pl.lit('Male')).
                when(pl.col('cst_gndr').str.to_uppercase().str.strip_chars() == 'F').then(pl.lit('Female')).
                otherwise(pl.lit('n/a')).alias('cst_gndr'),
                pl.col('cst_create_date').alias('cst_create_date'),
                pl.col('inserted_at').alias('bronze_inserted_at')])
        return fixed_cst_table
# ===================================================================================================================================
    if table_name == "crm_prd_info":
        crm_product_query = """
        SELECT * FROM bronze.crm_prd_info
        """
        df = pl.read_database_uri(query=crm_product_query, uri=connection_url)
        fixed_crm_prd_info = df.sort(["prd_key", "prd_start_dt"]).\
            with_columns([pl.col("prd_start_dt").shift(-1).over("prd_key").alias("prd_end_date"),
                          pl.col('prd_key').str.strip_chars().str.slice(
                0, 4).str.replace('-', '_').alias("prd_cat"),
                pl.col('prd_key').str.strip_chars().str.slice(
                6).alias("prd_key"),
                pl.when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "R").then(pl.lit("Road")).
                when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "S").then(pl.lit("Other Sales")).
                when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "M").then(pl.lit("Mountain")).
                when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "T").then(pl.lit("Touring")).
                otherwise(pl.lit("Unidentified")).alias("prd_line"),
                pl.when((pl.col("prd_cost").is_null()) | (pl.col("prd_cost") < 0)).
                then(pl.lit(0)).otherwise(pl.col("prd_cost")).alias("prd_cost")]).\
            select([pl.col("prd_id"), "prd_key", "prd_cat", pl.col("prd_nm"),
                    pl.col("prd_cost"), pl.col("prd_line"), pl.col(
                        "prd_start_dt"),
                    pl.when((pl.col("prd_start_dt") > pl.col(
                        'prd_end_dt')) | (pl.col('prd_end_dt') > pl.lit(datetime.now().date()))).
                    then(pl.col("prd_end_date").dt.offset_by("-1d")).
                    otherwise(pl.col("prd_end_dt")).alias("prd_end_dt"), pl.col("inserted_at").alias("bronze_inserted_at")])
        return fixed_crm_prd_info
# ===================================================================================================================================
    if table_name == "crm_sale_details":
        crm_sale_details_query = """
        SELECT * FROM bronze.crm_sale_details
        """
        df = pl.read_database_uri(
            query=crm_sale_details_query, uri=connection_url)
        fix_crm_sale_details = df.with_columns([pl.col("sls_order_dt").cast(
            pl.String).str.to_date("%Y%m%d", strict=False).alias("sls_order_dt"),
            pl.col("sls_ship_dt").cast(
            pl.String).str.to_date("%Y%m%d", strict=False).alias("sls_ship_dt"),
            pl.col("sls_due_dt").cast(
            pl.String).str.to_date("%Y%m%d", strict=False).alias("sls_due_dt")]).\
            with_columns(pl.when(pl.col("sls_quantity") == 0).then(pl.lit(1)).otherwise(pl.col("sls_quantity").abs()).alias("sls_quantity")).\
            with_columns([pl.when((pl.col("sls_sales").is_null()) | (pl.col("sls_sales") <= 0) |
                          (pl.col("sls_sales") != pl.col("sls_quantity").mul(pl.col("sls_price").abs())) &
                          ((pl.col("sls_price").is_not_null()) | (pl.col("sls_price") <= 0)))
                          .then(pl.col("sls_quantity").abs().mul(pl.col("sls_price").abs())).otherwise(pl.col("sls_sales").abs()).alias("sls_sales"),
                          pl.when((pl.col("sls_price").is_null()) | (pl.col("sls_price") <= 0)).
                          then(pl.col("sls_sales")/pl.col("sls_quantity")).otherwise(pl.col("sls_price").abs()).alias("sls_price")])\
            .select(pl.col("sls_ord_num").str.strip_chars(),
                    pl.col("sls_prd_key").str.strip_chars(),
                    pl.col("sls_cust_id"),
                    "sls_order_dt", "sls_due_dt", "sls_ship_dt",
                    "sls_price", "sls_quantity", "sls_sales", pl.col("inserted_at").alias("bronze_inserted_at"))
        return fix_crm_sale_details
# ===================================================================================================================================
    if table_name == "erp_cust_az12":
        erp_cust_az12_query = """
    SELECT * FROM bronze.erp_cust_az12
    """
        df = pl.read_database_uri(
            query=erp_cust_az12_query, uri=connection_url)
        fixing_erp_customer_az12 = df.with_columns([pl.when(pl.col("gen").str.strip_chars().str.to_uppercase() == "F").then(pl.lit("Female")).
                                                    when(pl.col("gen").str.strip_chars().str.to_uppercase() == "M").then(pl.lit("Male")).
                                                    when(pl.col("gen").str.strip_chars() == "").then(pl.lit(None)).
                                                    otherwise(pl.col("gen").str.to_titlecase()).alias(
            "gen"),
            pl.when(pl.col("cid").str.strip_chars().str.starts_with("NAS")).then(
            pl.col("cid").str.slice(3)).otherwise(pl.col("cid")).alias("cid"),
            pl.when(pl.col("bdate") > datetime.now().date()).then(pl.lit(None)).otherwise(pl.col("bdate")).alias("bdate")]).\
            select([pl.col("cid"), "gen", "bdate", pl.col(
                "inserted_at").alias("bronze_inserted_at")])
        return fixing_erp_customer_az12
# ===================================================================================================================================
    if table_name == "erp_loc_a101":
        erp_loc_a101_query = """
    SELECT * FROM bronze.erp_loc_a101
    """
        df = pl.read_database_uri(
            query=erp_loc_a101_query, uri=connection_url)
        fix_erp_loc_a101 = df.with_columns([pl.when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "USA").then(pl.lit("United States"))
                                            .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "US").then(pl.lit("United States"))
                                            .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "DE").then(pl.lit("Germany"))
                                            .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "").then(pl.lit(None))
                                            .otherwise(pl.col("cntry")).alias("cntry"),
                                            pl.when(pl.col("cid").str.strip_chars().str.contains("-")).
                                            then(pl.col("cid").str.strip_chars().str.replace_all("-", "")).
                                            otherwise(pl.col("cid").str.strip_chars()).alias("cid")]).\
            select(["cid", "cntry", pl.col(
                "inserted_at").alias("bronze_inserted_at")])
        return fix_erp_loc_a101
# ===================================================================================================================================
    if table_name == "erp_px_cat_g1v2":
        erp_px_cat_g1v2_query = """
        SELECT * FROM bronze.erp_px_cat_g1v2
        """
        df = pl.read_database_uri(
            query=erp_px_cat_g1v2_query, uri=connection_url)
        erp_px_cat_g1v2 = df.select([pl.col("id"), pl.col("cat"),
                                     pl.col("subcat"), "maintenance",
                                     pl.col("inserted_at").alias("bronze_inserted_at")])
        return erp_px_cat_g1v2
