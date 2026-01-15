import polars as pl
from datetime import datetime
from config.sql_service_var import sql_server_var

# **************************************************************
#                       MAIN DATAFRAME
# **************************************************************

query = """
SELECT * FROM bronze.crm_prd_info
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
print(df.head(10))
# **************************************************************

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#           CREATING category_key and product_key
# **************************************************************
# creating product_key and category_key as we saw in print_sample_of_all.py we need them for joining tables

# add_product_categ_key = df.with_columns([pl.col('prd_key').str.strip_chars().str.slice(0, 4).str.replace('-', '_').alias("category_key"),
#                                          pl.col('prd_key').str.strip_chars().str.slice(6).alias("product_key")])
# print(add_product_categ_key)
# ┌────────┬──────────────────┬───────────────────────────┬──────────┬───┬────────────┬────────────────────────────┬──────────────┬─────────────┐
# │ prd_id ┆ prd_key          ┆ prd_nm                    ┆ prd_cost ┆ … ┆ prd_end_dt ┆ inserted_at                ┆ category_key ┆ product_key │
# │ ---    ┆ ---              ┆ ---                       ┆ ---      ┆   ┆ ---        ┆ ---                        ┆ ---          ┆ ---         │
# │ i64    ┆ str              ┆ str                       ┆ f64      ┆   ┆ date       ┆ datetime[μs]               ┆ str          ┆ str         │
# ╞════════╪══════════════════╪═══════════════════════════╪══════════╪═══╪════════════╪════════════════════════════╪══════════════╪═════════════╡
# │ 210    ┆ CO-RF-FR-R92B-58 ┆ HL Road Frame - Black- 58 ┆ null     ┆ … ┆ null       ┆ 2026-01-10 03:29:57.206666 ┆ CO_R         ┆ FR-R92B-58  │
# │ 211    ┆ CO-RF-FR-R92R-58 ┆ HL Road Frame - Red- 58   ┆ null     ┆ … ┆ null       ┆ 2026-01-10 03:29:57.206666 ┆ CO_R         ┆ FR-R92R-58  │
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                  FIXING prd_end_date DATE
# **************************************************************
# NOTE : THAT SOME prd_end_dt ARE BEFORE THE prd_st_dt OR EVEN AFTER THE CURRENT DATE WHICH DOESNT MAKE SENESE
# ------------------------------------------------------------------

# date_fix = df.sort(["prd_key", "prd_start_dt"]).with_columns(pl.col("prd_start_dt").shift(-1).over("prd_key").alias("product_end_dt")).\
#     select(["prd_key", "prd_start_dt", pl.when((pl.col("prd_start_dt") > pl.col(
#         'prd_end_dt')) | (pl.col('prd_end_dt') > pl.lit(datetime.now().date()))).
#         then(pl.col("product_end_dt").dt.offset_by("-1d")).
#         otherwise(pl.col("prd_end_dt")).alias("product_end_date")])
# print(date_fix)
# ┌───────────────┬──────────────┬──────────────────┐
# │ prd_key       ┆ prd_start_dt ┆ product_end_date │
# │ ---           ┆ ---          ┆ ---              │
# │ str           ┆ date         ┆ date             │
# ╞═══════════════╪══════════════╪══════════════════╡
# │ AC-HE-HL-U509 ┆ 2011-07-01   ┆ 2012-06-30       │
# │ AC-HE-HL-U509 ┆ 2012-07-01   ┆ 2013-06-30       │
# │ AC-HE-HL-U509 ┆ 2013-07-01   ┆ null             │
# └───────────────┴──────────────┴──────────────────┘
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#               FIXING THE crm_prd_info_table
# **************************************************************
fixed_crm_prd_info = df.sort(["prd_key", "prd_start_dt"]).\
    with_columns([pl.col("prd_start_dt").shift(-1).over("prd_key").alias("product_end_dt"),
                  pl.col('prd_key').str.strip_chars().str.slice(
                      0, 4).str.replace('-', '_').alias("category_key"),
                  pl.col('prd_key').str.strip_chars().str.slice(
                      6).alias("product_key"),
                  pl.when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "R").then(pl.lit("Road")).
                  when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "S").then(pl.lit("Other Sales")).
                  when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "M").then(pl.lit("Mountain")).
                  when(pl.col("prd_line").str.strip_chars().str.to_uppercase() == "T").then(pl.lit("Touring")).
                  otherwise(pl.lit("Unidentified")).alias("product_line")]).\
    select([pl.col("prd_id").alias("product_id"), "product_key", "category_key", pl.col("prd_nm").alias("product_name"),
            pl.col("prd_cost").fill_null(0).alias(
        "product_cost"), pl.col("product_line"), pl.col("prd_start_dt").alias("product_start_date"),
        pl.when((pl.col("prd_start_dt") > pl.col(
            'prd_end_dt')) | (pl.col('prd_end_dt') > pl.lit(datetime.now().date()))).
        then(pl.col("product_end_dt").dt.offset_by("-1d")).
        otherwise(pl.col("prd_end_dt")).alias("product_end_date"), pl.col("inserted_at").alias("bronze_inserted_at")])
print(fixed_crm_prd_info)
# ┌────────────┬─────────────┬──────────────┬────────────────────────┬──────────────┬──────────────┬────────────────────┬──────────────────┐
# │ product_id ┆ product_key ┆ category_key ┆ product_name           ┆ product_cost ┆ product_line ┆ product_start_date ┆ product_end_date │
# │ ---        ┆ ---         ┆ ---          ┆ ---                    ┆ ---          ┆ ---          ┆ ---                ┆ ---              │
# │ i64        ┆ str         ┆ str          ┆ str                    ┆ f64          ┆ str          ┆ date               ┆ date             │
# ╞════════════╪═════════════╪══════════════╪════════════════════════╪══════════════╪══════════════╪════════════════════╪══════════════════╡
# │ 478        ┆ BC-M005     ┆ AC_B         ┆ Mountain Bottle Cage   ┆ 4.0          ┆ Mountain     ┆ 2013-07-01         ┆ null             │
# │ 479        ┆ BC-R205     ┆ AC_B         ┆ Road Bottle Cage       ┆ 3.0          ┆ Road         ┆ 2013-07-01         ┆ null             │
# │ 477        ┆ WB-H098     ┆ AC_B         ┆ Water Bottle - 30 oz.  ┆ 2.0          ┆ Other Sales  ┆ 2013-07-01         ┆ null             │
# │ 483        ┆ RA-H123     ┆ AC_B         ┆ Hitch Rack - 4-Bike    ┆ 45.0         ┆ Other Sales  ┆ 2013-07-01         ┆ null             │
# │ 486        ┆ ST-1401     ┆ AC_B         ┆ All-Purpose Bike Stand ┆ 59.0         ┆ Mountain     ┆ 2013-07-01         ┆ null             │
