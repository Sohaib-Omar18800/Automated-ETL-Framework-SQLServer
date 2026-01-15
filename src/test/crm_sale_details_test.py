import polars as pl
from config.sql_service_var import sql_server_var
pl.Config.set_tbl_cols(-1)

# **************************************************************
#                       MAIN DATAFRAME
# **************************************************************
query = """
SELECT * FROM bronze.crm_sale_details
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
print(df.head(5))
# ┌─────────────┬─────────────┬─────────────┬──────────────┬─────────────┬────────────┬───────────┬──────────────┬───────────┬────────────────────────────┐
# │ sls_ord_num ┆ sls_prd_key ┆ sls_cust_id ┆ sls_order_dt ┆ sls_ship_dt ┆ sls_due_dt ┆ sls_sales ┆ sls_quantity ┆ sls_price ┆ inserted_at                │
# │ ---         ┆ ---         ┆ ---         ┆ ---          ┆ ---         ┆ ---        ┆ ---       ┆ ---          ┆ ---       ┆ ---                        │
# │ str         ┆ str         ┆ i64         ┆ i64          ┆ i64         ┆ i64        ┆ f64       ┆ i64          ┆ f64       ┆ datetime[μs]               │
# ╞═════════════╪═════════════╪═════════════╪══════════════╪═════════════╪════════════╪═══════════╪══════════════╪═══════════╪════════════════════════════╡
# │ SO63553     ┆ TI-M267     ┆ 14312       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 25.0      ┆ 1            ┆ 25.0      ┆ 2026-01-10 03:30:19.126666 │
# │ SO63554     ┆ TI-M602     ┆ 18266       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 30.0      ┆ 1            ┆ 30.0      ┆ 2026-01-10 03:30:19.126666 │
# │ SO63554     ┆ TT-M928     ┆ 18266       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 5.0       ┆ 1            ┆ 5.0       ┆ 2026-01-10 03:30:19.126666 │
# │ SO63554     ┆ PK-7098     ┆ 18266       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 2.0       ┆ 1            ┆ 2.0       ┆ 2026-01-10 03:30:19.126666 │
# │ SO63555     ┆ CA-1098     ┆ 15253       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 9.0       ┆ 1            ┆ 9.0       ┆ 2026-01-10 03:30:19.126666 │
# **************************************************************

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#          TRANSFORM dts COLUMNS TO DATEDATATYPE
# **************************************************************

# to_date = df.with_columns([pl.col("sls_order_dt").cast(
#     pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_order_date"),
#     pl.col("sls_ship_dt").cast(
#     pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_ship_date"),
#     pl.col("sls_due_dt").cast(
#     pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_due_date")])
# print(to_date)
# ┌─────────────┬─────────────┬─────────────┬──────────────┬─────────────┬────────────┬───────────┬──────────────┬───────────┬──────────────┬──────────────┬──────────────┬─────────────┐
# │ sls_ord_num ┆ sls_prd_key ┆ sls_cust_id ┆ sls_order_dt ┆ sls_ship_dt ┆ sls_due_dt ┆ sls_sales ┆ sls_quantity ┆ sls_price ┆ inserted_at  ┆ sales_order_ ┆ sales_ship_d ┆ sales_due_d │
# │ ---         ┆ ---         ┆ ---         ┆ ---          ┆ ---         ┆ ---        ┆ ---       ┆ ---          ┆ ---       ┆ ---          ┆ date         ┆ ate          ┆ ate         │
# │ str         ┆ str         ┆ i64         ┆ i64          ┆ i64         ┆ i64        ┆ f64       ┆ i64          ┆ f64       ┆ datetime[μs] ┆ ---          ┆ ---          ┆ ---         │
# │             ┆             ┆             ┆              ┆             ┆            ┆           ┆              ┆           ┆              ┆ date         ┆ date         ┆ date        │
# ╞═════════════╪═════════════╪═════════════╪══════════════╪═════════════╪════════════╪═══════════╪══════════════╪═══════════╪══════════════╪══════════════╪══════════════╪═════════════╡
# │ SO63553     ┆ TI-M267     ┆ 14312       ┆ 20130804     ┆ 20130811    ┆ 20130816   ┆ 25.0      ┆ 1            ┆ 25.0      ┆ 2026-01-10   ┆ 2013-08-04   ┆ 2013-08-11   ┆ 2013-08-16  │

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#               CHECKING sls_sales, sls_price
# **************************************************************

# check_sales = df.select(["sls_sales", "sls_quantity", "sls_price"]).filter((pl.col("sls_price").is_null()) | (pl.col("sls_sales").is_null()) |
#                                                                            (pl.col("sls_sales") != pl.col("sls_quantity").mul(pl.col("sls_price"))))
# print(check_sales)
# ┌───────────┬──────────────┬───────────┐
# │ sls_sales ┆ sls_quantity ┆ sls_price │
# │ ---       ┆ ---          ┆ ---       │
# │ f64       ┆ i64          ┆ f64       │
# ╞═══════════╪══════════════╪═══════════╡
# │ null      ┆ 1            ┆ 10.0      │
# │ 5.0       ┆ 2            ┆ 5.0       │
# │ 64.0      ┆ 4            ┆ 64.0      │
# │ 25.0      ┆ 3            ┆ 25.0      │
# │ 40.0      ┆ 1            ┆ 2.0       │
# │ …         ┆ …            ┆ …         │
# │ null      ┆ 1            ┆ 8.0       │
# │ null      ┆ 1            ┆ 22.0      │
# │ -18.0     ┆ 1            ┆ 9.0       │
# │ 0.0       ┆ 1            ┆ 10.0      │
# │ 50.0      ┆ 2            ┆ 50.0      │
# └───────────┴──────────────┴───────────┘
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                       FIXING SALES
# **************************************************************
# fixing_sales = df.with_columns(pl.when(pl.col("sls_quantity") == 0).
#                                then(pl.lit(1)).otherwise(pl.col("sls_quantity").abs()).alias("quantity"))\
#     .with_columns([pl.when((pl.col("sls_sales").is_null()) | (pl.col("sls_sales") <= 0) | (pl.col("sls_sales") != pl.col(
#         "quantity").mul(pl.col("sls_price").abs()) &
#         ((pl.col("sls_price").is_not_null()) | (pl.col("sls_price") <= 0)))
#     ).
#         then(pl.col("quantity").abs().mul(pl.col("sls_price").abs())).
#         otherwise(pl.col("sls_sales").abs()).alias("sales"),
#         pl.when((pl.col("sls_price").is_null()) | (pl.col("sls_price") <= 0)
#                 ).
#         then(pl.col("sls_sales")/pl.col("quantity")).otherwise(pl.col("sls_price").abs()).alias("price")]
# ).select(["sales", "quantity", "price"]).filter((pl.col("price").is_null()) | (pl.col("sales").is_null()) |
#                                                 (pl.col("sales") != pl.col("quantity").mul(pl.col("price"))))
# print(fixing_sales)

# ┌───────┬──────────┬───────┐
# │ sales ┆ quantity ┆ price │
# │ ---   ┆ ---      ┆ ---   │
# │ f64   ┆ i64      ┆ f64   │
# ╞═══════╪══════════╪═══════╡
# └───────┴──────────┴───────┘

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                   FIXING crm_sale_details
# **************************************************************
fix_crm_sale_details = df.with_columns([pl.col("sls_order_dt").cast(
    pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_order_date"),
    pl.col("sls_ship_dt").cast(
    pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_ship_date"),
    pl.col("sls_due_dt").cast(
    pl.String).str.to_date("%Y%m%d", strict=False).alias("sales_due_date")]).\
    with_columns(pl.when(pl.col("sls_quantity") == 0).then(pl.lit(1)).otherwise(pl.col("sls_quantity").abs()).alias("quantity")).\
    with_columns([pl.when((pl.col("sls_sales").is_null()) | (pl.col("sls_sales") <= 0) |
                          (pl.col("sls_sales") != pl.col("quantity").mul(pl.col("sls_price").abs())) &
                          ((pl.col("sls_price").is_not_null()) | (pl.col("sls_price") <= 0)))
                  .then(pl.col("quantity").abs().mul(pl.col("sls_price").abs())).otherwise(pl.col("sls_sales").abs()).alias("sales"),
                  pl.when((pl.col("sls_price").is_null()) | (pl.col("sls_price") <= 0)).
                  then(pl.col("sls_sales")/pl.col("quantity")).otherwise(pl.col("sls_price").abs()).alias("price")])\
    .select(pl.col("sls_ord_num").str.strip_chars().alias("sls_order_number"),
            pl.col("sls_prd_key").str.strip_chars().alias("sls_product_key"),
            pl.col("sls_cust_id").alias("sls_customer_id"),
            "sales_order_date", "sales_due_date", "sales_ship_date",
            "price", "quantity", "sales", pl.col("inserted_at").alias("bronze_inserted_at"))
print(fix_crm_sale_details)

# ┌──────────────────┬─────────────────┬─────────────────┬──────────────────┬────────────────┬─────────────────┬───────┬──────────┬───────┐
# │ sls_order_number ┆ sls_product_key ┆ sls_customer_id ┆ sales_order_date ┆ sales_due_date ┆ sales_ship_date ┆ price ┆ quantity ┆ sales │
# │ ---              ┆ ---             ┆ ---             ┆ ---              ┆ ---            ┆ ---             ┆ ---   ┆ ---      ┆ ---   │
# │ str              ┆ str             ┆ i64             ┆ date             ┆ date           ┆ date            ┆ f64   ┆ i64      ┆ f64   │
# ╞══════════════════╪═════════════════╪═════════════════╪══════════════════╪════════════════╪═════════════════╪═══════╪══════════╪═══════╡
# │ SO63553          ┆ TI-M267         ┆ 14312           ┆ 2013-08-04       ┆ 2013-08-16     ┆ 2013-08-11      ┆ 25.0  ┆ 1        ┆ 25.0  │
# │ SO63554          ┆ TI-M602         ┆ 18266           ┆ 2013-08-04       ┆ 2013-08-16     ┆ 2013-08-11      ┆ 30.0  ┆ 1        ┆ 30.0  │
# │ SO63554          ┆ TT-M928         ┆ 18266           ┆ 2013-08-04       ┆ 2013-08-16     ┆ 2013-08-11      ┆ 5.0   ┆ 1        ┆ 5.0   │
