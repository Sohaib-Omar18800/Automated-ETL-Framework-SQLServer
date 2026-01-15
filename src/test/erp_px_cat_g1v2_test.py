import polars as pl
from config.sql_service_var import sql_server_var
pl.Config.set_tbl_cols(-1)
# **************************************************************
#                       MAIN DATAFRAME
# **************************************************************

query = """
SELECT * FROM bronze.erp_px_cat_g1v2
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
print(df.head(5))

# **************************************************************
#                   CHECKING maintenance VALUES
# **************************************************************
maintenance = df.select(pl.col("maintenance").unique())
print(maintenance)
# ┌─────────────┐
# │ maintenance │
# │ ---         │
# │ str         │
# ╞═════════════╡
# │ No          │
# │ Yes         │
# └─────────────┘
# **************************************************************
#                   CHECKING cat VALUES
# **************************************************************
cat = df.select(pl.col("cat").unique())
print(cat)
# ┌─────────────┐
# │ cat         │
# │ ---         │
# │ str         │
# ╞═════════════╡
# │ Bikes       │
# │ Accessories │
# │ Clothing    │
# │ Components  │
# └─────────────┘
# **************************************************************
#       DISCLAIMER: THE TABLE IS CLEAN FROM THE SOURCE
# **************************************************************
erp_px_cat_g1v2 = df.select([pl.col("id").alias("cat_id"), pl.col("cat").alias("category"),
                             pl.col("subcat").alias(
                                 "sub_category"), "maintenance",
                             pl.col("inserted_at").alias("bronze_inserted_at")])
print(erp_px_cat_g1v2)
# ┌────────┬─────────────┬───────────────────┬─────────────┬─────────────────────────┐
# │ cat_id ┆ category    ┆ sub_category      ┆ maintenance ┆ bronze_inserted_at      │
# │ ---    ┆ ---         ┆ ---               ┆ ---         ┆ ---                     │
# │ str    ┆ str         ┆ str               ┆ str         ┆ datetime[μs]            │
# ╞════════╪═════════════╪═══════════════════╪═════════════╪═════════════════════════╡
# │ AC_BR  ┆ Accessories ┆ Bike Racks        ┆ Yes         ┆ 2026-01-10 03:32:18.680 │
# │ AC_BS  ┆ Accessories ┆ Bike Stands       ┆ No          ┆ 2026-01-10 03:32:18.680 │
# │ AC_BC  ┆ Accessories ┆ Bottles and Cages ┆ No          ┆ 2026-01-10 03:32:18.680 │
