from datetime import datetime
import polars as pl
from config.sql_service_var import sql_server_var
pl.Config.set_tbl_cols(-1)
# **************************************************************
#                       MAIN DATAFRAME
# **************************************************************

query = """
SELECT * FROM bronze.erp_cust_az12
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
print(df.head(5))
# **************************************************************

# **************************************************************
#                        CHECK gen
# **************************************************************
# gen = df.select(pl.col("gen").unique())
# print(gen)
# ┌────────┐
# │ gen    │
# │ ---    │
# │ str    │
# ╞════════╡
# │ F      │
# │ Male   │
# │        │
# │ M      │
# │ null   │
# │ Female │
# │ F      │
# │        │
# │ M      │
# └────────┘
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                            FIX gen
# **************************************************************
# fix_gen = df.with_columns(pl.when(pl.col("gen").str.strip_chars().str.to_uppercase() == "F").then(pl.lit("Female")).
#                           when(pl.col("gen").str.strip_chars().str.to_uppercase() == "M").then(pl.lit("Male")).
#                           when(pl.col("gen").str.strip_chars() == "").then(pl.lit(None)).
#                           otherwise(pl.col("gen").str.to_titlecase()).alias("gender")).\
#     select([pl.col("gender").unique()])
# print(fix_gen)
# ┌────────┐
# │ gender │
# │ ---    │
# │ str    │
# ╞════════╡
# │ Female │
# │ null   │
# │ Male   │
# └────────┘
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                           FIX cid
# **************************************************************
# fix_cid = df.with_columns(
#     pl.when(pl.col("cid").str.strip_chars().str.starts_with("NAS")).then(pl.col("cid").str.slice(3)).otherwise(pl.col("cid")).alias("customer_id"))
# print(fix_cid)
# ┌───────────────┬────────────┬────────┬────────────────────────────┬─────────────┐
# │ cid           ┆ bdate      ┆ gen    ┆ inserted_at                ┆ customer_id │
# │ ---           ┆ ---        ┆ ---    ┆ ---                        ┆ ---         │
# │ str           ┆ date       ┆ str    ┆ datetime[μs]               ┆ str         │
# ╞═══════════════╪════════════╪════════╪════════════════════════════╪═════════════╡
# │ NASAW00011000 ┆ 1971-10-06 ┆ Male   ┆ 2026-01-10 03:31:40.826666 ┆ AW00011000  │
# │ NASAW00011001 ┆ 1976-05-10 ┆ Male   ┆ 2026-01-10 03:31:40.826666 ┆ AW00011001  │
# │ NASAW00011002 ┆ 1971-02-09 ┆ Male   ┆ 2026-01-10 03:31:40.826666 ┆ AW00011002  │

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                          CHECK bdate
# **************************************************************

# bdate = df.select("bdate").filter(
#     (pl.col("bdate") > datetime.now().date()))
# print(bdate)
# ┌────────────┐
# │ bdate      │
# │ ---        │
# │ date       │
# ╞════════════╡
# │ 2050-07-06 │
# │ 2042-02-22 │
# │ 2050-05-21 │
# │ 2038-10-17 │
# │ 2045-03-03 │
# │ …          │
# │ 9999-05-10 │
# │ 2050-09-07 │
# │ 2080-03-15 │
# │ 2055-01-23 │
# │ 2980-03-09 │
# └────────────┘
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                    FIXING erp_customer_az12
# **************************************************************
fixing_erp_customer_az12 = df.with_columns([pl.when(pl.col("gen").str.strip_chars().str.to_uppercase() == "F").then(pl.lit("Female")).
                                            when(pl.col("gen").str.strip_chars().str.to_uppercase() == "M").then(pl.lit("Male")).
                                            when(pl.col("gen").str.strip_chars() == "").then(pl.lit('n/a')).
                                            otherwise(pl.col("gen").str.to_titlecase()).alias(
                                                "gender"),
                                            pl.when(pl.col("cid").str.strip_chars().str.starts_with("NAS")).then(
    pl.col("cid").str.slice(3)).otherwise(pl.col("cid")).alias("customer_id"),
    pl.when(pl.col("bdate") > datetime.now().date()).then(pl.lit(None)).otherwise(pl.col("bdate")).alias("birthdate")]).\
    select([pl.col("customer_id"), "gender", "birthdate", "inserted_at"])
print(fixing_erp_customer_az12)

# ┌─────────────┬────────┬────────────┬────────────────────────────┐
# │ customer_id ┆ gender ┆ birthdate  ┆ inserted_at                │
# │ ---         ┆ ---    ┆ ---        ┆ ---                        │
# │ str         ┆ str    ┆ date       ┆ datetime[μs]               │
# ╞═════════════╪════════╪════════════╪════════════════════════════╡
# │ AW00011000  ┆ Male   ┆ 1971-10-06 ┆ 2026-01-10 03:31:40.826666 │
# │ AW00011001  ┆ Male   ┆ 1976-05-10 ┆ 2026-01-10 03:31:40.826666 │
# │ AW00011002  ┆ Male   ┆ 1971-02-09 ┆ 2026-01-10 03:31:40.826666 │
# │ AW00011003  ┆ Female ┆ 1973-08-14 ┆ 2026-01-10 03:31:40.826666 │
# │ AW00011004  ┆ Female ┆ 1979-08-05 ┆ 2026-01-10 03:31:40.826666 │
