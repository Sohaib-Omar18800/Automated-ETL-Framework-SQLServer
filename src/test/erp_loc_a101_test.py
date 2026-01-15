import polars as pl
from config.sql_service_var import sql_server_var
# pl.Config.set_tbl_rows(-1)
# **************************************************************
#                       MAIN DATAFRAME
# **************************************************************

query = """
SELECT * FROM bronze.erp_loc_a101
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
print(df.head(5))
# **************************************************************

# **************************************************************
#               REMOVING THE EXTRA HYPHEN IN cid
# **************************************************************
# fixing_cid = df.with_columns([pl.when(pl.col("cid").str.strip_chars().str.contains("-")).
#                               then(pl.col("cid").str.strip_chars().str.replace_all("-", "")).
#                               otherwise(pl.col("cid").str.strip_chars().alias("customer_id"))])
# print(fixing_cid)
# **************************************************************

# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                   CHECKING cntry VALUES
# **************************************************************
# check_cntry = df.select(pl.col("cntry").unique())
# print(check_cntry)
# ┌────────────────┐
# │ cntry          │
# │ ---            │
# │ str            │
# ╞════════════════╡
# │ Germany        │
# │ United Kingdom │
# │ United States  │
# │ Australia      │
# │ USA            │
# │ France         │
# │ US             │
# │                │
# │                │
# │ Canada         │
# │ DE             │
# │ null           │
# │                │
# └────────────────┘
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                        FIXING cntry
# **************************************************************
# fix_cntry = df.with_columns([pl.when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "USA").then(pl.lit("United States"))
#                              .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "US").then(pl.lit("United States"))
#                              .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "DE").then(pl.lit("Germany"))
#                              .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "").then(pl.lit(None))
#                              .otherwise(pl.col("cntry")).alias("country")]).select(pl.col("country").unique())
# print(fix_cntry)
# ┌────────────────┐
# │ country        │
# │ ---            │
# │ str            │
# ╞════════════════╡
# │ France         │
# │ null           │
# │ United Kingdom │
# │ United States  │
# │ Canada         │
# │ Australia      │
# │ Germany        │
# └────────────────┘
# **************************************************************
# ------------------------------------------------------------------
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ------------------------------------------------------------------

# **************************************************************
#                        FIXING erp_loc_a101
# **************************************************************
fix_erp_loc_a101 = df.with_columns([pl.when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "USA").then(pl.lit("United States"))
                                    .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "US").then(pl.lit("United States"))
                                    .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "DE").then(pl.lit("Germany"))
                                    .when(pl.col("cntry").str.strip_chars().str.to_uppercase() == "").then(pl.lit(None))
                                    .otherwise(pl.col("cntry")).alias("country"),
                                    pl.when(pl.col("cid").str.strip_chars().str.contains("-")).
                                    then(pl.col("cid").str.strip_chars().str.replace_all("-", "")).
                                    otherwise(pl.col("cid").str.strip_chars()).alias("customer_id")]).\
    select(["customer_id", "country", pl.col(
        "inserted_at").alias("bronze_inserted_at")])
print(fix_erp_loc_a101)
# ┌─────────────┬────────────────┬────────────────────────────┐
# │ customer_id ┆ country        ┆ bronze_inserted_at         │
# │ ---         ┆ ---            ┆ ---                        │
# │ str         ┆ str            ┆ datetime[μs]               │
# ╞═════════════╪════════════════╪════════════════════════════╡
# │ AW00011000  ┆ Australia      ┆ 2026-01-10 03:32:01.496666 │
# │ AW00011001  ┆ Australia      ┆ 2026-01-10 03:32:01.496666 │
# │ AW00011002  ┆ Australia      ┆ 2026-01-10 03:32:01.496666 │
# │ AW00011003  ┆ Australia      ┆ 2026-01-10 03:32:01.496666 │
# │ AW00011004  ┆ Australia      ┆ 2026-01-10 03:32:01.496666 │
