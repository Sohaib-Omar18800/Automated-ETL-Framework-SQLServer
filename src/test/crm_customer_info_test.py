from config.sql_service_var import sql_server_var
import polars as pl


# ===================================================================================================================
query = """
SELECT * FROM bronze.crm_cust_info
"""
user_info = sql_server_var()
df = pl.read_database_uri(query=query, uri=user_info[1])
# print(df)
# FIRST QUERY RESULT
# cst_id ┆ cst_key    ┆ cst_firstname ┆ cst_lastname ┆ cst_material_status ┆ cst_gndr ┆ cst_create_date ┆ inserted_at                │
# │ ---    ┆ ---        ┆ ---           ┆ ---          ┆ ---                 ┆ ---      ┆ ---             ┆ ---                        │
# │ i64    ┆ str        ┆ str           ┆ str          ┆ str                 ┆ str      ┆ date            ┆ datetime[μs]               │
# ╞════════╪════════════╪═══════════════╪══════════════╪═════════════════════╪══════════╪═════════════════╪════════════════════════════╡
# │ 11000  ┆ AW00011000 ┆  Jon          ┆ Yang         ┆ M                   ┆ M        ┆ 2025-10-06      ┆ 2026-01-10 03:28:43.203333 │
# │ 11001  ┆ AW00011001 ┆ Eugene        ┆ Huang        ┆ S                   ┆ M        ┆ 2025-10-06      ┆ 2026-01-10 03:28:43.203333 │
# │ 11002  ┆ AW00011002 ┆ Ruben         ┆  Torres      ┆ M                   ┆ M        ┆ 2025-10-06      ┆ 2026-01-10 03:28:43.203333 │
# │ 11003  ┆ AW00011003 ┆ Christy       ┆   Zhu        ┆ S                   ┆ F        ┆ 2025-10-06      ┆ 2026-01-10 03:28:43.203333 │
# ===================================================================================================================
#                                                 CHECK cst_id
# ===================================================================================================================
# WE TEST THE DUPLICATES AND CHECK THE NULLS
# test_duplicates = df.group_by(['cst_id', 'cst_key']).agg(
#     pl.col("cst_id").count().alias('counted')).filter(pl.col('counted') > 1)
# test_nulls = df.select('*').filter(pl.col('cst_id').is_null())

# print(test_duplicates)
# -------------------------------------------------------------------------------------------------------------------
# ┌────────┬────────────┬─────────┐
# │ cst_id ┆ cst_key    ┆ counted │
# │ ---    ┆ ---        ┆ ---     │
# │ i64    ┆ str        ┆ u32     │
# ╞════════╪════════════╪═════════╡
# │ 29473  ┆ AW00029473 ┆ 2       │
# │ 29449  ┆ AW00029449 ┆ 2       │
# │ 29483  ┆ AW00029483 ┆ 2       │
# │ 29466  ┆ AW00029466 ┆ 3       │
# │ 29433  ┆ AW00029433 ┆ 2       │
# └────────┴────────────┴─────────┘
# -------------------------------------------------------------------------------------------------------------------

# print(test_nulls)
# -------------------------------------------------------------------------------------------------------------------
# ┌────────┬──────────┬───────────────┬──────────────┬─────────────────────┬──────────┬─────────────────┬────────────────────────────┐
# │ cst_id ┆ cst_key  ┆ cst_firstname ┆ cst_lastname ┆ cst_material_status ┆ cst_gndr ┆ cst_create_date ┆ inserted_at                │
# │ ---    ┆ ---      ┆ ---           ┆ ---          ┆ ---                 ┆ ---      ┆ ---             ┆ ---                        │
# │ i64    ┆ str      ┆ str           ┆ str          ┆ str                 ┆ str      ┆ date            ┆ datetime[μs]               │
# ╞════════╪══════════╪═══════════════╪══════════════╪═════════════════════╪══════════╪═════════════════╪════════════════════════════╡
# │ null   ┆ SF566    ┆ null          ┆ null         ┆ null                ┆ null     ┆ null            ┆ 2026-01-10 03:28:45.193333 │
# │ null   ┆ PO25     ┆ null          ┆ null         ┆ null                ┆ null     ┆ null            ┆ 2026-01-10 03:28:45.193333 │
# │ null   ┆ 13451235 ┆ null          ┆ null         ┆ null                ┆ null     ┆ null            ┆ 2026-01-10 03:28:45.193333 │
# │ null   ┆ A01Ass   ┆ null          ┆ null         ┆ null                ┆ null     ┆ null            ┆ 2026-01-10 03:28:45.193333 │
# └────────┴──────────┴───────────────┴──────────────┴─────────────────────┴──────────┴─────────────────┴────────────────────────────┘
# -------------------------------------------------------------------------------------------------------------------

# ===================================================================================================================
# ** ITS CLEAR THAT THERES DUPLICATES AND NULLS REGARDING cst_id COLUMN SO LETS TAKE A LOOK AND KNOW WHY **
# ===================================================================================================================

# explode_test_dup = df.select('*').filter(pl.col('cst_id') == 29433)
# print(explode_test_dup)
# -------------------------------------------------------------------------------------------------------------------
# ┌────────┬────────────┬───────────────┬──────────────┬─────────────────────┬──────────┬─────────────────┬────────────────────────────┐
# │ cst_id ┆ cst_key    ┆ cst_firstname ┆ cst_lastname ┆ cst_material_status ┆ cst_gndr ┆ cst_create_date ┆ inserted_at                │
# │ ---    ┆ ---        ┆ ---           ┆ ---          ┆ ---                 ┆ ---      ┆ ---             ┆ ---                        │
# │ i64    ┆ str        ┆ str           ┆ str          ┆ str                 ┆ str      ┆ date            ┆ datetime[μs]               │
# ╞════════╪════════════╪═══════════════╪══════════════╪═════════════════════╪══════════╪═════════════════╪════════════════════════════╡
# │ 29433  ┆ AW00029433 ┆ null          ┆ null         ┆ M                   ┆ M        ┆ 2026-01-25      ┆ 2026-01-10 03:28:45.193333 │
# │ 29433  ┆ AW00029433 ┆ Thomas        ┆ King         ┆ M                   ┆ M        ┆ 2026-01-27      ┆ 2026-01-10 03:28:45.193333 │
# └────────┴────────────┴───────────────┴──────────────┴─────────────────────┴──────────┴─────────────────┴────────────────────────────┘
# -------------------------------------------------------------------------------------------------------------------

# ===================================================================================================================
#                                                cst_id FIX
# ===================================================================================================================
# WE NOTICE THAT THE DUPLICATE VALUES ARE JUST OUTDATED UPDATE INFO THAT NEED TO BE REMOVED
# fixed_cst_df = df.drop_nulls(subset=pl.col(['cst_id'])).\
#     sort(by=pl.col('cst_create_date'), descending=True).\
#     unique(pl.col('cst_key'), keep='first')
# print(fixed_cst_df)
# ===================================================================================================================
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ===================================================================================================================

# ===================================================================================================================
#                                                 CHECK cst_key
# ===================================================================================================================
# test_cst_key = df.select('cst_key').filter(pl.col('cst_key').is_null())
# print(test_cst_key)

# NO NULLS AND SINCE THE DUPLICATE BECASE OF cst_id AND WE FIXED THAT ALREADY cst_key IS CLEAR
# ===================================================================================================================
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ===================================================================================================================

# ===================================================================================================================
#                                            CHECK cst_firstname & cst_lastname
# ===================================================================================================================
# test_cst_firstname = df.select(pl.col('cst_firstname'), pl.col('cst_firstname').str.len_chars().alias('count_chars')).\
#     filter(pl.col('count_chars') != pl.col(
#         'cst_firstname').str.strip_chars().str.len_chars())
# print(test_cst_firstname)

# ITS CLEAR THAT THERES UNWANTED SPACES
# ┌───────────────┬─────────────┐
# │ cst_firstname ┆ count_chars │
# │ ---           ┆ ---         │
# │ str           ┆ u32         │
# ╞═══════════════╪═════════════╡
# │  Jon          ┆ 4           │
# │  Elizabeth    ┆ 10          │
# │   Lauren      ┆ 8           │
# │  Ian          ┆ 5           │
# │   Chloe       ┆ 7           │
# │ …             ┆ …           │
# │  Nicole       ┆ 7           │
# │  Maria        ┆ 7           │
# │  Allison      ┆ 9           │
# │  Adrian       ┆ 8           │
# │ Victoria      ┆ 10          │
# └───────────────┴─────────────┘

# ===================================================================================================================
#                                          cst_lastname & cst_firstname FIX
# ===================================================================================================================
# fixed_cst_first_name = df.select([pl.col(
#     'cst_firstname').str.strip_chars(), pl.col('cst_lastname').str.strip_chars()])
# print(fixed_cst_first_name)
# ===================================================================================================================
# BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK BREAK
# ===================================================================================================================

# ===================================================================================================================
#                                               cst_table FIX
# ===================================================================================================================

# -------------------------------------------------------------------------------------------------------------------
# PLEASE NOTICE THAT WE CHANGED THE VALUES of customer_gender & customer_material_status TO BE MORE MEANIGFULL AND THE COLUMN NAMES
# -------------------------------------------------------------------------------------------------------------------

fixed_cst_table = df.drop_nulls(subset=pl.col(['cst_id'])).\
    sort(by=pl.col('cst_create_date'), descending=True).\
    unique(pl.col('cst_key'), keep='first').\
    select([pl.col('cst_id').alias('customer_id'), pl.col('cst_key').alias('customer_key'),
            pl.col('cst_firstname').str.strip_chars().alias('customer_firstname'), pl.col(
                'cst_lastname').str.strip_chars().alias('customer_lastname'),
            pl.when(pl.col('cst_material_status').str.to_uppercase().str.strip_chars() == 'M').then(pl.lit('Married')).
            when(pl.col('cst_material_status').str.to_uppercase().str.strip_chars() == 'S').then(pl.lit('Single')).
            otherwise(pl.lit('n/a')).alias('customer_material_status'),
            pl.when(pl.col('cst_gndr').str.to_uppercase().str.strip_chars() == 'M').then(pl.lit('Male')).
            when(pl.col('cst_gndr').str.to_uppercase().str.strip_chars() == 'F').then(pl.lit('Female')).
            otherwise(pl.lit('n/a')).alias('customer_gender'),
            pl.col('cst_create_date').alias('customer_create_date'),
            pl.col('inserted_at').alias('bronze_inserted_at')])
print(fixed_cst_table)

# ┌─────────────┬──────────────┬────────────────────┬───────────────────┬──────────────────────────┬─────────────────┬──────────────────────┬────────────────────────────┐
# │ customer_id ┆ customer_key ┆ customer_firstname ┆ customer_lastname ┆ customer_material_status ┆ customer_gender ┆ customer_create_date ┆ bronze_inserted_at         │
# │ ---         ┆ ---          ┆ ---                ┆ ---               ┆ ---                      ┆ ---             ┆ ---                  ┆ ---                        │
# │ i64         ┆ str          ┆ str                ┆ str               ┆ str                      ┆ str             ┆ date                 ┆ datetime[μs]               │
# ╞═════════════╪══════════════╪════════════════════╪═══════════════════╪══════════════════════════╪═════════════════╪══════════════════════╪════════════════════════════╡
# │ 21410       ┆ AW00021410   ┆ Eric               ┆ Zhang             ┆ Married                  ┆ Male            ┆ 2026-01-05           ┆ 2026-01-10 03:28:44.743333 │
# │ 27282       ┆ AW00027282   ┆ Barry              ┆ Perez             ┆ Married                  ┆ Male            ┆ 2026-01-14           ┆ 2026-01-10 03:28:44.896666 │
# │ 19465       ┆ AW00019465   ┆ Sierra             ┆ Baker             ┆ Single                   ┆ Female          ┆ 2026-01-05           ┆ 2026-01-10 03:28:44.700    │
