import pyodbc


def create_silver_tables(cursor):
    """
THIS FUNCTION IS USED FOR CREATING
SILVER TABLES IF THEY ARE NULL
crm_cust_info, crm_prd_info, crm_sale_details,
erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
    """
    # ******************************************
    #           TABLES IN SILVER LAYER
    # ******************************************
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.crm_cust_info QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_crm_customer_info_query = """
    IF OBJECT_ID('silver.crm_cust_info','U') IS NULL
        BEGIN
            CREATE TABLE silver.crm_cust_info(
            cst_id int,
            cst_key NVARCHAR(50),
            cst_firstname NVARCHAR(50),
            cst_lastname NVARCHAR(50),
            cst_material_status NVARCHAR(25),
            cst_gndr NVARCHAR(25),
            cst_create_date DATE,
            bronze_inserted_at DATETIME,
            dwh_inserted_at DATETIME DEFAULT GETDATE()
            )
        END
    """
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.crm_prd_info QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_crm_prd_info_query = """
        IF OBJECT_ID('silver.crm_prd_info','U') IS NULL
        BEGIN
        CREATE TABLE silver.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_cat NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost DECIMAL(18,2),
        prd_line NVARCHAR(20),
        prd_start_dt DATE,
        prd_end_dt DATE,
        bronze_inserted_at DATETIME,
        dwh_inserted_at DATETIME DEFAULT GETDATE());
        END
    """
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.crm_sale_details QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_crm_sale_details_query = """
        IF OBJECT_ID('silver.crm_sale_details','U') IS NULL
        BEGIN
        CREATE TABLE silver.crm_sale_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt DATE,
        sls_due_dt DATE,
        sls_ship_dt DATE,
        sls_price DECIMAL(18,2),
        sls_quantity INT,
        sls_sales DECIMAL(18,2),
        bronze_inserted_at DATETIME,
        dwh_inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.erp_cust_az12 QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_erp_cust_az12 = """
        IF OBJECT_ID('silver.erp_cust_az12','U') IS NULL
        BEGIN
        CREATE TABLE silver.erp_cust_az12(
        cid NVARCHAR(50),
        gen NVARCHAR(20),
        bdate DATE,
        bronze_inserted_at DATETIME,
        dwh_inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.erp_loc_a101 QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_erp_loc_a101 = """
        IF OBJECT_ID('silver.erp_loc_a101','U') IS NULL
        BEGIN
        CREATE TABLE silver.erp_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50),
        bronze_inserted_at DATETIME,
        dwh_inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE silver.erp_px_cat_g1v2 QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_silver_erp_px_cat_g1v2 = """
        IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NULL
        BEGIN
        CREATE TABLE silver.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(20),
        bronze_inserted_at DATETIME,
        dwh_inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    cursor.execute(create_silver_crm_customer_info_query)
    cursor.execute(create_silver_crm_prd_info_query)
    cursor.execute(create_silver_crm_sale_details_query)
    cursor.execute(create_silver_erp_cust_az12)
    cursor.execute(create_silver_erp_loc_a101)
    cursor.execute(create_silver_erp_px_cat_g1v2)
    print('Creation of Silver Tables Completed')
