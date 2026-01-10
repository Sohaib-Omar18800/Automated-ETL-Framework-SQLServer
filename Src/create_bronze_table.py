def create_bronze_table(cursor):
    """
THIS FUNCTION IS USED FOR CREATING
BRONZE TABLES IF THEY ARE NULL
crm_cust_info, crm_prd_info, crm_sale_details,
erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
    """
    # ******************************************
    #           TABLES IN BRONZE LAYER
    # ******************************************
    # +++++++++++++++++++++++++++++++++++++++++
    #     TABLE bronze.crm_cust_info QUERIES
    # +++++++++++++++++++++++++++++++++++++++++
    create_bronze_crm_customer_info_query = """
    IF OBJECT_ID('bronze.crm_cust_info','U') IS NULL
        BEGIN
            CREATE TABLE bronze.crm_cust_info(
            cst_id int,
            cst_key NVARCHAR(50),
            cst_firstname NVARCHAR(50),
            cst_lastname NVARCHAR(50),
            cst_material_status NVARCHAR(20),
            cst_gndr NVARCHAR(20),
            cst_create_date DATE,
            inserted_at DATETIME DEFAULT GETDATE()
            )
        END
    """
    create_bronze_crm_prd_info_query = """
        IF OBJECT_ID('bronze.crm_prd_info','U') IS NULL
        BEGIN
        CREATE TABLE bronze.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost DECIMAL(18,2),
        prd_line NVARCHAR(10),
        prd_start_dt DATE,
        prd_end_dt DATE,
        inserted_at DATETIME DEFAULT GETDATE());
        END
    """
    create_bronze_crm_sale_details_query = """
        IF OBJECT_ID('bronze.crm_sale_details','U') IS NULL
        BEGIN
        CREATE TABLE bronze.crm_sale_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales DECIMAL(18,2),
        sls_quantity INT,
        sls_price DECIMAL(18,2),
        inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    create_bronze_erp_cust_az12 = """
        IF OBJECT_ID('bronze.erp_cust_az12','U') IS NULL
        BEGIN
        CREATE TABLE bronze.erp_cust_az12(
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(10),
        inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    create_bronze_erp_loc_a101 = """
        IF OBJECT_ID('bronze.erp_loc_a101','U') IS NULL
        BEGIN
        CREATE TABLE bronze.erp_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50),
        inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    create_bronze_erp_px_cat_g1v2 = """
        IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NULL
        BEGIN
        CREATE TABLE bronze.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(10),
        inserted_at DATETIME DEFAULT GETDATE()
        )
        END
    """
    # E: MAKE SURE TO USE autocommit=True FOR DDL COMMAND
    cursor.execute(create_bronze_crm_customer_info_query)
    cursor.execute(create_bronze_crm_prd_info_query)
    cursor.execute(create_bronze_crm_sale_details_query)
    cursor.execute(create_bronze_erp_cust_az12)
    cursor.execute(create_bronze_erp_loc_a101)
    cursor.execute(create_bronze_erp_px_cat_g1v2)
    print('Creation of Bronze Tables Completed')
    return None
