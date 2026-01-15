CREATE OR REPLACE TABLE dim_customers AS
        SELECT 
            ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_sk,
            c.cst_id as customer_id,
            c.cst_key as customer_number,
            c.cst_firstname || ' ' || c.cst_lastname AS full_name,
            l.cntry AS country,
            c.cst_material_status AS material_status,
            CASE
                WHEN c.cst_gndr = 'n/a' THEN e.gen
                ELSE COALESCE(c.cst_gndr,'n/a')
                END AS gender,
            e.bdate AS birthdate,
            e.dwh_inserted_at
        FROM temp_crm_cust_info c
        LEFT JOIN temp_erp_cust_az12 e 
                ON c.cst_key = e.cid 
        LEFT JOIN temp_erp_loc_a101 l 
                ON c.cst_key = l.cid;


CREATE OR REPLACE TABLE dim_products AS
        SELECT 
            ROW_NUMBER() OVER(ORDER BY prd_id) AS product_sk,
            p.prd_id AS product_id,
            p.prd_key AS product_number,
            p.prd_nm AS product_name,
            p.prd_cat AS category_id,
            cat.cat AS category_name,
            cat.subcat AS subcategory,
            cat.maintenance,
            p.prd_cost AS cost,
            p.prd_line AS product_line,
            p.prd_start_dt AS product_start_date

        FROM temp_crm_prd_info p
        LEFT JOIN temp_erp_px_cat_g1v2 cat ON p.prd_key = cat.id
        WHERE prd_end_dt IS NULL;


CREATE OR REPLACE TABLE fact_sales AS
        SELECT 
            dc.customer_sk,
            dp.product_sk,
            sd.sls_ord_num AS order_number,
            sd.sls_order_dt AS order_date,
            sd.sls_due_dt AS due_date,
            sd.sls_ship_dt AS shipping_date,
            sd.sls_quantity AS quantity,
            sd.sls_price AS price,
            sd.sls_sales AS sales
        FROM temp_crm_sale_details sd
        LEFT JOIN dim_customers dc
            ON sd.sls_cust_id = dc.customer_id
        LEFT JOIN dim_products dp
            ON sd.sls_prd_key = dp.product_number
