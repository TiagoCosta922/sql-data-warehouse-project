CREATE OR ALTER PROCEDURE gold.inserting_gold_tables AS
    BEGIN
    DECLARE @start_date DATE, @end_date DATE, @batch_start_date DATE, @batch_end_date DATE;
    BEGIN TRY
    PRINT '<< Start of ETL Process >>';
    PRINT '==================================================================================';
    SET @batch_start_date = GETDATE();

    --dim_customer
    PRINT '<< Truncate and Load Dim Customer Table >>';

    Truncate table gold.dim_customer_table;

    PRINT '<< Inserting Data into Dim Customer Table >>';

    SET @start_date = GETDATE();

    INSERT into gold.dim_customer_table (customer_key,customer_key_scd2, customer_id,customer_number, first_name, last_name, country, gender, marital_status, birth_date, create_date) 
    select
    row_number() over (order by ci.cst_id) as customer_key, 
    dense_rank() over (partition by ci.cst_id order by ci.cst_create_date desc) as customer_key_scd2,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    case 
        when ci.cst_gndr != UPPER('unknown') then ci.cst_gndr
        else upper(coalesce(ca.gen,'unknown'))
    end as gender,
    ci.cst_material_status as marital_status,
    ca.bdate as birth_date,
    ci.cst_create_date as create_date
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la on ci.cst_key = la.cid;

    SET @end_date = GETDATE();
    PRINT '<< Data Inserted into Dim Customer Table >>';
    PRINT 'The Time of inserting data into Dim Customer Table is: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS VARCHAR) + ' Seconds';
    PRINT '=================================================================================='; 


    -------------------------------------------------------------------

    --dim_product
    PRINT '<< Truncate and Load Dim Product Table >>';

    Truncate table gold.dim_product_table;

    PRINT '<< Inserting Data into Dim Product Table >>';

    SET @start_date = GETDATE();

    INSERT INTO gold.dim_product_table (product_key, product_id, product_number, product_name, category_id, category, subcategory, cost, maintenance, product_line, start_date)
    SELECT
    row_number() over (order by pn.prd_id) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pa.cat as category,
    pa.subcat as subcategory,
    pn.prd_cost as cost,
    pa.maintenance as maintenance,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
    from silver.crm_prd_info pn
    left join silver.erp_px_cat_g1v2 pa on pn.cat_id = pa.id

    SET @end_date = GETDATE();
    PRINT '<< Data Inserted into Dim Product Table >>';
    PRINT 'The Time of inserting data into Dim Product Table is: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS VARCHAR) + ' Seconds';
    PRINT '=================================================================================='; 


    -------------------------------------------------------------------

    --fact_sales
    PRINT '<< Truncate and Load Fact Sales Table >>';

    Truncate table gold.fact_sales_table;

    PRINT '<< Inserting Data into Fact Sales Table >>';

    SET @start_date = GETDATE();

    TRUNCATE TABLE gold.fact_sales_table;
    INSERT INTO gold.fact_sales_table (order_number, product_key, customer_key, customer_id, order_date, ship_date, due_date, sales, quantity, price)
    SELECT
    s.sls_ord_num as order_number,
    prd.product_key as product_key,
    cust.customer_key as customer_key,
    s.sls_cust_id as customer_id,
    s.sls_ord_dt as order_date,
    s.sls_ship_dt as ship_date,
    s.sls_due_dt as due_date,
    s.sls_sales as sales,
    s.sls_quantity as quantity,
    s.sls_price as price
    from silver.crm_sales_details as s
    left join gold.dim_product as prd on s.sls_prd_key = prd.product_number
    left join gold.dim_customer as cust on s.sls_cust_id = cust.customer_id

    SET @end_date = GETDATE();
    PRINT '<< Data Inserted into Fact Sales Table >>';
    PRINT 'The Time of inserting data into Fact Sales Table is: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS VARCHAR) + ' Seconds';
    PRINT '=================================================================================='; 

    SET @batch_end_date = GETDATE();
    PRINT '<< End of ETL Process >>';
    PRINT 'The Time of ETL Process is: ' + CAST(DATEDIFF(SECOND, @batch_start_date, @batch_end_date) AS VARCHAR) + ' Seconds';
  END TRY
  BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
  END CATCH
END;

EXEC gold.inserting_gold_tables
