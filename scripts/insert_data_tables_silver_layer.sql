/*
Purpose:
The load_silver stored procedure is designed to load data from the bronze schema into the silver schema in SQL Server. This procedure performs a data transformation and insertion process for various tables from the bronze schema into corresponding tables in the silver schema. It truncates the target tables before loading fresh data to ensure there are no duplicates and that the silver schema is up to date with the latest data from the bronze schema.

Process Overview:
1. Truncate the Table: For each target table in the silver schema, the procedure first truncates the table. This removes all existing records, ensuring that the next data insertion does not result in duplicate rows.
2. Data Transformation and Insertion: After truncating the table, the procedure inserts fresh data into the silver table. It also applies any necessary transformations or calculations, such as replacing null values, trimming spaces, and handling data inconsistencies.
3. Logging: The procedure logs the duration of each load operation for each table and provides information on the total execution time for the entire batch.
4. Error Handling: The procedure includes error handling to capture and display any errors that may occur during the execution process.

Tables Involved:
1. crm_cust_info: Customer information with transformations such as gender and marital status normalization.
2. crm_prd_info: Product information with calculated values for the product cost and categorization.
3. crm_sales_details: Sales data with validation and price recalculations.
4. erp_cust_az12: ERP customer data with gender and birthdate normalization.
5. erp_loc_a101: ERP location data with country normalization.
6. erp_px_cat_g1v2: ERP PX Category data.

Detailed Procedure Steps:
1. Truncate and Load crm_cust_info:
   - Truncates the silver.crm_cust_info table.
   - Loads and transforms customer data from bronze.crm_cust_info, handling string trimming, gender, and marital status normalization.
   - Logs the time taken for the operation.

2. Truncate and Load crm_prd_info:
   - Truncates the silver.crm_prd_info table.
   - Loads product data from bronze.crm_prd_info and applies transformations, including product category extraction, defaulting null product costs to the calculated average, and determining the product end date.
   - Logs the time taken for the operation.

3. Truncate and Load crm_sales_details:
   - Truncates the silver.crm_sales_details table.
   - Loads sales data from bronze.crm_sales_details and performs data validation, ensuring sales values match quantity and price, and handling invalid or missing dates.
   - Logs the time taken for the operation.

4. Truncate and Load erp_cust_az12:
   - Truncates the silver.erp_cust_az12 table.
   - Loads and transforms customer data from bronze.erp_cust_az12, normalizing the gender and birthdate data.
   - Logs the time taken for the operation.

5. Truncate and Load erp_loc_a101:
   - Truncates the silver.erp_loc_a101 table.
   - Loads and transforms location data from bronze.erp_loc_a101, normalizing country names.
   - Logs the time taken for the operation.

6. Truncate and Load erp_px_cat_g1v2:
   - Truncates the silver.erp_px_cat_g1v2 table.
   - Loads ERP PX Category data from bronze.erp_px_cat_g1v2 into the table.
   - Logs the time taken for the operation.

Final Summary:
After all tables have been loaded and transformed, the procedure calculates and logs the total duration of the entire batch process, providing an overview of the performance.

Key Concepts:
- Truncate: The TRUNCATE TABLE command is used to delete all rows from a table. This ensures that only the latest data is inserted, avoiding any duplication of records in the silver schema.
- Data Transformation: The procedure includes various data transformations to clean and normalize the data before inserting it into the silver schema (e.g., trimming spaces, handling null values, and applying calculated averages).
- Logging: Each step of the procedure logs the time taken for the operation, providing transparency and helping track performance.
- Error Handling: The procedure is wrapped in a TRY-CATCH block, which captures and prints error messages in case of failures during the execution process.

Error Handling:
In case of any errors during the execution, the procedure will catch them and display an error message with the details of what went wrong. This is done in the BEGIN CATCH block to ensure smooth troubleshooting and error tracking.
*/




CREATE or Alter PROCEDURE [silver].[load_silver] AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
      SET @batch_start_time = GETDATE();
      PRINT 'START LOAD SILVER';

  --crm_cust_info
    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting data into silver.crm_cust_info table';
    INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)

    SELECT cst_id,
        cst_key,
        TRIM(cst_firstname), --remove the blank spaces from the beginning and end of the string
        TRIM(cst_lastname),
        CASE WHEN UPPER(TRIM(cst_material_status)) ='S' THEN 'SINGLE'
              WHEN UPPER(TRIM(cst_material_status)) ='M' THEN 'MARRIED'
              ELSE 'UNKNOWN'
        END AS cst_material_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
              WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
              ELSE 'UNKNOWN'
        END AS cst_gndr,
        CAST(cst_create_date as DATE)
    FROM (
        SELECT *,
              ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) AS subquery
    WHERE flag_last = 1 and cst_id is not null;
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    -------------------------------------------------------------------------------------------------------------------------------------------

    --CRM_PRD_INFO


    -- Declare the variable to store the average value of prd_cost
    DECLARE @avg_prd_cost DECIMAL(18, 2); 

    -- Calculate the average value of prd_cost
    SELECT @avg_prd_cost = AVG(prd_cost)
    FROM [DataWarehouse].[bronze].[crm_prd_info]
    WHERE prd_cost IS NOT NULL;

    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting data into silver.crm_prd_info table';
    -- Insert the calculated values into the silver.prd_info table
    INSERT INTO silver.crm_prd_info (
        prd_id, 
        cat_id, 
        prd_key, 
        prd_nm, 
        prd_cost, 
        prd_line, 
        prd_start_dt, 
        prd_end_dt
    )
    SELECT 
        [prd_id],
        REPLACE(SUBSTRING([prd_key], 1, 5), '-', '_') AS cat_id,
        SUBSTRING([prd_key], 7, LEN(prd_key)) AS prd_key,
        [prd_nm],
        COALESCE([prd_cost], @avg_prd_cost) AS prd_cost, -- using the variable to replace the NULL values
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
            ELSE 'UNKNOWN'
        END AS prd_line,
        [prd_start_dt],
        DATEADD(DAY, -1, LEAD([prd_start_dt]) OVER (PARTITION BY [prd_key] ORDER BY [prd_start_dt])) AS prd_end_dt
    FROM 
        [DataWarehouse].[bronze].[crm_prd_info];
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    ---------------------------------------------------------------------------------------------------------------------------------------------------------

    --CRM_SALES_DETAILS
    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting data into silver.crm_sales_details table';

    INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_ord_dt, sls_ship_dt, sls_due_dt, sls_sales,sls_quantity, sls_price)

    SELECT [sls_ord_num],
          [sls_prd_key],
          [sls_cust_id],
          CASE 
            WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
          END AS sls_ord_dt,
        CASE 
            WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
          END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
          END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,    
          [sls_quantity],
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price
      FROM [DataWarehouse].[bronze].[crm_sales_details]
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------

    --ERP_CUST_AZ12
    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting data into silver.erp_cust_az12 table';

    Insert into silver.erp_cust_az12(cid,bdate,gen)


    SELECT 
        case
            when cid like 'NAS%' then substring([cid],4,len([cid])) 
            else [cid] 
        end as cid,
        case 
            when [bdate] > GETDATE() then null 
            else [bdate]
        end as bdate,
        case 
            when trim(Upper([gen])) in ('M','MALE') then 'Male'
            when trim(UPPER(gen)) in ('F','FEMALE') then 'Female' 
            else 'Unknown'
        end as gen
      FROM [DataWarehouse].[bronze].[erp_cust_az12]
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------

    --ERP_LOC_A101

    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting data into silver.erp_loc_a101 table';
    INSERT into silver.erp_loc_a101 (cid, cntry)

    SELECT replace([cid],'-','') as cid,
        case 
            when trim(upper([cntry])) = 'DE' then 'Germany'
            when trim(upper([cntry])) in ('US', 'USA') then 'United States'
            when trim(cntry) = '' or cntry is null then 'Unknown'
            else cntry
        end as cntry
      FROM [DataWarehouse].[bronze].[erp_loc_a101]
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --ERP_PRD_G1V2

    SET @start_time = GETDATE();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting data into silver.erp_px_cat_g1v2 table';

    INSERT INTO  silver.erp_px_cat_g1v2 (id, cat, subcat,maintenance)
    SELECT [id]
          ,[cat]
          ,[subcat]
          ,[maintenance]
      FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
    SET @end_time = GETDATE();
    PRINT '>> Time taken to load silver.crm_cust_info table: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '============================================================================================================================';
    PRINT '============================================================================================================================';
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    SET @batch_end_time = GETDATE();
    PRINT 'END LOAD SILVER';
    PRINT 'Total time taken to load silver tables: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
  END TRY
  BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
  END CATCH
END;

EXEC [silver].[load_silver];
