*/ add the content of csv files to a sql database with bulk, this bulk put every data on the table at once. 

Note that we are creating the tables but before we truncate the table, the truncate remove all the data in the table. It is necessary for
our case because we want to overwrite all the content, so if we on bulk the content if we run the code twice, we will append the same data twice,
thats means we will have duplicate data, so we need remove the all data before insert the data again.

we also create a store procedure because this code will run multiple times, so the best way to do that is create a store procedure
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Truncate and bulk insert into crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- Truncate and bulk insert into crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- Truncate and bulk insert into crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- Truncate and bulk insert into erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- Truncate and bulk insert into erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- Truncate and bulk insert into erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2, -- skip the first row because it is the header
        FIELDTERMINATOR = ',',
        TABLOCK
    );
END;


