/*
Purpose:
The load_bronze stored procedure is designed to load data from CSV files into SQL Server tables in the bronze schema. This procedure performs a bulk insert into each table, overwriting any existing data. It achieves this by first truncating the table (removing all existing records) and then loading fresh data from the corresponding CSV file. This ensures that each run of the procedure will insert the latest data without duplicates.

Process Overview:
Truncate the Table: The procedure first truncates each target table in the bronze schema. This removes all existing records, ensuring that the next data insertion does not result in duplicate rows.
Bulk Insert: After truncating the table, the procedure uses BULK INSERT to load data from the respective CSV file into the table.
Logging: The procedure logs the duration of each load operation and provides information on the total execution time for the entire batch.
Error Handling: The procedure includes error handling to capture and display any errors that may occur during the execution.
Tables Involved:
crm_cust_info: Customer information.
crm_prd_info: Product information.
crm_sales_details: Sales details.
erp_px_cat_g1v2: ERP PX Category data.
erp_cust_az12: ERP customer data.
erp_loc_a101: ERP location data.
CSV File Paths:
Each table corresponds to a specific CSV file:

crm_cust_info: cust_info.csv
crm_prd_info: prd_info.csv
crm_sales_details: sales_details.csv
erp_px_cat_g1v2: px_cat_g1v2.csv
erp_cust_az12: cust_az12.csv
erp_loc_a101: LOC_A101.csv
These files are located in the following directory on the local machine:
C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\

Detailed Procedure Steps:
Truncate and Load crm_cust_info:

Truncates the bronze.crm_cust_info table.
Loads data from cust_info.csv into the table.
Logs the time taken for the operation.
Truncate and Load crm_prd_info:

Truncates the bronze.crm_prd_info table.
Loads data from prd_info.csv into the table.
Logs the time taken for the operation.
Truncate and Load crm_sales_details:

Truncates the bronze.crm_sales_details table.
Loads data from sales_details.csv into the table.
Logs the time taken for the operation.
Truncate and Load erp_px_cat_g1v2:

Truncates the bronze.erp_px_cat_g1v2 table.
Loads data from px_cat_g1v2.csv into the table.
Logs the time taken for the operation.
Truncate and Load erp_cust_az12:

Truncates the bronze.erp_cust_az12 table.
Loads data from cust_az12.csv into the table.
Logs the time taken for the operation.
Truncate and Load erp_loc_a101:

Truncates the bronze.erp_loc_a101 table.
Loads data from LOC_A101.csv into the table.
Logs the time taken for the operation.
Final Summary:

After all tables have been loaded, the procedure calculates and logs the total duration of the entire batch process.
Key Concepts:
Truncate: The TRUNCATE TABLE command is used to delete all rows from a table. This is necessary to prevent duplicate data during the bulk insert operation. When the procedure is run multiple times, truncating ensures that only the latest data is inserted without duplication.

Bulk Insert: The BULK INSERT command is used to efficiently load large datasets from a CSV file into a SQL Server table. It directly inserts data from a file into the table.

Stored Procedure: The procedure is created as a stored procedure because this data-loading task needs to be performed multiple times, typically for periodic data updates. The stored procedure encapsulates the logic in a reusable and efficient manner.

Error Handling:
In case of any errors during the execution, the procedure will catch them and display an error message with the details of what went wrong. This is done in the BEGIN CATCH block.

    
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		-- Truncate and bulk insert into crm_cust_info
		PRINT 'Load the table crm_cust_info';
		PRINT '=============================';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';

		-- Truncate and bulk insert into crm_prd_info
		PRINT '-------------------------------';
		PRINT 'Load the table crm_prd_info';
		PRINT '=============================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';

		-- Truncate and bulk insert into crm_sales_details
		PRINT '-------------------------------';
		PRINT 'Load the table crm_sales_details';
		PRINT '=============================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';


		-- Truncate and bulk insert into erp_px_cat_g1v2
		PRINT '-------------------------------';
		PRINT 'Load the table erp_px_cat_g1v2';
		PRINT '=============================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';

		-- Truncate and bulk insert into erp_cust_az12
		PRINT '-------------------------------';
		PRINT 'Load the table erp_cust_az12';
		PRINT '=============================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';


		-- Truncate and bulk insert into erp_loc_a101
		PRINT '-------------------------------';
		PRINT 'Load the table erp_loc_a101';
		PRINT '=============================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\TiagoCosta\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row because it is the header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		
		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'The total duration of batch is: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR Message:' + ERROR_MESSAGE();
		PRINT '=========================================';
	END CATCH
	
END


-- Exec bronze.load_bronze
