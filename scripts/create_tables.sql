if object_id('bronze.crm_cust_info', 'u') is not null
	drop table bronze.crm_cust_info;
Create table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

go


if object_id('bronze.crm_prd_info', 'u') is not null
	drop table bronze.crm_prd_info;
Create table bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(5),
	prd_start_dt DATE,
	prd_end_dt DATE
);

go

if object_id('bronze.crm_sales_details', 'u') is not null
	drop table bronze.crm_sales_details;
Create table bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

go

if object_id('bronze.erp_px_cat_g1v2', 'u') is not null
	drop table bronze.erp_px_cat_g1v2;
Create table bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

go

if object_id('bronze.erp_cust_az12', 'u') is not null
	drop table bronze.erp_cust_az12;
Create table bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

go

if object_id('bronze.erp_loc_a101', 'u') is not null
	drop table bronze.erp_loc_a101;
Create table bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

go



if object_id('silver.crm_cust_info', 'u') is not null
	drop table silver.crm_cust_info;
Create table silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT GETDATE() -- The keyword 'Default' helps us to insert any data per default, so we dont need to pass value for that column the value will passed automatic
);

go


if object_id('silver.crm_prd_info', 'u') is not null
	drop table silver.crm_prd_info;
Create table silver.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(5),
	prd_start_dt DATE,
	prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT GETDATE()
);

go

if object_id('silver.crm_sales_details', 'u') is not null
	drop table silver.crm_sales_details;
Create table silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
    dwh_create_date DATETIME DEFAULT GETDATE()
);

go

if object_id('silver.erp_px_cat_g1v2', 'u') is not null
	drop table silver.erp_px_cat_g1v2;
Create table silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
    dwh_create_date DATETIME DEFAULT GETDATE()
);

go

if object_id('silver.erp_cust_az12', 'u') is not null
	drop table silver.erp_cust_az12;
Create table silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
    dwh_create_date DATETIME DEFAULT GETDATE()
);

go

if object_id('silver.erp_loc_a101', 'u') is not null
	drop table silver.erp_loc_a101;
Create table silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
    dwh_create_date DATETIME DEFAULT GETDATE()
);

go
