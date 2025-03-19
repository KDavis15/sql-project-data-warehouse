/*
==========================================================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
==========================================================================================================
Script Purpose:
  This stored procedure performs ETL (Extract, Transform, and load) process to populate the 'silver'
   schema tables from the 'bronze' schema.

  Actions performed:
  - Truncates the silver tables before loading data.
  - INSERT transformed and cleansed data from the 'bronze' schema into the 'silver'.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC silver.load_silver;
==========================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Silver Layer';
		PRINT '========================================';

		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cust_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
     
		SELECT
		cust_id,
		cst_key,
		TRIM(cst_firstname)AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown'
		END cst_gndr,
		cst_create_date
   
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		) t WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		[sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,[sls_order_dt]
			  ,[sls_ship_dt]
			  ,[sls_due_dt]
			  ,[sls_sales]
			  ,[sls_quantity]
			  ,[sls_price]
			  )
		SELECT  sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			  ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			  END AS sls_order_dt,
			  CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			  ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
			  END AS sls_ship_dt,      
    			 CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			  ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
			  END AS sls_due_dt,
			   CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END AS sls_sales, --Recalculate sales if original value is missing or incorrect
			  sls_quantity,
			   CASE WHEN sls_price IS NULL OR sls_price <=0 
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price --Dervive price if original value is invalid
				END AS sls_price
		  FROM bronze.crm_sales_details
		  SET @end_time = GETDATE();
		  PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		  PRINT '-----------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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

		SELECT prd_id,
  			  REPLACE(SUBSTRING(prd_key,1,5), '-','_') cat_id,
			  SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			  prd_nm,
			  ISNULL(prd_cost,0) AS prd_cost,
     
			 CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'other Sales'
			 WHEN  'T' THEN 'Touring'
			 ELSE 'Unknown'
			 END AS  prd_line,
			 CAST(prd_start_dt AS DATE) AS prd_start_dt,
			 CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM [bronze].[crm_prd_info]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------------------------------';

		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12 ';
		INSERT INTO silver.erp_cust_az12 (
		CID,
		BDATE,
		GEN
		)

		SELECT 
				CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING( cid, 4, LEN(cid))
				ELSE CID
				END cid,
   				CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
				END BDATE, -- Set future birthdates to Null
				CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
					 ELSE 'Unknown'
					 END GEN -- Normalized gender values and handle unknown cases

		FROM [DataWarehouseProject].[bronze].[erp_cust_az12]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101 ';
		INSERT INTO silver.erp_loc_a101 ( 
		CID,
		CNTRY
		)
		SELECT REPLACE(CID,'-','') cid,
   			  CASE WHEN TRIM(CNTRY) IN ('DE') THEN 'Germany'
					 WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
					 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'Unknown'
					 ELSE TRIM(CNTRY)
					 END cntry -- Normalize and Handle missing or blank country codes

		FROM DataWarehouseProject.bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2 ';
		INSERT INTO silver.erp_px_cat_g1v2 (
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		SELECT  [ID]
			  ,[CAT]
			  ,[SUBCAT]
			  ,[MAINTENANCE]
		  FROM [DataWarehouseProject].[bronze].[erp_px_cat_g1v2]
		  SET @batch_end_time = GETDATE();
		  PRINT '>> Load Duration ' + CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + ' seconds'
		  PRINT '-----------------------------------------------------------------------------------------';
		
		  SET @batch_end_time = GETDATE()
		  PRINT '=========================================';
		  PRINT 'Loading of the Silver Layer is Completed';
		  PRINT 'Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time)AS NVARCHAR) + ' seconds';		
		  PRINT '=========================================';

	END TRY
		BEGIN CATCH
			PRINT '=========================================';
			PRINT 'Error occurred during SILVER LAYER ';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER()AS VARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE()AS VARCHAR);
			PRINT '=========================================';
		END CATCH
END;
