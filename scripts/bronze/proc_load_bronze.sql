/*
==========================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv files to 'bronze' Tables.

Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
==========================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================';

		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';

		PRINT '>> Tuncating Table bronze.crm_cust_info';

		TRUNCATE TABLE [bronze].[crm_cust_info];

		PRINT '>> Inserting data into bronze.crm_cust_info';
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT COUNT(*) FROM [bronze].[crm_cust_info]
		PRINT '>> Tuncating Table bronze.crm_prd_info';
		TRUNCATE TABLE [bronze].[crm_prd_info];
		PRINT '>> Inserting data into bronze.crm_prd_info';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT * FROM [bronze].[crm_prd_info]
		--SELECT COUNT(*) FROM [bronze].[crm_prd_info]
		--------------------------------------------------------------------------

		PRINT '>> Tuncating Table bronze.sales_details';
		TRUNCATE TABLE [bronze].[crm_sales_details];
		PRINT '>> Inserting data into bronze.sales_details';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT * FROM [bronze].[crm_sales_details]
		--SELECT COUNT(*) FROM [bronze].[crm_sales_details]

		------------------------------------------------------------------------------------
		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';

		PRINT '>> Tuncating Table bronze.erp_cust_az12o';
		TRUNCATE TABLE [bronze].[erp_cust_az12];
		PRINT '>> Inserting data into bronze.erp_cust_az12';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT * FROM [bronze].[erp_cust_az12]
		--SELECT COUNT(*) FROM [bronze].[erp_cust_az12]
		------------------------------------------------------------------------------------------------
		PRINT '>> Tuncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101];
		PRINT '>> Inserting data into bronze.erp_loc_a101';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT * FROM [bronze].[erp_loc_a101]
		--SELECT COUNT(*) FROM [bronze].[erp_loc_a101]
		--------------------------------------------------------------------------------------------------------
		PRINT '>> Tuncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\kstre\Documents\Kia\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);

		--SELECT * FROM [bronze].[erp_px_cat_g1v2]
		--SELECT COUNT(*) FROM [bronze].[erp_px_cat_g1v2]
	END TRY
		BEGIN CATCH
			PRINT '=========================================';
			PRINT 'Error occurred during BRONZE LAYER ';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER()AS VARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE()AS VARCHAR);
			PRINT '=========================================';
		END CATCH
END;
