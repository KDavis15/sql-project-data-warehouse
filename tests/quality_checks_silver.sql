/*
=========================================================================================
Quality Checks
=========================================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
- Nulls or duplicate primary keys.
-Unwanted spaces in string fields
- Invalid date ranges and orders
-Data consistency between related fields

These are not all the tests that could be performed, but some of the tests used
*/


--========================================================================================
  --Checking 'silver.crm_sales_details'
   --========================================================================================


  /********************************************************************************************/
--Check for unwanted spaces from string fields
-- Expectation is no results
SELECT [sls_ord_num]
FROM silver.crm_sales_details
Where [sls_ord_num] != TRIM([sls_ord_num])  

/********************************************************************************************/
----Check for Invalid date orders
-- Expectation is no results

SELECT*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt >sls_due_dt


/********************************************************************************************/
----Business Rules
-- Sales = Quantity * Price
--No negatives, Zeros, Nulls 

SELECT  DISTINCT
	   sls_sales,
      sls_quantity,
     sls_price
  FROM silver.crm_sales_details
  WHERE sls_sales != sls_quantity * sls_price
  OR sls_sales <=0
  OR sls_quantity <=0
  OR sls_price <=0
    OR sls_sales IS NULL
  OR sls_quantity IS NULL
  OR sls_price IS NULL
  ORDER BY   sls_sales, sls_quantity, sls_price

  SELECT * FROM silver.crm_sales_details

  --========================================================================================
  --Checking 'silver.erp_cust_az12'
   --========================================================================================

  /********************************************************************************************/
----Check for Invalid date orders
-- Expectation is no results

SELECT DISTINCT BDATE
FROM silver.erp_cust_az12
WHERE BDATE < '1924-01-01' or BDATE >GETDATE()

  /********************************************************************************************/

-- Check For Nulls or Duplicate Primary Keys
-- Expectation: No Results
SELECT DISTINCT GEN
FROM silver.erp_cust_az12

SELECt * FROM silver.erp_cust_az12

 --========================================================================================
  --Checking 'silver.crm_sales_details'
   --========================================================================================

  /********************************************************************************************/
----Check for unwanted Spaces
-- Expectation is no results

SELECT  *
  FROM [DataWarehouseProject].[silver].[erp_px_cat_g1v2]
  WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) or MAINTENANCE != TRIM(MAINTENANCE)
