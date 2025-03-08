/*
==========================================================================================================
Create Database and Schemas
==========================================================================================================

Script Purpose: 
  This script creates a new Database named 'DataWarehouseProject' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up the three schemas
  within the database: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this script will drop the entire ' DataWarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution
  and ensure you have proper backupsa before running this script.
*/

USE master;
GO

-- Drop and recreate the ' DataWarehouse' database

IF EXISTS ( SELECT 1 FROM sys.databases WHERE  name = 'DataWarehouseProject')
BEGIN
    ALTER DATABASE DataWarehouseProject SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseProject;
END;
GO

-- Create Database 'DataWarehouse'

Create DATABASE DataWarehouseProject;
GO

USE DataWarehouseProject;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
