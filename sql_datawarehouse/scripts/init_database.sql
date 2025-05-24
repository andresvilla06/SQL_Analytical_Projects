/*	DATAWAREHOUSE Proyect 1
  script purpose: This script is for create a new database but before checking it firts if exist. Then create the
  schemas for the database.

	--Bronze Layer--

*/
-------------------------------------------------------------------------------------
--Checking first if the database exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
  BEGIN
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWareHouse;
END;
GO
  
--1. (Creating Database)
CREATE DATABASE DataWareHouse;

--2. (Creating Schemas for the new database)
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
