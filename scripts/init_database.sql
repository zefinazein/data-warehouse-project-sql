/*
===============================
Create Database 'DataWarehouse'
===============================

This Script Contains:
	- Creating 'DataWarehouse' database. If the database with the same name exist, it will drop the previous and
	  recreate a new one.
	  
	  !!!WARNING: YOUR DATABASE WILL BE PERMANENTLY DELETED IF IT HAS THE SAME NAME AS 'DataWarehouse' SO MAKE 
				  SURE YOU HAVE PROPER BACKUP BEFORE RUNNING THIS SCRIPT

	- Make 3 schemas: bronze, silver, gold
*/

USE master;
GO

-- DROP AND RECREATE DataWarehouse database if exist
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- CREATE DWH DATABASE
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- CREATE SCHEMAS
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
