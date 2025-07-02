/*
This script is used to initialize a fresh Data Warehouse environment following the medallion architecture (Bronze, Silver, Gold layers). It:

Drops the existing DataWarehouse database, if it exists.

Creates a new clean DataWarehouse database.

Defines three schemas within the database:

bronze – Raw data layer (ingested as-is).

silver – Cleaned and transformed data.

gold – Aggregated and business-ready data.

For Memorization:
| Statement                        | Meaning                                    |
| -------------------------------- | ------------------------------------------ |
| `IF EXISTS (...)`                | Check if database exists                   |
| `ALTER DATABASE ... SINGLE_USER` | Kick everyone out so we can drop it safely |
| `WITH ROLLBACK IMMEDIATE`        | Forcefully rollback and disconnect users   |
| `DROP DATABASE`                  | Remove the database entirely               |

========
Warning:
========
-- ⚠️ WARNING: This script will permanently DELETE the 'DataWarehouse' database if it exists.
-- ❌ All data, tables, schemas, and objects inside the database will be lost.
-- ✅ Make sure you have a backup before running this script.
*/

-- Creating DataBase for DataWarehouse


USE master;

GO

-- Drop and recreate the 'Datawarehouse' database



IF EXISTS (SELECT 1 FROM sys.databases  WHERE name = 'DataWarehouse')
BEGIN 
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- Creating Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
