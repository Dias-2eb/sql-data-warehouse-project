/*
=============================================================
Create Database and Schemas (PostgreSQL)
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse'. 
    If the database already exists, it is dropped and recreated. 
    It also sets up three schemas within the database: 'bronze', 
    'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'datawarehouse' 
    database if it exists. All data will be permanently deleted. 
    Proceed with caution and ensure you have backups before running.
=============================================================
*/

-- Terminate active connections to the database before dropping
DO
$$
BEGIN
   PERFORM pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();
END
$$;

-- Drop and recreate the database
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- Connect to the new database (run this separately in your client)
\c datawarehouse;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;










