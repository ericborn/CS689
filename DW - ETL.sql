-- DW Creation and ETL Code
IF EXISTS 
   (
     SELECT name FROM master.dbo.sysdatabases 
    WHERE name = N'Opioids_DW'
    )
BEGIN
    SELECT 'Database already exists' AS Message
END
ELSE
BEGIN
    CREATE DATABASE [Opioids_DW]
    SELECT 'Olist_DW database has been created'
END;

USE Opioids_DW

-- Code to setup the distributor table within the warehouse
--DROP SEQUENCE distributor_key
CREATE SEQUENCE distributor_key
START WITH 1000 
INCREMENT BY 1;

SELECT NEXT VALUE FOR distributor_key AS distributor_key, 
ar.REPORTER_NAME AS 'distributor_name', ar.REPORTER_ADDRESS1 AS 'distributor_address', 
ar.REPORTER_CITY AS 'distributor_city', ar.REPORTER_STATE AS 'distributor_state', ar.REPORTER_ZIP AS 'distributor_zip', ar.REPORTER_COUNTY AS 'distributor_county'
INTO distributor
FROM (SELECT DISTINCT REPORTER_NAME, REPORTER_ADDRESS1, REPORTER_CITY, REPORTER_STATE, REPORTER_ZIP, REPORTER_COUNTY
	  FROM Opioids.dbo.arcos) ar;