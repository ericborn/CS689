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

USE Opioids_DW;

-- Code to setup the distributor_dim table within the warehouse
--DROP SEQUENCE distributor_dim_key
CREATE SEQUENCE distributor_key
START WITH 1000
INCREMENT BY 1;

SELECT NEXT VALUE FOR distributor_key AS distributor_key, 
ar.REPORTER_NAME AS 'distributor_name', ar.REPORTER_ADDRESS1 AS 'distributor_address', 
ar.REPORTER_CITY AS 'distributor_city', ar.REPORTER_STATE AS 'distributor_state', ar.REPORTER_ZIP AS 'distributor_zip', ar.REPORTER_COUNTY AS 'distributor_county'
INTO distributor_dim
FROM (SELECT DISTINCT REPORTER_NAME, REPORTER_ADDRESS1, REPORTER_CITY, REPORTER_STATE, REPORTER_ZIP, REPORTER_COUNTY
	  FROM Opioids.dbo.arcos) ar;
  
-- Code to setup the buyer_dim table within the warehouse
--DROP SEQUENCE buyer_dim_key
CREATE SEQUENCE buyer_key
START WITH 10
INCREMENT BY 1;

SELECT NEXT VALUE FOR buyer_key AS buyer_key, 
ar.BUYER_NAME AS 'buyer_name', ar.BUYER_ADDRESS1 AS 'buyer_address', 
ar.BUYER_CITY AS 'buyer_city', ar.BUYER_STATE AS 'buyer_state', ar.BUYER_ZIP AS 'buyer_zip', ar.BUYER_COUNTY AS 'buyer_county'
INTO buyer_dim
FROM (SELECT DISTINCT BUYER_NAME, BUYER_ADDRESS1, BUYER_CITY, BUYER_STATE, BUYER_ZIP, BUYER_COUNTY
	  FROM Opioids.dbo.arcos) ar;
	  
-- Code to setup the drug_dim table within the warehouse
--DROP SEQUENCE drug_dim_key
CREATE SEQUENCE drug_key
START WITH 1
INCREMENT BY 1;

SELECT NEXT VALUE FOR drug_key AS drug_key, ar.DRUG_NAME AS 'drug_name'
INTO drug_dim
FROM (SELECT DISTINCT DRUG_NAME
	  FROM Opioids.dbo.arcos) ar;

-- Code to setup the relabeler_dim table within the warehouse
--DROP SEQUENCE relabeler_dim_key
CREATE SEQUENCE relabeler_key
START WITH 1
INCREMENT BY 1;

SELECT NEXT VALUE FOR relabeler_key AS relabeler_key, 
ar.combined_labeler_name AS 'relabeler_name'
INTO relabeler_dim
FROM (SELECT DISTINCT combined_labeler_name
	  FROM Opioids.dbo.arcos) ar;

-- Created indexes to help with reading data from the arcos table
-- reporter name and address
USE Opioids
CREATE NONCLUSTERED INDEX indx_1 ON dbo.arcos
([reporter_name], [reporter_address1]) include ([buyer_name], [buyer_address1], [drug_name],[quantity], [transaction_Date], [Combined_Labeler_Name]);

-- transaction date
USE Opioids
CREATE NONCLUSTERED INDEX indx_Date ON dbo.arcos
(transaction_date) include ([buyer_name], [buyer_address1], [drug_name],[quantity], [reporter_name], [reporter_address1], [Combined_Labeler_Name]);

-- combined labeler name
USE Opioids
CREATE NONCLUSTERED INDEX indx_labeler ON dbo.arcos
([Combined_Labeler_Name]) include ([buyer_name], [buyer_address1], [drug_name],[quantity], [transaction_Date], [reporter_name], [reporter_address1]);

-- function found on stack overflow for identifying non-numeric values when trying to convert between varchar and float
-- https://stackoverflow.com/questions/8085015/error-converting-varchar-to-float

CREATE FUNCTION dbo.isReallyNumeric  
(  
    @num VARCHAR(64)  
)  
RETURNS BIT  
BEGIN  
    IF LEFT(@num, 1) = '-'  
        SET @num = SUBSTRING(@num, 2, LEN(@num))  

    DECLARE @pos TINYINT  

    SET @pos = 1 + LEN(@num) - CHARINDEX('.', REVERSE(@num))  

    RETURN CASE  
    WHEN PATINDEX('%[^0-9.-]%', @num) = 0  
        AND @num NOT IN ('.', '-', '+', '^') 
        AND LEN(@num)>0  
        AND @num NOT LIKE '%-%' 
        AND  
        (  
            ((@pos = LEN(@num)+1)  
            OR @pos = CHARINDEX('.', @num))  
        )  
    THEN  
        1  
    ELSE  
    0  
    END  
END  
GO;

-- Using the above function, I'm performing an update to set all records that have a non-numeric value
-- in the dosage_unit column to 999
UPDATE Opioids.dbo.arcos
SET dosage_unit = 999
WHERE dbo.isReallyNumeric(dosage_unit) = 0;

-- Same for the quantity and CALC_BASE_WT_IN_GM columns
UPDATE Opioids.dbo.arcos
SET quantity = 999
WHERE dbo.isReallyNumeric(quantity) = 0;

UPDATE Opioids.dbo.arcos
SET CALC_BASE_WT_IN_GM = 999
WHERE dbo.isReallyNumeric(CALC_BASE_WT_IN_GM) = 0;

--DROP TABLE orders
-- Gathers the initial data from the Opioids database and insert it into a table called transactions_fact in the Opioids_DW database
-- does a convert on the time_period.date_key from INT to DATE
-- Also converts using RIGHT AND LEFT to transform the arcos data format from MMDDYYYY to YYYYMMDD
USE Opioids_DW;

-- Full table build for all states
--SELECT t.Month, t.Year, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key, COUNT(buyer_key) AS 'Total_transactions_fact',
--ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'Average_Quantity', SUM(CAST(ar.quantity AS FLOAT)) AS 'Total_Quantity',
--ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'Average_Doses', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'Total_Doses',
--ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'Average_Grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'Total_Grams'
--INTO Opioids_DW.dbo.transactions_fact
--FROM Opioids.dbo.arcos ar
--INNER JOIN Opioids_DW.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,(RIGHT(transaction_date,4)+LEFT(transaction_date,4)),112)
--INNER JOIN Opioids_DW.dbo.distributor_dim di ON di.distributor_name = ar.REPORTER_NAME AND di.distributor_address = ar.REPORTER_ADDRESS1
--INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_name = ar.buyer_NAME AND b.buyer_address = ar.buyer_ADDRESS1
--INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
--INNER JOIN Opioids_DW.dbo.relabeler_dim r ON r.relabeler_name = ar.Combined_Labeler_Name
--GROUP BY t.Month, t.Year, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key;

-- Table build with only records with buying state = MA
SELECT t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key, COUNT(buyer_key) AS 'Total_transactions_fact',
ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'Average_Quantity', SUM(CAST(ar.quantity AS FLOAT)) AS 'Total_Quantity',
ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'Average_Doses', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'Total_Doses',
ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'Average_Grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'Total_Grams'
INTO Opioids_DW.dbo.transactions_ma_fact
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period t ON t.year = RIGHT(ar.transaction_Date,4) AND t.month = left(ar.transaction_Date,2) 
INNER JOIN Opioids_DW.dbo.distributor_dim di ON di.distributor_name = ar.REPORTER_NAME AND di.distributor_address = ar.REPORTER_ADDRESS1
INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_name = ar.buyer_NAME AND b.buyer_address = ar.buyer_ADDRESS1
INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
INNER JOIN Opioids_DW.dbo.relabeler_dim r ON r.relabeler_name = ar.Combined_Labeler_Name
WHERE b.buyer_state = 'MA'
GROUP BY t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key;

SELECT TOP 10 *
FROM buyer_dim

SELECT TOP 10 *
FROM distributor_dim

SELECT TOP 10 *
FROM drug_dim

SELECT TOP 10 *
FROM relabeler_dim

SELECT *
FROM time_period

SELECT TOP 10 *
FROM transactions_ma_fact

-----------------------------
-- null value for a county with the zipcode of 02401
UPDATE buyer_dim
SET buyer_county = 'Plymouth'
WHERE buyer_zip = 02401