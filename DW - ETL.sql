SELECT *
INTO arcos
FROM arcos_full
WHERE BUYER_STATE = 'ma'

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

--ALTER TABLE distributor_dim
--ADD
--current_row_indicator INT,
--row_effective_date DATE,
--row_expiration_date DATE;

--ALTER TABLE buyer_dim
--ADD
--current_row_indicator INT,
--row_effective_date DATE,
--row_expiration_date DATE;

--UPDATE distributor_dim
--SET 
--current_row_indicator = 1,
--row_effective_date = '20040101',
--row_expiration_date = '20991231'

--UPDATE buyer_dim
--SET 
--current_row_indicator = 1,
--row_effective_date = '20040101',
--row_expiration_date = '20991231'

USE Opioids_DW;

-- Code to setup the distributor_dim table within the warehouse
--DROP SEQUENCE distributor_key
CREATE SEQUENCE distributor_key
START WITH 1000
INCREMENT BY 1;

-- DROP TABLE distributor_dim
SELECT NEXT VALUE FOR distributor_key AS distributor_key, ar.REPORTER_BUS_ACT AS 'distributor_type', ar.REPORTER_NAME AS 'distributor_name',
ar.REPORTER_ADDRESS1 AS 'distributor_address', ar.REPORTER_CITY AS 'distributor_city', ar.REPORTER_STATE AS 'distributor_state', 
ar.REPORTER_ZIP AS 'distributor_zip', ar.REPORTER_COUNTY AS 'distributor_county', '20040101' AS 'effective_date', 'Y' AS 'current_flag'
INTO distributor_dim
FROM (SELECT DISTINCT REPORTER_BUS_ACT, REPORTER_NAME, REPORTER_ADDRESS1, REPORTER_CITY, REPORTER_STATE, REPORTER_ZIP, REPORTER_COUNTY
	  FROM Opioids.dbo.arcos) ar;
  
-- Code to setup the buyer_dim table within the warehouse
--DROP SEQUENCE buyer_key
CREATE SEQUENCE buyer_key
START WITH 10
INCREMENT BY 1;

-- DROP TABLE buyer_dim
SELECT NEXT VALUE FOR buyer_key AS buyer_key, ar.BUYER_BUS_ACT AS 'buyer_type', ar.BUYER_NAME AS 'buyer_name', ar.BUYER_ADDRESS1 AS 'buyer_address', 
ar.BUYER_CITY AS 'buyer_city', ar.BUYER_STATE AS 'buyer_state', ar.BUYER_ZIP AS 'buyer_zip', ar.BUYER_COUNTY AS 'buyer_county'
--,1 AS 'current_row_indicator', '20040101' AS 'row_effective_date', '20991231' AS 'row_expiration_date'
INTO buyer_dim
FROM (SELECT DISTINCT BUYER_BUS_ACT, BUYER_NAME, BUYER_ADDRESS1, BUYER_CITY, BUYER_STATE, BUYER_ZIP, BUYER_COUNTY
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
SET CALC_BASE_WT_IN_GM = 0
WHERE dbo.isReallyNumeric(CALC_BASE_WT_IN_GM) = 0;

-- Insert fake distributor address changes
INSERT INTO distributor_dim
SELECT NEXT VALUE FOR distributor_key AS distributor_key,
dd.distributor_type, dd.distributor_name, dau.distributor_address, 
dau.distributor_city, dau.distributor_state, dau.distributor_zip, '', '20080211', 'Y'
FROM distributor_address dau
JOIN distributor_dim dd ON dd.distributor_key = dau.distributor_key;

-- set old records to not current
UPDATE distributor_dim
SET current_flag = 'N'
FROM distributor_address dau
JOIN distributor_dim dd ON dd.distributor_key = dau.distributor_key
WHERE effective_date = '20040101'

-- update distributor county based on zipcode from uszips table
-- uszips table downloaded from here: https://simplemaps.com/data/us-zips
UPDATE distributor_dim
SET distributor_dim.distributor_county = u.county_name
FROM distributor_dim d
JOIN uszips u ON d.distributor_city = u.city AND d.distributor_state = u.state_name
WHERE d.distributor_county = ''

-- null value for a county with the zipcode of 02401, 02174, 66225, 10131 AND 10260
UPDATE buyer_dim
SET buyer_county = 'Plymouth'
WHERE buyer_zip = 02401

UPDATE buyer_dim
SET buyer_county = 'Middlesex'
WHERE buyer_zip = 02174

UPDATE distributor_dim
SET distributor_dim.distributor_county = 'New York'
WHERE distributor_zip = 10115


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

-- Table build with only records where buying state = MA, excludes dosage_unit and quantity of 999 and CALC_BASE_WT_IN_GM of 0
-- filters distributors with current_flag set to Y
-- Approx 24 min to build
--DROP TABLE transactions_ma_fact
SELECT t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key, COUNT(buyer_key) AS 'total_transactions',
ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'average_pill_quantity', SUM(CAST(ar.quantity AS FLOAT)) AS 'total_pill_quantity',
ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'average_doses', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'total_doses',
ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'average_grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'total_grams'
INTO Opioids_DW.dbo.transactions_ma_fact
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period_dim t ON t.year = RIGHT(ar.transaction_Date,4) AND t.month = left(ar.transaction_Date,2) 
INNER JOIN Opioids_DW.dbo.distributor_dim di ON di.distributor_name = ar.REPORTER_NAME AND di.current_flag = 'Y'
INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_name = ar.buyer_NAME AND b.buyer_address = ar.buyer_ADDRESS1
INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
INNER JOIN Opioids_DW.dbo.relabeler_dim r ON r.relabeler_name = ar.Combined_Labeler_Name
WHERE b.buyer_state = 'MA' AND dosage_unit != '999' AND quantity != '999' AND CALC_BASE_WT_IN_GM != '0' AND ar.transaction_Date < '20150101'
GROUP BY t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key;

SELECT TOP 100 transaction_Date
FROM Opioids.dbo.arcos 

SELECT TOP 10 *
FROM buyer_dim

SELECT TOP 100 *
FROM distributor_dim

SELECT TOP 10 *
FROM drug_dim

SELECT TOP 10 *
FROM relabeler_dim

SELECT *
FROM time_period_dim

SELECT TOP 10 *
FROM transactions_ma_fact
order by date_key
-----------------------------
--Delete fake data from the opioids database
--DELETE FROM Opioids.dbo.arcos
--WHERE RIGHT(transaction_Date,4) LIKE '%2015'

-- Create fake data to insert into original opioids database to test ETL
-- !!!TODO!!! USE BUILD DATE TABLE WHILE LOOP TO BUILD THIS DATA WITH ITERATIVE DATES
INSERT INTO Opioids.dbo.arcos
VALUES
('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '12262015', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '03202015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01102015', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '11162015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '02112015', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01292015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '10102015', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '06242015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '12211015', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '03112015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01302015', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '11022015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '02162015', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01102015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '10122015', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '06122015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5')



SELECT * FROM time_period_dim

SELECT * 
FROM [dbo].[transactions_ma_fact] tmf
JOIN time_period_dim tpd ON tpd.Date_key = tmf.date_key
WHERE Year = 2015