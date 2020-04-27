/*
Eric Born
25 April 2020
CS 689 Final project
ARCOS data warehouse
*/

-- create a smaller arcos table based upon only records where the buying state was MA
SELECT *
INTO arcos
FROM arcos_full
WHERE BUYER_STATE = 'ma';

USE Opioids_DW;
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
SELECT NEXT VALUE FOR buyer_key AS buyer_key, ar.BUYER_DEA_NO AS 'buyer_dea_num', ar.BUYER_BUS_ACT AS 'buyer_type', ar.BUYER_NAME AS 'buyer_name', ar.BUYER_ADDRESS1 AS 'buyer_address', 
ar.BUYER_CITY AS 'buyer_city', ar.BUYER_STATE AS 'buyer_state', ar.BUYER_ZIP AS 'buyer_zip', ar.BUYER_COUNTY AS 'buyer_county'
--,1 AS 'current_row_indicator', '20040101' AS 'row_effective_date', '20991231' AS 'row_expiration_date'
INTO buyer_dim
FROM (SELECT DISTINCT BUYER_DEA_NO, BUYER_BUS_ACT, BUYER_NAME, BUYER_ADDRESS1, BUYER_CITY, BUYER_STATE, BUYER_ZIP, BUYER_COUNTY
	  FROM Opioids.dbo.arcos
	  WHERE BUYER_STATE = 'MA') ar;
	  
-- Code to setup the drug_dim table within the warehouse
--DROP SEQUENCE drug_dim_key
CREATE SEQUENCE drug_key
START WITH 1
INCREMENT BY 1;

SELECT NEXT VALUE FOR drug_key AS drug_key, ar.DRUG_NAME AS 'drug_name'
INTO drug_dim
FROM (SELECT DISTINCT DRUG_NAME
	  FROM Opioids.dbo.arcos) ar;

-- Code to setup the county_dim table within the warehouse
--DROP SEQUENCE county_dim_key
CREATE SEQUENCE county_dim_key
START WITH 1
INCREMENT BY 1;

-- table holds county name and population
CREATE TABLE county_pop_dim
(
county_pop_key INT,
county_name VARCHAR(50),
[year] INT,
[population] INT
);

-- populate the county populations over time
INSERT INTO county_pop_dim
VALUES
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2006,546353),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2007,547380),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2008,548713),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2009,549743),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2010,549177),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2011,549308),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2012,551120),
(NEXT VALUE FOR county_dim_key, 'BRISTOL',2013,552379),

(NEXT VALUE FOR county_dim_key, 'DUKES', 2006, 15552),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2007, 15667),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2008, 15847),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2009, 16051),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2010, 16572),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2011, 16697),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2012, 16829),
(NEXT VALUE FOR county_dim_key, 'DUKES', 2013, 17159),

(NEXT VALUE FOR county_dim_key, 'ESSEX',2006,735261),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2007,737283),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2008,741781),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2009,747196),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2010,745479),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2011,751507),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2012,757338),
(NEXT VALUE FOR county_dim_key, 'ESSEX',2013,764563),

(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2006,72132),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2007,72047),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2008,72080),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2009,71937),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2010,71366),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2011,71692),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2012,71687),
(NEXT VALUE FOR county_dim_key,	 'FRANKLIN',2013,71361),

(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2006,461786),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2007,461794),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2008,462301),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2009,462847),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2010,464256),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2011,466171),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2012,466955),
(NEXT VALUE FOR county_dim_key,	'HAMPDEN',2013,467747),

(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2006,155365),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2007,155883),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2008,156460),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2009,156582),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2010,159320),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2011,160154),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2012,160419),
(NEXT VALUE FOR county_dim_key,	'HAMPSHIRE',2013,160856),

(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2006,1464000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2007,1472000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2008,1487000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2009,1506000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2010,1508000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2011,1525000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2012,1543000),
(NEXT VALUE FOR county_dim_key,	'MIDDLESEX',2013,1560000),

(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2006,10400),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2007,10598),
(NEXT VALUE FOR county_dim_key, 'NANTUCKET',2008,10794),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2009,10857),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2010,10167),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2011,10130),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2012,10311),
(NEXT VALUE FOR county_dim_key,	'NANTUCKET',2013,10567),

(NEXT VALUE FOR county_dim_key,	'NORFOLK',2006,656303),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2007,659793),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2008,665197),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2009,669990),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2010,673039),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2011,677782),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2012,682865),
(NEXT VALUE FOR county_dim_key,	'NORFOLK',2013,552379),

(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2006,491697),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2007,493891),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2008,496695),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2009,499749),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2010,495930),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2011,498184),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2012,499495),
(NEXT VALUE FOR county_dim_key,	'SUFFOLK',2013,502792),

(NEXT VALUE FOR county_dim_key,	'WORCESTER',2006,785445),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2007,788179),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2008,790569),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2009,794718),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2010,800401),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2011,804063),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2012,806942),
(NEXT VALUE FOR county_dim_key,	'WORCESTER',2013,810846);


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

---- update distributor county based on zipcode from uszips table
---- uszips table downloaded from here: https://simplemaps.com/data/us-zips
--UPDATE distributor_dim
--SET distributor_dim.distributor_county = u.county_name
--FROM distributor_dim d
--JOIN uszips u ON d.distributor_city = u.city AND d.distributor_state = u.state_name
--WHERE d.distributor_county = '';

--DROP TABLE orders
-- Gathers the initial data from the Opioids database and insert it into a table called transactions_fact in the Opioids_DW database
-- does a convert on the time_period.date_key from INT to DATE
-- Also converts using RIGHT AND LEFT to transform the arcos data format from MMDDYYYY to YYYYMMDD
USE Opioids_DW;

-- Full table build for all states
-- Approx 24 min to build
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
-- Approx 25 seconds to build
--DROP TABLE transactions_ma_fact
SELECT DISTINCT t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, COUNT(buyer_key) AS 'total_transactions', 
ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'average_boxes', SUM(CAST(ar.quantity AS FLOAT)) AS 'total_boxes',
ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'average_pills', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'total_pills',
ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'average_grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'total_grams'
INTO Opioids_DW.dbo.transactions_ma_fact
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period_dim t ON t.year = RIGHT(ar.transaction_Date,4) AND t.month = left(ar.transaction_Date,2) 
INNER JOIN Opioids_DW.dbo.distributor_dim di ON ar.REPORTER_ADDRESS1 = di.distributor_address 
AND ar.REPORTER_CITY = di.distributor_city AND ar.REPORTER_ZIP = di.distributor_zip
INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_address = ar.buyer_ADDRESS1 AND b.buyer_city = ar.BUYER_CITY
AND b.buyer_zip = ar.BUYER_ZIP AND b.buyer_dea_num = ar.BUYER_DEA_NO
INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
WHERE dosage_unit != '999' AND quantity != '999' AND CALC_BASE_WT_IN_GM != '0'
GROUP BY t.date_key, dr.drug_key, di.distributor_key, b.buyer_key;

--SELECT TOP 100 transaction_Date
--FROM Opioids.dbo.arcos 

--SELECT TOP 10 *
--FROM buyer_dim

--SELECT TOP 100 *
--FROM distributor_dim

--SELECT TOP 10 *
--FROM drug_dim

--SELECT TOP 10 *
--FROM relabeler_dim

--SELECT *
--FROM time_period_dim

--SELECT TOP 10 *
--FROM transactions_ma_fact
--order by date_key

--SELECT DISTINCT buyer_type FROM buyer_dim

-- Convert PRACTITIONER buyer types from three sub category types into a single category
-- Moved to SSIS package
--UPDATE buyer_dim
--SET buyer_type = 'PRACTITIONER'
--WHERE buyer_type LIKE 'PRACTITIONER-%'

-- null value for a county with the zipcode of 02401, 02174, 10115
--UPDATE buyer_dim
--SET buyer_county = 'Plymouth'
--WHERE buyer_zip = 02401;

--UPDATE buyer_dim
--SET buyer_county = 'Middlesex'
--WHERE buyer_zip = 02174;

--UPDATE distributor_dim
--SET distributor_dim.distributor_county = 'New York'
--WHERE distributor_zip = 10115;

--SELECT * FROM BUYER_DIM
--WHERE BUYER_COUNTY = 'NULL'

-----------------------------
-- Update distributor data with fake addresses from address table
UPDATE distributor_DIM
SET distributor_address = a.distributor_address, current_flag = 'N'
FROM distributor_DIM dd
JOIN addresses a ON a.distributor_key = dd.distributor_key
WHERE dd.distributor_key = a.distributor_key

-- Insert real distributor data as changed address
INSERT INTO distributor_DIM
VALUES
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'PRICE CHOPPER OPERATING CO INC',	'501 DUANESBURG ROAD', 			 'SCHENECTADY',	'NY', '12306', 'SCHENECTADY',	'20050716', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'H D SMITH WHOLESALE DRUG CO',		'410 COMMERCE BLVD UNIT B', 	 'CARLSTADT',	'NJ', '7072',  'BERGEN',		'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'HENRY SCHEIN, INC',				'41 WEAVER ROAD',				 'DENVER',		'PA', '17517', 'LANCASTER', 	'20050716', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'AMERICAN HEALTH SERVICE SALES',	'DBA MED-VET INTERNATIONAL', 	 'METTAWA',		'IL', '60045', 'LAKE',			'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'ACTAVIS PHARMA INC.',				'605 TRI-STATE PARKWAY', 		 'GURNEE',		'IL', '60031', 'LAKE',			'20050716', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'CALIGOR PHYSICIANS & HOSPITAL',	'DBA CALIGOR & ROANE BARKER', 	 'SPARKS',		'NV', '89434', 'WASHOE',		'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'RITE AID MID-ATLANTIC',			'CUSTOMER SUPPORT CENTER', 		 'ABERDEEN',	'MD', '21001', 'HARFORD',		'20080211', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'MIDWEST VETERINARY SUPPLY INC',	'5374 MALY ROAD, RTE 1',		 'SUN PRAIRIE',	'WI', '53590', 'DANE',			'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'THE HARVARD DRUG GROUP-MI',		'FIRST VETERINARY SUPPLY', 		 'LIVONIA',		'MI', '48150', 'WAYNE',			'20060216', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'AMERISOURCEBERGEN DRUG CORP.',		'5100 JAINDL BLVD.', 			 'BETHLEHEM',	'PA', '18017', 'NORTHAMPTON',	'20060216', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'WALGREEN CO',						'15998 WALGREENS DRIVE', 		 'JUPITER',		'FL', '33478', 'PALM BEACH',	'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'LIFELINE PHARMACEUTICALS LLC',		'1301 NW 84 AVE', 				 'MIAMI',		'FL', '33126', 'MIAMI-DADE',	'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'ANDA PHARMACEUTICALS INC',			'6500 ADELAIDE COURT', 			 'GROVEPORT',	'OH', '43125', 'FRANKLIN',		'20080211', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'MCKESSON MEDICAL SURGICAL INC',	'16043 EL PRADO ROAD', 			 'CHINO',		'CA', '91708', 'SAN BERNARDINO','20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'BUTLER ANIMAL HEALTH SUPPLY',		'STOCKROOM 02', 				 'COLUMBUS',	'OH', '43204', 'FRANKLIN',		'20060216', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'TOP RX, INC.',						'2950 BROTHER BOULEVARD', 		 'BARTLETT',	'TN', '38133', 'SHELBY',		'20080624', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'MCGUFF COMPANY',					'3524 WEST LAKE CENTER DRIVE',	 'SANTA ANA', 	'CA', '92704', 'ORANGE',		'20080211', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'CARDINAL HEALTH',					'2840 ELM POINT INDUSTRIAL DR.', 'ST CHARLES',	'MO', '63301', 'SAINT CHARLES',	'20080211', 'Y'),
(NEXT VALUE FOR distributor_key, 'DISTRIBUTOR', 'MCKESSON CORPORATION',				'DBA MCKESSON DRUG COMPANY', 	 'LANDOVER', 	'MD', '20785', 'PRINCE GEORGES','20060216', 'Y')


--Delete fake data from the opioids database
--DELETE FROM Opioids.dbo.arcos
--WHERE RIGHT(transaction_Date,4) = 2013

-- Delete fake data from fact table
-- 85 is the date key for jan 1 2013
--DELETE FROM Opioids_DW.dbo.transactions_ma_fact
--WHERE date_key >= 85

--SELECT * FROM distributor_dim
--where distributor_name = 'ACE SURGICAL SUPPLY CO INC' or
-- distributor_name = 'BURLINGTON DRUG COMPANY'

-- check max datekey within the fact table before putting any fake data in
-- result is 84, december 2012
SELECT *
FROM [time_period_dim]
WHERE DATE_KEY = (SELECT MAX(DATE_KEY)
				  FROM transactions_ma_fact)

SELECT count(*)
FROM Opioids.dbo.arcos
WHERE RIGHT(TRANSACTION_DATE,4) = '2013'

-- Insert fake data for 2013 into original opioids database to test ETL
INSERT INTO Opioids.dbo.arcos
VALUES
('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01262013', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01052013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01102013', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '01162013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01112013', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01292013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01102013', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '01242013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01212013', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01112013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01302013', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '01022015', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'TABRIZI, HAMID R DMD', 'NULL', '389 MAIN STREET, SUITE 404', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01162013', '0.6054', '100.0', '64', 'HYDROCODONE BIT/ACETA 10MG/500MG USP', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'SpecGx LLC', 'Mallinckrodt', 'ACE Surgical Supply Co Inc', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'AUCELLA DRUG', 'NULL', '705 SALEM STREET', 'NULL', 'MALDEN', 'MA', '2148', 'MIDDLESEX', 'S', '9143', '59011010710', 'OXYCODONE', '12.0', 'null', 'null', 'null', 
'null', 'null', '01102013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Purdue Pharma LP', 'Burlington Drug Company', '80.0'),

('PA0006836','DISTRIBUTOR', 'ACE SURGICAL SUPPLY CO INC', 'NULL', '1034 PEARL STREET', 'NULL', 'BROCKTON', 'MA', '2301', 'PLYMOUTH', 'BT3484653', 'PRACTITIONER', 
'BILLS PHCY OF GT BARRINGTON', 'NULL', '362 MAIN ST, SUITE 2', 'NULL', 'GREAT BARRINGTON', 'MA', '1230', 'MIDDLESEX', 'S', '9193', '00406036301', 'HYDROCODONE', '1.0', 'null', 
'null', 'null', 'null', 'null', '01122013', '2.27025', '500.0', '801001226', 'HYDROCODONE.BITARTRATE 7.5MG/APAP 75', 'HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'TAB', '1.0', 
'Amneal Pharmaceuticals LLC', 'Amneal Pharmaceuticals, Inc.', 'Burlington Drug Company', '10.0'),

('PA0006836','DISTRIBUTOR', 'BURLINGTON DRUG COMPANY', 'NULL', '91 CATAMOUNT DR', 'NULL', 'MILTON', 'VT', '5468', 'CHITTENDEN', 'AA3181891', 'RETAIL PHARMACY', 
'BROWNS REXALL DRUG', 'NULL', '214 WINTHROP STREET', 'NULL', 'WINTHROP', 'MA', '2152', 'SUFFOLK', 'S', '9193', '53746011201', 'HYDROCODONE', '2.0', 'null', 'null', 'null', 
'null', 'null', '01232013', '86.064', '1200.0', '701007813', 'OXYCONTIN - 80MG OXYCODONE.HCL CONTR', 'OXYCODONE HYDROCHLORIDE', 'TAB', '1.5', 'Purdue Pharma LP', 
'Par Pharmaceutical', 'Endo Pharmaceuticals, Inc.', '7.5')

-- check max datekey within the fact table 
-- result is 85
-- 85 is January 2013
SELECT *
FROM [time_period_dim]
WHERE DATE_KEY = (SELECT MAX(DATE_KEY)
				  FROM transactions_ma_fact)