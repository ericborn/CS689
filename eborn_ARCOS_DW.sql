/*
Eric Born
25 April 2020
CS 689 Final project
ARCOS data warehouse
*/

-- Create a smaller arcos table based upon only records where the buying state was MA
SELECT *
INTO arcos
FROM arcos_full
WHERE BUYER_STATE = 'ma';

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

----------------
-- Start code to create sequences and dimension tables
----------------
-- set the query to use the data warehouse database
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

-- Build out a time_period table which contains month, month name, quarter and year
BEGIN TRY
	DROP TABLE [dbo].[time_period_dim]
END TRY

BEGIN CATCH
	/*No Action*/
END CATCH;

/**********************************************************************************/

CREATE TABLE	[dbo].[time_period_dim]
	(	
		[Date_key] INT PRIMARY KEY, 
		[Month] INT, --Number of the Month 1 to 12
		[MonthName] VARCHAR(9),--January, February etc
		[Quarter] INT,
		[Year] INT,-- Year value of Date stored in Row
	)
GO

/********************************************************************************************/

-- Create start and end year and current date value
DECLARE @StartYear DATE = '01/01/2006',
		@EndYear DATE = '01/01/2015',
		@CurrentDate DATE = '01/01/2006';

-- set current date = to start year
SET @CurrentDate = @StartYear;

/********************************************************************************************/
--Proceed only if Start Date(Current date ) is less than End date you specified above
BEGIN TRY
	DROP SEQUENCE date_key
END TRY

BEGIN CATCH
	/*No Action*/
END CATCH;

CREATE SEQUENCE date_key
START WITH 1
INCREMENT BY 1;

WHILE @CurrentDate < @EndYear
BEGIN
 
/* Populate Your Dimension Table with values*/
	
	INSERT INTO [dbo].[time_period_dim]
	SELECT
		NEXT VALUE FOR date_key AS date_key, 
		DATEPART(MM, @CurrentDate) AS Month,
		DATENAME(MM, @CurrentDate) AS MonthName,
		DATEPART(QQ, @CurrentDate) AS Quarter,
		DATEPART(YEAR, @CurrentDate) AS Year
	SET @CurrentDate = DATEADD(MM, 1, @CurrentDate)
END

-- Update months 1-9 with a 0 to the left
UPDATE [time_period_dim]
SET Month = (SELECT LEFT('0',1)+month)
WHERE Month <= 9

SELECT * FROM [time_period_dim]
/********************************************************************************************/

----------------
-- End code to create sequences and dimension tables
----------------

-- Function found on stack overflow for identifying non-numeric values when trying to convert between varchar and float
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

----------------
-- Start code to create fact table
----------------

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

----------------
-- End code to create fact table
----------------

----------------
-- Start code to create fake distributor address information
----------------
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

----------------
-- End code to create fake distributor address information
----------------

----------------
-- Start code to test ETL package
----------------

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

----------------
-- End code to test ETL package
----------------