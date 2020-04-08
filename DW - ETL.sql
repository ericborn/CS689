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

-- Code to setup the buyer table within the warehouse
--DROP SEQUENCE buyer_key
CREATE SEQUENCE buyer_key
START WITH 10
INCREMENT BY 1;

SELECT NEXT VALUE FOR buyer_key AS buyer_key, 
ar.BUYER_NAME AS 'buyer_name', ar.BUYER_ADDRESS1 AS 'buyer_address', 
ar.BUYER_CITY AS 'buyer_city', ar.BUYER_STATE AS 'buyer_state', ar.BUYER_ZIP AS 'buyer_zip', ar.BUYER_COUNTY AS 'buyer_county'
INTO buyer
FROM (SELECT DISTINCT BUYER_NAME, BUYER_ADDRESS1, BUYER_CITY, BUYER_STATE, BUYER_ZIP, BUYER_COUNTY
	  FROM Opioids.dbo.arcos) ar;

-- Code to setup the drug table within the warehouse
--DROP SEQUENCE drug_key
CREATE SEQUENCE drug_key
START WITH 1
INCREMENT BY 1;

SELECT NEXT VALUE FOR drug_key AS drug_key, 
ar.DRUG_NAME AS 'drug_name'
INTO drug
FROM (SELECT DISTINCT DRUG_NAME
	  FROM Opioids.dbo.arcos) ar;

-- Code to setup the relabeler table within the warehouse
--DROP SEQUENCE relabeler_key
CREATE SEQUENCE relabeler_key
START WITH 1
INCREMENT BY 1;

SELECT NEXT VALUE FOR relabeler_key AS relabeler_key, 
ar.combined_labeler_name AS 'relabeler_name'
INTO relabeler
FROM (SELECT DISTINCT combined_labeler_name
	  FROM Opioids.dbo.arcos) ar;




--DROP TABLE orders
-- Gathers the initial data from the Opioids database and insert it into a table called transactions in the Opioids_DW database
-- does a convert on the time_period.date_key from INT to DATE
-- also converts orders order_purchase_timestamp from DATETIME to DATE
-- filters out any canceled orders and only orders earlier than 2019 for SSIS demonstration purposes
USE Opioids_DW
SELECT t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key,
AVG(ar.quantity) AS 'Avg_Qty', SUM(ar.quantity) AS 'Sum_QTY',
AVG(ar.dosage_unit) AS 'Avg_Dose', SUM(ar.dosage_unit) AS 'Sum_Dose',
AVG(ar.CALC_BASE_WT_IN_GM) AS 'Avg_Grams', SUM(ar.CALC_BASE_WT_IN_GM) AS 'Sum_Grams'
--INTO Opioids_DW.dbo.transactions
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,ar.transaction_date,112)
INNER JOIN Opioids_DW.dbo.distributor di ON di.distributor_name = ar.REPORTER_NAME AND di.distributor_address = ar.REPORTER_ADDRESS1
INNER JOIN Opioids_DW.dbo.buyer b ON b.buyer_name = ar.BUYER_NAME AND b.buyer_address = ar.BUYER_ADDRESS1
INNER JOIN Opioids_DW.dbo.drug dr ON dr.drug_name = ar.DRUG_NAME
INNER JOIN Opioids_DW.dbo.relabeler r ON r.relabeler_name = ar.Combined_Labeler_Name
GROUP BY t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, r.relabeler_key

SELECT * FROM time_period

l.location_key, p2.product_key, oi.seller_id, 
SUM(oi.price) AS 'sales_total', COUNT(oi.product_id) AS 'sales_quantity'
INTO Olist_DW.dbo.orders
FROM Olist_Orders.dbo.orders o
INNER JOIN Olist_Orders.dbo.order_items oi ON oi.order_id = o.order_id
INNER JOIN Olist_Orders.dbo.products p ON p.product_id = oi.product_id
INNER JOIN Olist_Orders.dbo.category c ON c.product_category_name = p.product_category_name
INNER JOIN Olist_DW.dbo.product p2 ON p2.product = c.Product_category_name_english
INNER JOIN Olist_Orders.dbo.sellers s ON s.seller_id = oi.seller_id
INNER JOIN Olist_DW.dbo.time_period t ON CONVERT(DATE,CONVERT(VARCHAR(8),t.date_key,112)) = CONVERT(DATE,o.order_purchase_timestamp,112)
INNER JOIN Olist_DW.dbo.location l ON l.z-ip = s.seller_zip_code_prefix AND l.city = s.seller_city
WHERE o.order_status != 'canceled' AND order_purchase_timestamp < '20190101'
GROUP BY t.date_key, l.location_key, p2.product_key, oi.seller_id;