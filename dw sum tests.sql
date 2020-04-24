SELECT TOP 100 * --SUM(CAST(ar.dosage_unit AS FLOAT))
FROM Opioids.dbo.arcos ar
where REPORTER_DEA_NO = 'PM0020850'
-- REPORTER_DEA_NO = 'RA0290724'

-- PM0020850 MCKESSON CORPORATION
-- RA0290724 AMERISOURCEBERGEN DRUG CORP

-- 215,733,265 MCKESSON CORPORATION
--  52,870,220 AMERISOURCEBERGEN DRUG CORP
SELECT SUM(CAST(ar.dosage_unit AS FLOAT))
FROM Opioids.dbo.arcos ar
where REPORTER_DEA_NO = 'PM0020850'
AND RIGHT(ar.transaction_Date,4) = 2006 AND left(ar.transaction_Date,2) = 01
AND dosage_unit != '999' AND quantity != '999' AND CALC_BASE_WT_IN_GM != '0'
--where REPORTER_DEA_NO = 'RA0290724'

-- 1,421,580 -- 2006 jan

-- 1,421,580
-- 1,534,180



-- 
-- 52,870,220 AMERISOURCEBERGEN DRUG CORP
SELECT DISTINCT t.date_key, di.distributor_key, 
b.buyer_key, 
dr.drug_key, --COUNT(buyer_key) AS 'total_transactions', 
--ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'average_pill_quantity', SUM(CAST(ar.quantity AS FLOAT)) AS 'total_pill_quantity',
ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'average_doses', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'total_doses'--,
--ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'average_grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'total_grams'
--INTO Opioids_DW.dbo.transactions_ma_fact
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period_dim t ON t.year = RIGHT(ar.transaction_Date,4) AND t.month = left(ar.transaction_Date,2) 
INNER JOIN Opioids_DW.dbo.distributor_dim di ON ar.REPORTER_ADDRESS1 = di.distributor_address 
AND ar.REPORTER_CITY = di.distributor_city AND ar.REPORTER_ZIP = di.distributor_zip
INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_address = ar.buyer_ADDRESS1 AND b.buyer_city = ar.BUYER_CITY
AND b.buyer_zip = ar.BUYER_ZIP AND b.buyer_dea_num = ar.BUYER_DEA_NO
INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
WHERE --b.buyer_state = 'MA' AND 
dosage_unit != '999' AND quantity != '999' AND CALC_BASE_WT_IN_GM != '0' AND REPORTER_DEA_NO = 'PM0020850'
AND date_key = 1
GROUP BY t.date_key, dr.drug_key, di.distributor_key, b.buyer_key

SELECT * 
FROM distributor_dim

SELECT top 1 *
--REPORTER_BUS_ACT,	REPORTER_NAME	,REPORTER_ADDL_CO_INFO	,REPORTER_ADDRESS1	,REPORTER_ADDRESS2	,REPORTER_CITY	,REPORTER_STATE	,REPORTER_ZIP	,REPORTER_COUNTY
FROM Opioids.dbo.arcos ar