select distinct buyer_type
from buyer_dim

SELECT TOP 100 * --SUM(CAST(ar.dosage_unit AS FLOAT))
FROM Opioids.dbo.arcos ar
where REPORTER_DEA_NO not in ('PA0006836', 'PB0020139', 'PK0070297', 'PM0020850')

--SELECT SUM(CAST(dosage_unit AS FLOAT))
FROM [dbo].[transactions_ma_fact]

drug_dim

SELECT DISTINCT distributor_key
FROM [dbo].[transactions_ma_fact]

--1,281,378,611
--30,095,941,901


select count(*)
FROM Opioids.dbo.arcos ar
where REPORTER_DEA_NO = 'PB0034861'

-- PM0020850 mckessen

--400
SELECT SUM(CAST(ar.dosage_unit AS FLOAT))
FROM Opioids.dbo.arcos ar
where REPORTER_DEA_NO = 'PB0034861'

-- 115,000
-- 115,000


SELECT t.date_key, di.distributor_key, b.buyer_key, dr.drug_key, COUNT(buyer_key) AS 'total_transactions', 
--t.Month, left(ar.transaction_Date,2) AS 'AR month', t.Year, RIGHT(ar.transaction_Date,4) AS 'AR year',
ROUND(AVG(CAST(ar.quantity AS FLOAT)),3) AS 'average_pill_quantity', SUM(CAST(ar.quantity AS FLOAT)) AS 'total_pill_quantity',
--dosage_unit, quantity, 
ROUND(AVG(CAST(ar.dosage_unit AS FLOAT)),3) AS 'average_doses', SUM(CAST(ar.dosage_unit AS FLOAT)) AS 'total_doses',
ROUND(AVG(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'average_grams', ROUND(SUM(CAST(ar.CALC_BASE_WT_IN_GM AS FLOAT)),3) AS 'total_grams'
--INTO Opioids_DW.dbo.transactions_ma_fact
FROM Opioids.dbo.arcos ar
INNER JOIN Opioids_DW.dbo.time_period_dim t ON t.year = RIGHT(ar.transaction_Date,4) AND t.month = left(ar.transaction_Date,2) 
INNER JOIN Opioids_DW.dbo.distributor_dim di ON di.distributor_name = ar.REPORTER_NAME AND di.current_flag = 'Y'
INNER JOIN Opioids_DW.dbo.buyer_dim b ON b.buyer_name = ar.buyer_NAME AND b.buyer_address = ar.buyer_ADDRESS1
INNER JOIN Opioids_DW.dbo.drug_dim dr ON dr.drug_name = ar.drug_NAME
--INNER JOIN Opioids_DW.dbo.relabeler_dim r ON r.relabeler_name = ar.Combined_Labeler_Name
WHERE b.buyer_state = 'MA' AND dosage_unit != '999' AND quantity != '999' AND CALC_BASE_WT_IN_GM != '0' AND REPORTER_DEA_NO = 'PM0020850'
GROUP BY t.date_key, di.distributor_key, b.buyer_key, dr.drug_key--, quantity, dosage_unit
--ORDER BY buyer_key