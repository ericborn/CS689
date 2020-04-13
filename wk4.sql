-- Part 1.
USE CS689_Assign4;

SELECT COUNT(*)
FROM manufacture_fact;

-- Part 2.
SELECT TOP 10 *
FROM MATERIAL_DIM;

SELECT TOP 10 *
FROM [dbo].[CALENDAR_MANUFACTURE_DIM];

SELECT TOP 10 *
FROM [dbo].[FACTORY_DIM];

SELECT TOP 10 *
FROM [dbo].[PERSON_FOREMAN_DIM];

SELECT TOP 10 *
FROM [dbo].[PRODUCT_DIM];

SELECT TOP 10 *
FROM [dbo].[PERSON_SALES_DIM];

SELECT TOP 10 *
FROM [dbo].[CALENDAR_SALES_DIM];

SELECT TOP 10 *
FROM [dbo].[PERSON_CUSTOMER_DIM];

-- Part 3A.
-- Identify the three highest product unit output factories for each year
-- Uses a subquery with rank on the sum of the QTY_PASSED added with QTY_FAILED, partitioned by the production year
SELECT [Year], [Factory Name], [Product Description], [Total Units produced], [Total Units rejected]
 FROM (
SELECT RIGHT(MANUFACTURE_YEAR,4) AS 'Year', FACTORY_LABEL AS 'Factory Name', PRODUCT_DESCRIPTION AS 'Product Description', 
SUM(QTY_PASSED) AS 'Total Units produced', SUM(QTY_FAILED) AS 'Total Units rejected', 
RANK() OVER(PARTITION BY MANUFACTURE_YEAR ORDER BY SUM(QTY_PASSED + QTY_FAILED) DESC) Qty_Rank
FROM MANUFACTURE_FACT mf
JOIN CALENDAR_MANUFACTURE_DIM cm ON cm.MANUFACTURE_CAL_KEY = mf.MANUFACTURE_CAL_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = mf.PRODUCT_KEY
JOIN FACTORY_DIM fd ON fd.FACTORY_KEY = mf.FACTORY_KEY
GROUP BY MANUFACTURE_YEAR, FACTORY_LABEL, PRODUCT_DESCRIPTION) t
WHERE Qty_Rank <= 3
ORDER BY Year

-- Part 3B.
-- Subquery in the where statement uses rank to find the highest production year.
-- Outer subquery uses rank to find the top three factories from each month
SELECT [Month], [Year], [Factory Name], [Product Description], [Total Units produced], [Total Units rejected] FROM (
SELECT RIGHT(MANUFACTURE_YEARMONTH,2) AS 'Month', LEFT(RIGHT(MANUFACTURE_YEARMONTH,6),4) AS 'Year', FACTORY_LABEL AS 'Factory Name',
PRODUCT_DESCRIPTION AS 'Product Description', SUM(QTY_PASSED) AS 'Total Units produced', SUM(QTY_FAILED) AS 'Total Units rejected',
RANK() OVER(PARTITION BY MANUFACTURE_YEARMONTH ORDER BY SUM(QTY_PASSED + QTY_FAILED) DESC) FIRST_Rank
FROM MANUFACTURE_FACT mf
JOIN CALENDAR_MANUFACTURE_DIM cm ON cm.MANUFACTURE_CAL_KEY = mf.MANUFACTURE_CAL_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = mf.PRODUCT_KEY
JOIN FACTORY_DIM fd ON fd.FACTORY_KEY = mf.FACTORY_KEY
WHERE MANUFACTURE_YEAR = (SELECT MANUFACTURE_YEAR FROM (SELECT MANUFACTURE_YEAR, 
RANK() OVER(ORDER BY SUM(QTY_PASSED + QTY_FAILED) DESC) Qty_Rank
FROM MANUFACTURE_FACT mf
JOIN CALENDAR_MANUFACTURE_DIM cm ON cm.MANUFACTURE_CAL_KEY = mf.MANUFACTURE_CAL_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = mf.PRODUCT_KEY
JOIN FACTORY_DIM fd ON fd.FACTORY_KEY = mf.FACTORY_KEY
GROUP BY MANUFACTURE_YEAR
) i WHERE Qty_Rank = 1) 
GROUP BY MANUFACTURE_YEARMONTH, FACTORY_LABEL, PRODUCT_DESCRIPTION
) o
WHERE FIRST_Rank <= 3
ORDER BY [Month], [Total Units produced] DESC

-- Part 3C.
SELECT RIGHT(SALES_YEAR,4) AS 'Year', BRAND_LABEL, SUM(ORDER_AMOUNT) AS 'Total Money Earned'
FROM SALES_FACT sf
JOIN CALENDAR_SALES_DIM csd ON sf.SALES_DAY_KEY = csd.SALES_DAY_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = sf.PRODUCT_KEY
WHERE SALES_MONTHOFYEAR = 'sM09'
GROUP BY SALES_YEAR, BRAND_LABEL
ORDER BY BRAND_LABEL, [Year]

-- Part 3D.
-- Pivot table displaying the factory names and their total quantity produced, both passed and failed, by year.
SELECT *
FROM (
SELECT RIGHT(MANUFACTURE_YEAR,4) AS 'Year', FACTORY_LABEL AS 'Factory Name', (QTY_PASSED + QTY_FAILED) AS 'Total_Produced'
FROM MANUFACTURE_FACT mf
JOIN CALENDAR_MANUFACTURE_DIM cm ON cm.MANUFACTURE_CAL_KEY = mf.MANUFACTURE_CAL_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = mf.PRODUCT_KEY
JOIN FACTORY_DIM fd ON fd.FACTORY_KEY = mf.FACTORY_KEY
)t
PIVOT(
SUM(Total_Produced)
FOR Year IN (
[2010],[2011],[2012],[2013],
[2014],[2015],[2016],[2017],
[2018],[2019],[2020],[2021],
[2022])) AS pivot_table
ORDER BY [Factory Name]