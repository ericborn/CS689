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

-- Part 3.
-- Identify the three highest product unit output factories for each year
-- uses a subquery with rank on the sum of the QTY_PASSED, partitioned by the production year
SELECT [Year], [Factory Name], [Product Description], [Total Units produced], [Total Units rejected]
 FROM (
SELECT RIGHT(MANUFACTURE_YEAR,4) AS 'Year', FACTORY_LABEL AS 'Factory Name', PRODUCT_DESCRIPTION AS 'Product Description', 
SUM(QTY_PASSED) AS 'Total Units produced', SUM(QTY_FAILED) AS 'Total Units rejected', 
RANK() OVER(PARTITION BY MANUFACTURE_YEAR ORDER BY SUM(QTY_PASSED) DESC) Qty_Rank
FROM MANUFACTURE_FACT mf
JOIN CALENDAR_MANUFACTURE_DIM cm ON cm.MANUFACTURE_CAL_KEY = mf.MANUFACTURE_CAL_KEY
JOIN PRODUCT_DIM pd ON pd.PRODUCT_KEY = mf.PRODUCT_KEY
JOIN FACTORY_DIM fd ON fd.FACTORY_KEY = mf.FACTORY_KEY
GROUP BY MANUFACTURE_YEAR, FACTORY_LABEL, PRODUCT_DESCRIPTION) t
WHERE Qty_Rank <= 3
ORDER BY Year