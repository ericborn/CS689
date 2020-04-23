/*
This script will build out a time_period table which contains month, month name, quarter and year

Original script create by Mubin M. Shaikh
from https://www.codeproject.com/Articles/647950/Create-and-Populate-Date-Dimension-for-Data-Wareho
*/
--select * from [time_period]
-- set the query to use the data warehouse database
USE Opioids_DW

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
		[Month] VARCHAR(2), --Number of the Month 1 to 12
		[MonthName] VARCHAR(9),--January, February etc
		[Quarter] CHAR(1),
		[Year] CHAR(4),-- Year value of Date stored in Row
	)
GO

/********************************************************************************************/

-- Create start and end year and current date value
DECLARE @StartYear DATE = '01/01/2006',
		@EndYear DATE = '01/01/2016',
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

select * from [time_period_dim]
/********************************************************************************************/