/*
Eric Born
CS 689
Assignment 1B
19 March 2020
*/

-- 1.
--SELECT * FROM education_codes;
--SELECT * FROM states
--SELECT * FROM employment_categories
--SELECT * FROM person_economic_info 

-- Create a view with state name, employment category, education level and all columns from person_economic_info
CREATE VIEW annotated_person_info AS
SELECT s.us_state_terr AS 'State_Name', ecat.category_description AS 'Employment_Category_Desc', 
	   ecode.education_level_achieved AS 'Education_Level', pei.*
FROM person_economic_info pei
JOIN education_codes ecode ON ecode.code = pei.education
JOIN states s ON s.numeric_id = pei.address_state
JOIN employment_categories ecat ON ecat.employment_category = pei.employment_category;

SELECT * FROM annotated_person_info

-- 2.
-- Select state, total people, 4k TV's, smartphones, highest and average incomes and percent of male respondents
-- Joins annotated_person_info on two subqueries to calculate only males and total respondents by state
-- Groups on state, total males and total respondents
SELECT api.State_Name, COUNT(*) AS 'Total_People', SUM(own_4k_tv) AS 'Total_4K_TVs', SUM(own_smartphone) AS 'Total_Smartphones',
	   MAX(income) AS 'Highest_Income', AVG(income) AS 'Average_Income', (m.male * 100 / t.total) AS 'Male_Respondents_Percent'
FROM annotated_person_info api
JOIN (SELECT State_Name, COUNT(gender) AS 'male'
	   FROM annotated_person_info 
	   WHERE gender = 'm'
	   GROUP BY State_Name
	   ) AS m ON m.State_Name = api.State_Name
JOIN (SELECT State_Name, COUNT(gender) AS 'total'
	   FROM annotated_person_info 
	   GROUP BY State_Name
	   ) AS t ON t.State_Name = api.State_Name
GROUP BY api.State_Name, m.male, t.total

-- 3.
-- I added in the gender column so it's easier to see which number corresponds with which gender
-- Select state, total people, 4k TV's, smartphones, highest and average incomes and percent of male respondents
-- Joins annotated_person_info on two subqueries to calculate only males and total respondents by state
-- Groups on state, gender, total males and total respondents
SELECT api.State_Name,api.gender, COUNT(*) AS 'Total_People', SUM(own_4k_tv) AS 'Total_4K_TVs', SUM(own_smartphone) AS 'Total_Smartphones',
	   MAX(income) AS 'Highest_Income', AVG(income) AS 'Average_Income', (m.male * 100 / t.total) AS 'Male_Respondents_Percent'
FROM annotated_person_info api
JOIN (SELECT State_Name, COUNT(gender) AS 'male'
	   FROM annotated_person_info 
	   WHERE gender = 'm'
	   GROUP BY State_Name
	   ) AS m ON m.State_Name = api.State_Name
JOIN (SELECT State_Name, COUNT(gender) AS 'total'
	   FROM annotated_person_info 
	   GROUP BY State_Name
	   ) AS t ON t.State_Name = api.State_Name
GROUP BY api.State_Name, m.male, t.total, api.gender
ORDER BY State_Name

-- 4.
