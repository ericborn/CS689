/*
Eric Born
CS 689
Assignment 1B
19 March 2020
*/

SELECT * FROM education_codes;
SELECT COUNT(*) FROM household_income;
SELECT * FROM states
SELECT * FROM employment_categories
SELECT * FROM person_economic_info 

SELECT s.us_state_terr, ecat.category_description AS 'Employment_Category', 
	   ecode.education_level_achieved AS 'Education_Level', pei.*
FROM person_economic_info pei
JOIN education_codes ecode ON ecode.code = pei.education
JOIN states s ON s.numeric_id = pei.address_state
JOIN employment_categories ecat ON ecat.employment_category = pei.employment_category
