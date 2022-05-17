ALTER TABLE online_retail_main
ALTER COLUMN invoice_date TYPE date;

SELECT * FROM online_retail


-- TOTAL COUNT = 541909
-- RECORDS WITHOUT CUSTOMER ID = 135080
-- RECORDS WITH CUSTOMER ID = 406829

SELECT COUNT(*) FROM online_retail
WHERE customer_id is not null



WITH CTE as
	(
	-- RECORDS WITH CUSTOMER ID = 406829
	SELECT * FROM online_retail
	WHERE customer_id is not null
	),
CTE2 as
	(
	--- RECORDS WITH VALID QUANTITY AND UNIT PRICE = 397884
	SELECT * FROM CTE
	WHERE quantity > 0 AND unit_price > 0
	),
DUPLICATE as
	(
    -- DUPLICATE CHECK 
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY invoice_no, stock_code, quantity ORDER BY invoice_date) AS dup	
	FROM CTE2
	)
    -- 392669 CLEAN DATA AND 5215 DUPLICATES
	--SELECT COUNT(*) FROM DUPLICATE
	--WHERE dup > 1
SELECT *
INTO TABLE online_retail_main
FROM DUPLICATE
WHERE dup = 1


-- I'VE PASSED THE CLEAN DATA INTO A NEW TABLE online_retail_main


SELECT * FROM online_retail_main


--- TO BEGIN THE COHORT ANALYSIS WE NEED:
--- UNIQUE IDENTIFIER (CUSTOMER ID)
--- INTITAL DATE (FIRST INVOICE DATE)
--- REVENUE DATA


SELECT 
customer_id,
MIN(invoice_date),
MAKE_DATE(CAST (EXTRACT(YEAR FROM invoice_date) AS INTEGER), CAST(EXTRACT(MONTH FROM invoice_date) AS INTEGER), 1) AS cohort_date
INTO TABLE cohort
FROM online_retail_main
GROUP BY customer_id, cohort_date



--(I'VE CREATED ANOTHER TABLE COHORT)

--(A COHORT IS SIMPLY A GROUP OF PEOPLE WITH SOMETHING IN COMMON)
--(A COHORT ANALYSIS IS AN ANALYSIS OF SEVERAL DIFFERENT COHORTS TO GET BETTER UNDERSTANDING OF BEHAVIORS, PATTERS AND TRENDS)

SELECT * FROM cohort



-- CREATING COHORT INDEX
-- THEN PASSING THE VALUES INTO A NEW TABLE

SELECT
temp2.*,
(year_diff * 12 + month_diff + 1) AS cohort_index
INTO TABLE cohort_retention
FROM
	(
		SELECT
		temp.*,
		(invoice_year - cohort_year) AS year_diff,
		(invoice_month - cohort_month) AS month_diff
		FROM
			(
				SELECT a.*,
					   b.cohort_date,
					   EXTRACT(YEAR FROM a.invoice_date) AS invoice_year,
					   EXTRACT(MONTH FROM a.invoice_date) AS invoice_month,
					   EXTRACT(YEAR FROM b.cohort_date) AS cohort_year,
					   EXTRACT(MONTH FROM b.cohort_date) AS cohort_month
				FROM online_retail_main a
				LEFT JOIN cohort b
				ON a.customer_id = b.customer_id
			 ) temp
	 ) temp2 	
 



SELECT * FROM cohort_retention




-- (PASSING THE EXTENSION TABLEFUNC SO THAT I CAN USE CROSSTAB FUNCTION IN ORDER TO CREATE A PIVOT TABLE)

CREATE EXTENSION IF NOT EXISTS tablefunc;



-- PIVOT DATA TO SEE THE COHORT TABLE AND THEN PASSING IT INTO A NEW TABLE cohort_pivot

SELECT DISTINCT cohort_date,
				cohort_index,
				COUNT( DISTINCT customer_id)
FROM cohort_retention
WHERE cohort_index > 0
GROUP BY cohort_date, cohort_index
ORDER BY 1, 2, 3


SELECT * 
INTO TABLE cohort_pivot
FROM
		(
		SELECT cohort_date,
		COALESCE("1", 0) AS "1",
		COALESCE("2", 0) AS "2",
		COALESCE("3", 0) AS "3",
		COALESCE("4", 0) AS "4",
		COALESCE("5", 0) AS "5",
		COALESCE("6", 0) AS "6",
		COALESCE("7", 0) AS "7",
		COALESCE("8", 0) AS "8",
		COALESCE("9", 0) AS "9",
		COALESCE("10", 0) AS "10",
		COALESCE("11", 0) AS "11",
		COALESCE("12", 0) AS "12",
		COALESCE("13", 0) AS "13"
		FROM CROSSTAB ('SELECT DISTINCT cohort_date,
							   cohort_index,
							   COUNT( DISTINCT customer_id)
						FROM cohort_retention
						WHERE cohort_index > 0
						GROUP BY cohort_date, cohort_index
						ORDER BY 1, 2, 3',
						'values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13)')
					AS RESULT(cohort_date date, "1" integer, "2" integer, "3" integer, "4" integer,
							  "5" integer, "6" integer, "7" integer, "8" integer, "9" integer,
							  "10" integer, "11" integer, "12" integer, "13" integer)

		) AS PIVOT



SELECT * FROM cohort_pivot;




-- (TRANSFERED THE DATA FROM THE PREVIOUS TABLE INTO PERCENTAGE)

SELECT cohort_date,
	   ROUND(CAST("1" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "1",
	   ROUND(CAST("2" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "2",
	   ROUND(CAST("3" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "3",
	   ROUND(CAST("4" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "4",
	   ROUND(CAST("5" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "5",
	   ROUND(CAST("6" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "6",
	   ROUND(CAST("7" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "7",
	   ROUND(CAST("8" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "8",
	   ROUND(CAST("9" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "9",
	   ROUND(CAST("10" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "10",
	   ROUND(CAST("11" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "11",
	   ROUND(CAST("12" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "12",
	   ROUND(CAST("13" AS NUMERIC)/CAST("1" AS NUMERIC), 2) * 100 AS "13"
FROM cohort_pivot












