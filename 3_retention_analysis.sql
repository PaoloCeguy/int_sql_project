WITH customer_last_purchase as(
SELECT  
	customerkey,
	cleaned_name,
	orderdate,
	ROW_number() OVER (PARTITION BY customerkey order BY orderdate desc) AS rn,
	first_purchase_date,
	cohort_year
FROM
	cohort_analysis
),

churned_customers AS (
SELECT  
	customerkey,
	cleaned_name,
	orderdate AS last_purchase_date,
	CASE 
		WHEN orderdate < (SELECT MAX (orderdate) FROM sales)  - INTERVAL '6 month' THEN 'Churned'
		ELSE 'active'
	END AS customer_status,
	cohort_year
FROM customer_last_purchase
WHERE rn = 1
	AND first_purchase_date < (SELECT MAX (orderdate) FROM sales)  - INTERVAL '6 months'
)

SELECT
	customer_status,
	cohort_year,
	COUNT (customerkey) AS num_customers,
	SUM (COUNT ( customerkey ) ) OVER (PARTITION BY cohort_year ) AS total_customers,
	ROUND (COUNT (customerkey ) / SUM (COUNT (customerkey) ) OVER (PARTITION BY cohort_year), 2) AS status_percentage
FROM churned_customers
GROUP BY customer_status, cohort_year