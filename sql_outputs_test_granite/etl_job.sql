INSERT INTO "s3a://dx_dl_comcast_marts/output/target_table"
SELECT 
    a.customer_id,
    a.status,
    b.order_amount,
    SUM(b.amount) OVER (PARTITION BY a.customer_id) AS total_amount
FROM #temp_view a
LEFT JOIN hive_metastore.schema.table2 b 
  ON a.id = b.customer_id
WHERE a.as_of_date = '2023-01-01'

Validation steps:

Before producing the final answer, verify that every INSERT INTO target corresponds to a real write operation in the code.
If a name appears only on the left side of a DataFrame assignment and never in a write API, it is an error to emit INSERT INTO for it. Dataframe creation or assignment is not a write opperation.

INSERT INTO "s3a://dx_dl_comcast_marts/output/target_table"
SELECT 
    a.customer_id,
    a.status,
    b.order_amount,
    SUM(b.amount) OVER (PARTITION BY a.customer_id) AS total_amount
FROM #temp_view a
LEFT JOIN hive_metastore.schema.table2 b 
  ON a.id = b.customer_id
WHERE a.as_of_date = '2023-01-01'

# Define metrics
metrics = {}

# Compare Row today vs Row yesterday
metrics[
    "ROW_COUNT"
] = f"""
	SELECT
    	'numeric' AS metric_flag
    	, row_today AS metric_numeric
    	, CAST(NULL AS STRING) AS sample_data
    	,CASE WHEN row_today >= row_yesterday * 1.05 THEN 'Yes'
		WHEN row_today <= row_yesterday * 0.95 THEN 'Yes'
        		ELSE 'No'
	END AS is_anomaly
	FROM 
	(
    		SELECT
    		(SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = '{TANGGAL_POSISI}') AS row_today,
    		(SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_
