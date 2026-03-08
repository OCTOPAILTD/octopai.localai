-- Read data from Hive
INSERT INTO #temp_table SELECT * FROM ACL_DATALAKE.MART_CTA_CTA

-- Write the processed data back to the Hive table
INSERT INTO PRD_ACL_DATALAKE.MASTER_CTA PARTITION (as_of_date) SELECT * FROM #temp_table

-- Calculate metrics
INSERT INTO #metrics SELECT 'numeric' AS metric_flag, row_today AS metric_numeric, CAST(NULL AS STRING) AS sample_data, CASE WHEN row_today >= row_yesterday * 1.05 THEN 'Yes' WHEN row_today <= row_yesterday * 0.95 THEN 'Yes' ELSE 'No' END AS is_anomaly FROM ( SELECT (SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = '{TANGGAL_POSISI}') AS row_today, (SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = date_add('{TANGGAL_POSISI}',-1)) AS row_yesterday ) AS counts

-- Check Duplicate
INSERT INTO #metrics SELECT 'numeric' AS metric_flag, check_duplicate AS metric_numeric, CAST(NULL AS STRING) AS sample_data, CASE WHEN check_duplicate IS NULL THEN 'Yes' ELSE 'No' END AS is_anomaly FROM ( SELECT COUNT(*) AS check_duplicate FROM ( SELECT ID_NUMBER FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = CAST('{TANGGAL_POSISI}' AS DATE) GROUP BY ID_NUMBER HAVING COUNT(*) > 1 ) A ) counts

-- Perform housekeeping
INSERT INTO #housekeeping SELECT * FROM ( SELECT * FROM acl_datalake.mart_master_channel_baseline UNION ALL SELECT * FROM acl_datalake.mart_master_channel_nwow_daily ) AS CHANNEL WHERE AS_OF_DATE = '{TANGGAL_POSISI}' AND ID_NUMBER IN ( SELECT ID_NUMBER FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = '{TANGGAL_POSISI}' )

-- Log operation details
INSERT INTO #logs SELECT 'operation' AS operation, 'success' AS status, '{TANGGAL_POSISI}' AS date, '{start_time}' AS start_time, '{end_time}' AS end_time, '{rows_inserted_count}' AS rows_inserted_count FROM #temp_table
