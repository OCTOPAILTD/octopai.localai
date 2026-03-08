#!/bin/env python3

import argparse, datetime
from etl_logger import ETLLogger

from pyspark.sql import SparkSession

# Track start time and run date
start_time = datetime.datetime.now()

# Main process variables
TARGET_TABLE = "PRD_ACL_DATALAKE.MASTER_CTA"
PROCESS_NAME = "TX_MART_MASTER_CTA"
TANGGAL_POSISI = None

# Retention & housekeeping, only accept DATE data type for partition date column
PARTITION_DATE_COLUMN_NAME = "AS_OF_DATE"  # Partition date column
RETENTION = 0  # Retention in days, 0 means disable housekeeping

# List of comma separated hardcoded partitions, leave blank or None to disable
# Examples:
# OTHER_PARTITIONS_TO_DROP = ""  # Disable additional hardcoded partitions
# OTHER_PARTITIONS_TO_DROP = None  # Disable additional hardcoded partitions
# OTHER_PARTITIONS_TO_DROP = "flag='DPK', process_name='TX_MART_LOAN', counter=1"
OTHER_PARTITIONS_TO_DROP = ""

# Parse parameter tanggal_posisi
parser = argparse.ArgumentParser()
parser.add_argument("--tanggal_posisi", default=None, help="Parameter tanggal_posisi.")
args = parser.parse_args()
if args.tanggal_posisi is None:
    raise RuntimeError("tanggal_posisi is undefined.")

# Convert string tanggal_posisi ke date karena etl_logger butuh date object
try:
    TANGGAL_POSISI = datetime.datetime.strptime(args.tanggal_posisi, "%Y-%m-%d").date()
except ValueError as e:
    raise RuntimeError("Format parameter tanggal_posisi salah.")

# Build Spark session
spark = SparkSession.builder.appName(PROCESS_NAME).enableHiveSupport().getOrCreate()

# spark hive configuration
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("hive.enforce.bucketing", "false")
spark.conf.set("hive.enforce.sorting", "false")
spark.conf.set("mapred.input.dir.recursive","true") 
spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")


# Instantiate ETLLogger
etl_logger = ETLLogger(spark, PROCESS_NAME)

# Disable Excessive Logging
etl_logger.set_log_level("org", "WARN")
etl_logger.set_log_level("com", "WARN")
etl_logger.set_log_level(PROCESS_NAME, "INFO")

try:
    ### DEFINE METRICS BELOW ###

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
    		(SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MASTER_CTA WHERE AS_OF_DATE = date_add('{TANGGAL_POSISI}',-1)) AS row_yesterday
	) AS counts
    """



     # Check Duplicate
    metrics[
        "DUPLICATE ACCOUNT_NUMBER"
    ] = f"""
	SELECT
    'numeric' AS metric_flag,
    check_duplicate AS metric_numeric,
    CAST(NULL AS STRING) AS sample_data,
    CASE 
        WHEN check_duplicate IS NULL THEN 'Yes'
        ELSE 'No'
    END AS is_anomaly
FROM 
(
    SELECT 
        -- Count of duplicated ACCOUNTS (ID_NUMBER count > 1)
        COUNT(*) AS check_duplicate
    FROM 
    (
        SELECT ID_NUMBER
        FROM PRD_ACL_DATALAKE.MASTER_CTA
        WHERE AS_OF_DATE = CAST('{TANGGAL_POSISI}' AS DATE)
        GROUP BY ID_NUMBER
        HAVING COUNT(*) > 1
    ) A
) counts
    """

    ### END OF METRICS DEFINITION ###

    # Test metrics
    etl_logger.test_metrics(metrics)

    # Define SQL query for reading data from Hive
    sc_read_query = f"""

SELECT 
CTA.ID_NUMBER
,CTA.CIF_KEY
,CUST.CUSTOMER_NAME
,CUST.CIF_OPEN_BRANCH_CODE
,CUST.CIF_OPEN_BRANCH
,CUST.CUST_TYPE_CODE
,CUST.CIF_OPEN_REGION_CODE
,CUST.CIF_OPEN_REGION
,CUST.SEGMENT_DIV_OWNER
,CUST.SUB_SEGMENT
,CUST.DIR_OWNER
,CTA.ACCOUNT_TYPE
,CTA.SUB_CATEGORY
,CTA.PRODUCT_NAME
,CTA.CURRENCY
,CTA.BRANCH_CODE
,CTA.SUBBRANCH_CODE
,CTA.ACCT_APPROVAL_DATE
,CTA.ACCT_OPEN_DATE
,CTA.ACCT_EXPIRY_DATE
,CTA.GL_CLASS_CODE
,CTA.GL_ACCOUNT_ID
,CTA.MARKET_SEGMENT_CD
,CTA.ACCOUNT_STATUS
,CTA.RATE
,CTA.APPROVAL_AMT
,CTA.APPROVAL_AMT_IDR
,CTA.CUR_BOOK_BAL
,CTA.CUR_BOOK_BAL_IDR
,CTA.NO_PK
,CTA.ADM_FEE
,CTA.ID_BANK_GARANSI
,CTA.JAMINAN
,CHANNEL.CHANNEL_OWNER_L1
,CHANNEL.CHANNEL_TYPE_L1
,CHANNEL.CHANNEL_OWNER_L2
,CHANNEL.CHANNEL_TYPE_L2
,CHANNEL.CHANNEL_OWNER_L3
,CHANNEL.CHANNEL_TYPE_L3
,CHANNEL.CHANNEL_OWNER_MIRROR
,CTA.AS_OF_DATE

FROM ACL_DATALAKE.MART_CTA CTA


LEFT JOIN DM_CUSTOMER.CUSTOMER_ICONS CUST
ON CTA.CIF_KEY = CUST.CIF_KEY
AND CUST.AS_OF_DATE = '{TANGGAL_POSISI}'

LEFT JOIN (
		SELECT
			*
		FROM 
		(
			SELECT 
				CHANNEL.*
				, ROW_NUMBER() OVER(PARTITION BY ID_NUMBER ORDER BY AS_OF_DATE DESC) RN
			FROM 
			(
				SELECT DISTINCT 
					CIF_KEY
					, ID_NUMBER
					, SEGMENT_DIV_OWNER
					, CHANNEL_OWNER_L1
					, CHANNEL_TYPE_L1
					, CHANNEL_OWNER_L2
					, CHANNEL_TYPE_L2
					, CHANNEL_OWNER_L3
					, CHANNEL_TYPE_L3
					, CHANNEL_OWNER_MIRROR
					, AS_OF_DATE
				FROM acl_datalake.mart_master_channel_baseline 
				UNION ALL
				SELECT 
					CIF_KEY
					, ID_NUMBER
					, SEGMENT_DIV_OWNER
					, CHANNEL_OWNER_L1
					, CHANNEL_TYPE_L1
					, CHANNEL_OWNER_L2
					, CHANNEL_TYPE_L2
					, CHANNEL_OWNER_L3
					, CHANNEL_TYPE_L3
					, CHANNEL_OWNER_MIRROR
					, AS_OF_DATE
				FROM acl_datalake.mart_master_channel_nwow_daily 
				--WHERE as_of_date  = '{TANGGAL_POSISI}'
			) AS CHANNEL
		) AS CHANNEL
		WHERE RN = 1
) AS CHANNEL
ON CTA.ID_NUMBER = CHANNEL.ID_NUMBER
AND CTA.AS_OF_DATE = CHANNEL.AS_OF_DATE




WHERE CTA.AS_OF_DATE = '{TANGGAL_POSISI}'

    """

    # Log query
    etl_logger.logger.debug(sc_read_query)

    # Query to write data back to Hive
    write_query = (
        f"INSERT INTO {TARGET_TABLE} PARTITION (as_of_date) SELECT * FROM temp_table"
    )

    # Read data from Hive
    rpt_df = spark.sql(sc_read_query)
    etl_logger.logger.info("Successfully completed read_query.")

    # Create a temporary table to insert the data
    rpt_df.createOrReplaceTempView("temp_table")
    rpt_df = rpt_df.coalesce(10)

    # Write the processed data back to the Hive table
    spark.sql(write_query)
    etl_logger.logger.info("Successfully completed write_query.")

    # End time
    end_time = datetime.datetime.now()

    # Count rows inserted
    rows_inserted_count = spark.sql(
        f"SELECT 1 FROM {TARGET_TABLE} WHERE as_of_date = TO_DATE('{TANGGAL_POSISI}')"
    ).count()

    # Anomaly detection using calculated metrics
    etl_logger.calculate_metrics(TARGET_TABLE, metrics, TANGGAL_POSISI)

    # Housekeeping
    etl_logger.perform_housekeeping(
        TARGET_TABLE,
        PARTITION_DATE_COLUMN_NAME,
        TANGGAL_POSISI,
        RETENTION,
        OTHER_PARTITIONS_TO_DROP,
    )

    # Log operation details
    etl_logger.log_success(
        TARGET_TABLE, start_time, end_time, TANGGAL_POSISI, rows_inserted_count
    )

    etl_logger.logger.info("Successfully logged operation details.")

except Exception as e:
    # Log any errors encountered during the ETL process
    etl_logger.log_error(str(e), TANGGAL_POSISI)
    etl_logger.logger.error(f"Error occurred: {str(e)}")
    raise RuntimeError(str(e)) from e

finally:
    # Close Spark session and ETL logger
    etl_logger.close()
