#!/bin/env python3

import argparse, datetime
from etl_logger import ETLLogger

from pyspark.sql import SparkSession
from pyspark_llap import HiveWarehouseSession


# Track start time and run date
start_time = datetime.datetime.now()

# Main process variables
TARGET_TABLE = "PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD"
STAGING_TABLE = "PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP"
PROCESS_NAME = "TX_MART_CUSTOMER_ICONS_SCD"
TANGGAL_POSISI = None

# Argument parsing for date
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

# HWC
hwc = HiveWarehouseSession.session(spark).build()



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
    	, CASE WHEN row_today >= row_yesterday * 1.1 THEN 'Yes'
		WHEN row_today <= row_yesterday * 0.9 THEN 'Yes'
        		ELSE 'No'
	END AS is_anomaly
	FROM 
	(
    		SELECT
    		(SELECT COUNT(*) FROM {TARGET_TABLE} WHERE '{TANGGAL_POSISI}' BETWEEN START_DATE AND END_DATE) AS row_today,
    		(SELECT COUNT(*) FROM {TARGET_TABLE} WHERE date_add('{TANGGAL_POSISI}',-1) BETWEEN START_DATE AND END_DATE) AS row_yesterday
	) AS counts
    """

    metrics[
        "ROW COUNT FINAL VS ROW_COUNT CUSM"
    ] = f"""
	SELECT
    	'numeric' AS metric_flag
    	, RC_FINAL AS metric_numeric
    	, CAST(NULL AS STRING) AS sample_data
    	, CASE WHEN RC_FINAL = RC_CUSM then 'No' ELSE 'Yes' END AS is_anomaly
	 FROM 
	(
    		SELECT
    		(SELECT COUNT(*) FROM {TARGET_TABLE} WHERE '{TANGGAL_POSISI}' BETWEEN START_DATE AND END_DATE) AS RC_FINAL,
		(SELECT COUNT(*) FROM PRD_SRI_DATALAKE.SRI_101_111_CUSM 
			WHERE '{TANGGAL_POSISI}' BETWEEN START_DATE AND END_DATE 
			AND COALESCE(TRIM(ACCOUNT_SYSTEM), 'XXX') NOT IN ( 'GEN' , 'LON' , 'DEP' , 'CTA')
		) AS RC_CUSM
	) AS counts
    """

    metrics[
        "DUPLICATE CIF_KEY"
    ] = f"""
	SELECT
	'numeric' AS metric_flag
	,(check_duplicate + cif_key_null) AS metric_numeric
	,CAST(NULL AS STRING) AS sample_data
	,CASE WHEN check_duplicate > 0 THEN 'Yes' ELSE 'No' END AS is_anomaly
	FROM 
	(
		-- Count of duplicated cif_key (cif_key count > 1)
		SELECT 
		(	SELECT COUNT(*) 
         		FROM 
			(
				SELECT COUNT(*),CIF_KEY
				FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS
				WHERE AS_OF_DATE = CAST('{TANGGAL_POSISI}' AS DATE)
				GROUP BY CIF_KEY
				HAVING COUNT(*) > 1
			) A
		) AS check_duplicate,

        	-- Count of NULL ACCOUNTS
        	(
			SELECT COUNT(*)
         		FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS
         		WHERE AS_OF_DATE = CAST('{TANGGAL_POSISI}' AS DATE)
           	AND CIF_KEY IS NULL
		) AS cif_key_null

	) AAA
    """


    ### END OF METRICS DEFINITION ###

    # Test metrics
    etl_logger.test_metrics(metrics)


    # Test metrics
    etl_logger.test_metrics(metrics)

    # Set Hive/Tez configurations
    # Set Hive/Tez configurations
    hive_config_queries = [
        "SET hive.execution.engine=tez",
        "SET hive.auto.convert.join=true",
        "SET hive.mapjoin.smalltable.filesize=268435456",
        "SET hive.optimize.skewjoin=true",
        "SET tez.am.resource.memory.mb=32768",
        "SET hive.tez.container.size=32768",
        "SET tez.runtime.io.sort.mb=1024",
        "SET tez.runtime.unordered.output.buffer.size-mb=256",
        "SET hive.exec.reducers.bytes.per.reducer=256000000"
    ]

    for config_query in hive_config_queries:
        hwc.executeUpdate(config_query)

    # Update old data
    sc_update_query = f"""

  	UPDATE {TARGET_TABLE}
	SET END_DATE = DATE_ADD('{TANGGAL_POSISI}',-1)
	WHERE CIF_KEY IN 
	(
  		SELECT CIF_KEY 
  		FROM {STAGING_TABLE}
  		where label = 'UPDATED'
	)
	AND DATE_ADD('{TANGGAL_POSISI}',-1) BETWEEN START_DATE AND END_DATE

"""

    # Execute the MERGE update
    update_status = hwc.executeUpdate(sc_update_query)

    if update_status is None:
        raise RuntimeError("Failed to run update query! (executeUpdate returned None)")


    # Insert from STG
    sc_insert_query = f"""
INSERT INTO {TARGET_TABLE}
SELECT
cif_key
,cif_open_date
,open_branch_cd
,cust_type
,cust_status
,cust_notice_ind
,salutation_code
,salutation
,title_name
,first_name
,middle_name
,last_name
,company_name
,short_name
,id_type
,id_number
,sex_cd
,birth_date
,birth_place
,citizen_country_cd
,marital_cd
,mother_mdn_name
,cust_user_define_code
,no_of_dependents
,education_cd
,segment_cd
,industry_cd
,bus_sector_code
,religion_cd
,hobby_cd
,country_of_risk_cd
,ivr_flag
,language_flag
,property_ownership_cd
,relation_mgr_cd
,cust_tax_id
,grup_cd
,cust_worst_collectibility
,cust_email
,mail_indicator
,phone_no_res
,phone_no_bus
,fax_no
,hp_no
,home_add_street
,home_add_rtrw
,home_add_kelurahan
,home_add_kecamatan
,home_add_kota_kabupaten
,home_add_provinsi
,home_postcode
,home_city_code
,phone_no_res_ktp
,phone_no_bus_ktp
,fax_no_ktp
,hp_no_ktp
,home_add_street_ktp
,home_add_rtrw_ktp
,home_add_kelurahan_ktp
,home_add_kecamatan_ktp
,home_add_kota_kabupaten_ktp
,home_add_provinsi_ktp
,home_postcode_ktp
,home_city_code_ktp
,res_country_cd
,cust_income
,cust_income_period
,cust_rental_income
,cust_other_income
,cust_ext_deposit_amt
,cust_other_asset_amt
,corp_group_cd
,employer_name
,employed_from
,occupation_cd
,occupation_desc
,office_address1
,office_address2
,office_postcode
,id_expiry_date
,id_issue_date
,id_issue_place
,relation_mgr_name
,tfn_status
,tfn_ind
,domestic_risk
,cross_border_risk
,vip_code
,wtax_exempt
,tax_basis
,bi_owner_cd
,'{TANGGAL_POSISI}' as start_date
,'2999-12-31' as end_date
FROM {STAGING_TABLE}


    """

    # Execute insert
    insert_status = hwc.executeUpdate(sc_insert_query)

    if not insert_status:
        raise RuntimeError("Failed to run insert query!")

    # End time
    end_time = datetime.datetime.now()

    # Count rows inserted
    rows_inserted_count = hwc.sql(
        f"SELECT 1 FROM {TARGET_TABLE} WHERE START_DATE = to_date('{TANGGAL_POSISI}')"
    ).count()

    # Count rows updated
    rows_updated_count = hwc.sql(
        f"SELECT 1 FROM {TARGET_TABLE} WHERE END_DATE = date_add(to_date('{TANGGAL_POSISI}'), -1)"
    ).count()

    # Anomaly detection using calculated metrics
    etl_logger.calculate_metrics(TARGET_TABLE, metrics, TANGGAL_POSISI)

    # Log operation details
    etl_logger.log_success(
        TARGET_TABLE,
        start_time,
        end_time,
        TANGGAL_POSISI,
        rows_inserted_count,
        rows_updated_count,
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
