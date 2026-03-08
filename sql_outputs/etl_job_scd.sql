INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD
SELECT 
    'numeric' AS metric_flag,
    row_today AS metric_numeric,
    CAST(NULL AS STRING) AS sample_data,
    CASE WHEN row_today >= row_yesterday * 1.1 THEN 'Yes'
         WHEN row_today <= row_yesterday * 0.9 THEN 'Yes'
         ELSE 'No'
    END AS is_anomaly
FROM 
(
    SELECT
    (SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD WHERE '2023-10-05' BETWEEN START_DATE AND END_DATE) AS row_today,
    (SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD WHERE date_add('2023-10-05',-1) BETWEEN START_DATE AND END_DATE) AS row_yesterday
) AS counts;

INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD
SELECT 
    'numeric' AS metric_flag,
    RC_FINAL AS metric_numeric,
    CAST(NULL AS STRING) AS sample_data,
    CASE WHEN RC_FINAL = RC_CUSM THEN 'No' ELSE 'Yes' END AS is_anomaly
FROM 
(
    SELECT
    (SELECT COUNT(*) FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD WHERE '2023-10-05' BETWEEN START_DATE AND END_DATE) AS RC_FINAL,
    (SELECT COUNT(*) FROM PRD_SRI_DATALAKE.SRI_101_111_CUSM 
     WHERE '2023-10-05' BETWEEN START_DATE AND END_DATE 
     AND COALESCE(TRIM(ACCOUNT_SYSTEM), 'XXX') NOT IN ( 'GEN' , 'LON' , 'DEP' , 'CTA')
    ) AS RC_CUSM
) AS counts;

INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD
SELECT
    'numeric' AS metric_flag,
    (check_duplicate + cif_key_null) AS metric_numeric,
    CAST(NULL AS STRING) AS sample_data,
    CASE WHEN check_duplicate > 0 THEN 'Yes' ELSE 'No' END AS is_anomaly
FROM 
(
    -- Count of duplicated cif_key (cif_key count > 1)
    SELECT 
    (SELECT COUNT(*) 
     FROM 
     (
         SELECT COUNT(*),CIF_KEY
         FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS
         WHERE AS_OF_DATE = CAST('2023-10-05' AS DATE)
         GROUP BY CIF_KEY
         HAVING COUNT(*) > 1
     ) A
    ) AS check_duplicate,

    -- Count of NULL ACCOUNTS
    (
        SELECT COUNT(*)
        FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS
        WHERE AS_OF_DATE = CAST('2023-10-05' AS DATE)
        AND CIF_KEY IS NULL
    ) AS cif_key_null

) AAA;

UPDATE PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD
SET END_DATE = DATE_ADD('2023-10-05',-1)
WHERE CIF_KEY IN 
(
    SELECT CIF_KEY 
    FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP
    WHERE label = 'UPDATED'
)
AND DATE_ADD('2023-10-05',-1) BETWEEN START_DATE AND END_DATE;

INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD
SELECT
    cif_key,
    cif_open_date,
    open_branch_cd,
    cust_type,
    cust_status,
    cust_notice_ind,
    salutation_code,
    salutation,
    title_name,
    first_name,
    middle_name,
    last_name,
    company_name,
    short_name,
    id_type,
    id_number,
    sex_cd,
    birth_date,
    birth_place,
    citizen_country_cd,
    marital_cd,
    mother_mdn_name,
    cust_user_define_code,
    no_of_dependents,
    education_cd,
    segment_cd,
    industry_cd,
    bus_sector_code,
    religion_cd,
    hobby_cd,
    country_of_risk_cd,
    ivr_flag,
    language_flag,
    property_ownership_cd,
    relation_mgr_cd,
    cust_tax_id,
    grup_cd,
    cust_worst_collectibility,
    cust_email,
    mail_indicator,
    phone_no_res,
    phone_no_bus,
    fax_no,
    hp_no,
    home_add_street,
    home_add_rtrw,
    home_add_kelurahan,
    home_add_kecamatan,
    home_add_kota_kabupaten,
    home_add_provinsi,
    home_postcode,
    home_city_code,
    phone_no_res_ktp,
    phone_no_bus_ktp,
    fax_no_ktp,
    hp_no_ktp,
    home_add_street_ktp,
    home_add_rtrw_ktp,
    home_add_kelurahan_ktp,
    home_add_kecamatan_ktp,
    home_add_kota_kabupaten_ktp,
    home_add_provinsi_ktp,
    home_postcode_ktp,
    home_city_code_ktp,
    res_country_cd,
    cust_income,
    cust_income_period,
    cust_rental_income,
    cust_other_income,
    cust_ext_deposit_amt,
    cust_other_asset_amt,
    corp_group_cd,
    employer_name,
    employed_from,
    occupation_cd,
    occupation_desc,
    office_address1,
    office_address2,
    office_postcode,
    id_expiry_date,
    id_issue_date,
    id_issue_place,
    relation_mgr_name,
    tfn_status,
    tfn_ind,
    domestic_risk,
    cross_border_risk,
    vip_code,
    wtax_exempt,
    tax_basis,
    bi_owner_cd,
    '2023-10-05' as start_date,
    '2999-12-31' as end_date
FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP;
