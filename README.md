{
"claudeCode.environmentVariables": [
         {"name": "AWS_PROFILE", "value": "aedeon-bedrock"},
         {"name": "AWS_REGION", "value": "us-west-2"},
         {"name": "ANTHROPIC_MODEL", "value": "arn:aws:bedrock:us-west-2:110325908427:inference-profile/global.anthropic.claude-opus-4-6-v1"},
         {"name": "CLAUDE_CODE_USE_BEDROCK", "value": "1"},
         {"name": "ANTHROPIC_MODEL", "value": "global.anthropic.claude-opus-4-6-v1"}
    ],
    "claudeCode.preferredLocation": "panel",
    "claudeCode.selectedModel": "global.anthropic.claude-opus-4-6-v1",}


spark-submit --deploy-mode client --packages com.databricks:spark-xml_2.12:0.18.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=glue --conf spark.sql.catalog.spark_catalog.glue.id=889415020100 --conf spark.sql.catalog.spark_catalog.warehouse=s3://aep-datalake-work-dev/test/warehouse --driver-cores 2 --driver-memory 10g --conf spark.driver.maxResultSize=5g --executor-cores 1 --executor-memory 5g /root/nonvee_uiq_info_job.py --job_name uiq-nonvee-info --aws_env dev --opco oh --work_bucket aep-datalake-work-dev --archive_bucket aep-datalake-raw-dev --batch_start_dttm_str "2024-12-01 00:00:00" --batch_end_dttm_str "2024-12-05 00:00:00"

spark-submit --deploy-mode client --packages com.databricks:spark-xml_2.12:0.18.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=glue --conf spark.sql.catalog.spark_catalog.glue.id=889415020100 --conf spark.sql.catalog.spark_catalog.warehouse=s3://aep-datalake-work-dev/test/warehouse --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_catalog.type=glue --conf spark.sql.catalog.iceberg_catalog.glue.id=889415020100 --conf spark.sql.catalog.iceberg_catalog.warehouse=s3://aep-datalake-work-dev/test/warehouse --conf spark.sql.session.timeZone="America/New_York" --conf spark.driver.extraJavaOptions="-Duser.timezone=America/New_York" --conf spark.executor.extraJavaOptions="-Duser.timezone=America/New_York" --conf spark.sql.legacy.timeParserPolicy=LEGACY --driver-cores 2 --driver-memory 10g --conf spark.driver.maxResultSize=5g --executor-cores 1 --executor-memory 5g nonvee_uiq_info_job.py --job_name uiq-nonvee-info --aws_env dev --opco oh --work_bucket aep-datalake-work-dev --archive_bucket aep-datalake-raw-dev --batch_start_dttm_str "2024-10-25 00:00:00" --batch_end_dttm_str "2024-10-26 00:00:00" --skip_archive true




create_table_sql = f"""
CREATE TABLE IF NOT EXISTS iceberg_catalog.usage_nonvee.reading_ivl_nonvee_{opco} (
    serialnumber STRING,
    source STRING,
    aep_devicecode STRING,
    isvirtual_meter STRING,
    timezoneoffset STRING,
    aep_premise_nb STRING,
    aep_service_point STRING,
    aep_srvc_dlvry_id STRING,
    name_register STRING,
    isvirtual_register STRING,
    toutier STRING,
    toutiername STRING,
    aep_derived_uom STRING,
    aep_raw_uom STRING,
    aep_srvc_qlty_idntfr STRING,
    aep_channel_id STRING,
    aep_sec_per_intrvl DOUBLE,
    aep_meter_alias STRING,
    aep_meter_program STRING,
    aep_billable_ind STRING,
    aep_usage_type STRING,
    aep_timezone_cd STRING,
    endtimeperiod STRING,
    starttimeperiod STRING,
    value FLOAT,
    aep_raw_value FLOAT,
    scalarfloat FLOAT,
    aep_data_quality_cd STRING,
    aep_data_validation STRING,
    aep_acct_cls_cd STRING,
    aep_acct_type_cd STRING,
    aep_mtr_pnt_nb STRING,
    aep_tarf_pnt_nb STRING,
    aep_comp_mtr_mltplr DOUBLE,
    aep_endtime_utc STRING,
    aep_mtr_removal_ts STRING,
    aep_mtr_install_ts STRING,
    aep_city STRING,
    aep_zip STRING,
    aep_state STRING,
    hdp_update_user STRING,
    hdp_insert_dttm TIMESTAMP,
    hdp_update_dttm TIMESTAMP,
    authority STRING
)
USING iceberg
PARTITIONED BY (aep_opco, aep_usage_dt, aep_meter_bucket)
LOCATION 's3://aep-datalake-consume-dev/intervals/iceberg_catalog/usage_nonvee/reading_ivl_nonvee_{opco}'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'parquet.compress' = 'SNAPPY'
)
"""
spark.sql(create_table_sql)
