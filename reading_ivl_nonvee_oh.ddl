# Cell 2: Create new session with Iceberg configs
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("create-table") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://aep-datalake-consume-dev/iceberg_catalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Cell 3: Drop and create table
spark.sql("DROP TABLE IF EXISTS iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh")

CREATE TABLE IF NOT EXISTS iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh(
    serialnumber STRING,
    source STRING,
    aep_devicecode STRING,
    isvirtual_meter STRING,
    timezoneoffset STRING,
    aep_premise_nb STRING,
    aep_service_point STRING,
    aep_mtr_install_ts STRING,
    aep_mtr_removal_ts STRING,
    aep_srvc_dlvry_id STRING,
    aep_comp_mtr_mltplr DOUBLE,
    name_register STRING,
    isvirtual_register STRING,
    toutier STRING,
    toutiername STRING,
    aep_srvc_qlty_idntfr STRING,
    aep_channel_id STRING,
    aep_raw_uom STRING,
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
    aep_endtime_utc STRING,
    aep_city STRING,
    aep_zip STRING,
    aep_state STRING,
    hdp_update_user STRING,
    hdp_insert_dttm TIMESTAMP,
    hdp_update_dttm TIMESTAMP,
    authority STRING,
    aep_derived_uom STRING,
    aep_opco STRING,
    aep_usage_dt STRING,
    aep_meter_bucket STRING
)
PARTITIONED BY (aep_usage_dt)
LOCATION 's3://aep-datalake-consume-dev/intervals/iceberg_catalog/usage_nonvee/reading_ivl_nonvee_oh'
TBLPROPERTIES (
    'table_type' = 'iceberg',
    'format-version' = '2',
    'format' = 'parquet',
    'write_compression' = 'snappy'
)





# Step 4: Verify
spark.sql("DESCRIBE TABLE iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh").show(truncate=False)
