# Step 1: Configure Iceberg catalog
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.iceberg_catalog.warehouse", "s3://aep-datalake-consume-dev/iceberg_catalog")

# Step 2: Drop the corrupted table
spark.sql("DROP TABLE IF EXISTS iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh")

# Step 3: Create the table
spark.sql("""
CREATE TABLE iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh (
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
LOCATION 's3://aep-datalake-consume-dev/intervals/iceberg_catalog/usage_nonvee/reading_ivl_nonvee_oh'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'parquet.compress' = 'SNAPPY'
)
""")

# Step 4: Verify
spark.sql("DESCRIBE TABLE iceberg_catalog.usage_nonvee.reading_ivl_nonvee_oh").show(truncate=False)
