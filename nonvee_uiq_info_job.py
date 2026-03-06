"""
spark-submit \
  --deploy-mode client \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=glue \
  --conf spark.sql.catalog.spark_catalog.glue.id=889415020100 \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3://aep-datalake-work-dev/test/warehouse \
  --driver-cores 2 \
  --driver-memory 10g \
  --conf spark.driver.maxResultSize=5g \
  --executor-cores 1 \
  --executor-memory 5g
"""

import time
import json
import uuid
import boto3
import argparse

from typing import Dict
from functools import reduce
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.window import Window

parms_prefix = "aws_analytics_applications/aep_dl_util_ami_nonvee_info/parms"
work_prefix = "raw"
archive_prefix = "util"
data_files_dir = "intervals/nonvee/uiq_info"

marker_file_name = "index.txt"
marker_file_max_depth = 2
data_file_ext = ".xml"
data_file_min_size = 0

xml_row_tag = "MeterData"
xml_read_mode = "PERMISSIVE"
xml_infer_schema = "true"
xml_tag_ignore_namespace = "true"
xml_data_timezone = "America/New_York"
xml_tag_value_col_name = "_VALUE"
xml_attribute_prefix = "_"                
xml_null_value = ""                     

# Reference: stg_nonvee.interval_data_files_{opco}_src.ddl
# DDL Line 10: interval_reading array<struct<endtime,blocksequencenumber,
#              gatewaycollectedtime,intervalsequencenumber,
#              `interval`:array<struct<channel,rawvalue,value,uom,blockendvalue
info_xml_schema = StructType([
    StructField("_MeterName", StringType(), True),
    StructField("_UtilDeviceID", StringType(), True),
    StructField("_MacID", StringType(), True),
    
    StructField("IntervalReadData", StructType([
        StructField("_IntervalLength", StringType(), True),
        StructField("_StartTime", StringType(), True),
        StructField("_EndTime", StringType(), True),
        StructField("_NumberIntervals", StringType(), True),
        
        StructField("Interval", ArrayType(
            StructType([
                StructField("_EndTime", StringType(), True),
                StructField("_BlockSequenceNumber", StringType(), True),
                StructField("_GatewayCollectedTime", StringType(), True),
                StructField("_IntervalSequenceNumber", StringType(), True),
                
                StructField("Reading", ArrayType(
                    StructType([
                        StructField("_Channel", StringType(), True),
                        StructField("_RawValue", StringType(), True),
                        StructField("_Value", StringType(), True),
                        StructField("_UOM", StringType(), True),
                        StructField("_BlockEndValue", StringType(), True)
                    ]), True)
                )
            ]), True)
        )
    ]), True)
])

# Reference: stg_nonvee.interval_data_files_{opco}_src_vw
# Source: scripts/ddl/stg_nonvee.interval_data_files_{opco}_src_vw.ddl
# Called by: scripts/source_interval_data_files.sh
# UOM Mapping Reference Table
uom_data = [
    ("1", "KWH", "kwh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("2", "KW", "kw", "kW_del_mtr_ivl_len_min", "DEMAND", "N"),
    ("3", "KVARH", "kvarh", "kVArh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("4", "KVAR", "kvar", "kVAr_del_mtr_ivl_len_min", "DEMAND", "N"),
    ("5", "KVAH", "kvah", "kVAh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("100", "KWH", "wh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "Y"),
    ("101", "KWH", "kwh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("102", "KWH", "kwh(rec)", "kWh_rec_mtr_ivl_len_min", "INTERVAL", "N"),
    ("108", "KVARH", "kvarh", "kVArh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("109", "KVAH", "kvah", "kVAh_del_mtr_ivl_len_min", "INTERVAL", "N"),
]

uom_schema = StructType([
    StructField("aep_channel_id", StringType()),
    StructField("aep_derived_uom", StringType()),
    StructField("aep_raw_uom", StringType()),
    StructField("name_register", StringType()),
    StructField("aep_srvc_qlty_idntfr", StringType()),
    StructField("value_mltplr_flg", StringType()),
])

# OPCO to Company Code mapping (for MACS filter)
OPCO_TO_CO_CD = {
    'oh':  ['07', '10'],
    'im':  ['04'],
    'pso': ['95'],
    'tx':  ['94', '97'],
    'ap':  ['01', '02', '06'],
    'swp': ['96']
}

def get_s3_file_contents(
    s3_bucket: str,
    s3_key: str,
) -> str:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    contents = response["Body"].read().decode("utf-8")
    return contents


def get_filtered_data_files_map(
    s3_bucket: str,
    s3_prefix: str,
    marker_file_name: str,
    batch_start_dttm_ltz: datetime,
    batch_end_dttm_ltz: datetime,
    marker_file_max_depth: int,
    data_file_min_size: int,
    data_file_ext: str,
) -> Dict:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    base_prefix_len = len(s3_prefix.strip("/"))

    _time = time.time()
    print(f'===== listing objects in base prefix {s3_prefix} =====')
    i = 0
    j = 0
    k = 0
    all_files_map = defaultdict(list)
    filtered_dirs_prefix = set()
    filtered_files_map = None

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        i += 1
        for obj in page.get('Contents', []):
            j += 1
            obj_key = obj['Key']
            obj_size = obj['Size']
            obj_last_modified = obj['LastModified']
            relative = obj_key[base_prefix_len:]
            relative_depth = relative.count("/")
            level1_dir = relative.strip("/").split("/")[0]
            level1_dir_prefix = f'{s3_prefix}/{level1_dir}'
            file_name = relative.split("/")[-1]

            if (
                obj_key.endswith(f'/{marker_file_name}')
                and obj_last_modified >= batch_start_dttm_ltz
                and obj_last_modified < batch_end_dttm_ltz
                and relative_depth <= marker_file_max_depth
            ):
                k += 1
                filtered_dirs_prefix.add(level1_dir_prefix)
            elif obj_size > data_file_min_size and data_file_ext in file_name:
                all_files_map[level1_dir_prefix].append(obj_key)
    print(f'===== found {j} objects in {i} pages =====')
    print(f'completed listing objects in {str(timedelta(seconds=time.time() - _time))}')

    _time = time.time()
    print(f'===== filtering files =====')
    filtered_files_map = {prefix: all_files_map[prefix] for prefix in filtered_dirs_prefix}
    print(f'===== selected {sum(len(f) for f in filtered_files_map.values())} files in {len(filtered_files_map)} dirs =====')
    print(f'completed filtering files in {str(timedelta(seconds=time.time() - _time))}')

    return filtered_files_map

def main():
    print(f'===== starting job =====')
    _bgn_time = time.time()

    _time = time.time()
    print(f'===== started parsing job args =====')
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--aws_env", required=True)
    parser.add_argument("--opco", required=True)
    parser.add_argument("--work_bucket", required=True)
    parser.add_argument("--archive_bucket", required=True)
    parser.add_argument("--batch_start_dttm_str", required=False, default=None)
    parser.add_argument("--batch_end_dttm_str", required=False, default=None)
    parser.add_argument("--skip_archive", required=False, default=None)
    args = parser.parse_args()
    print(f'completed parsing in {str(timedelta(seconds=time.time() - _time))}')
    print(f'===== args [{type(args)}]: {args} =====')

    print(f'===== init vars =====')
    job_name = args.job_name
    aws_env = args.aws_env
    opco = args.opco
    work_bucket = args.work_bucket
    archive_bucket = args.archive_bucket
    batch_start_dttm_str = args.batch_start_dttm_str
    batch_end_dttm_str = args.batch_end_dttm_str
    skip_archive = args.skip_archive

    # # hard-coded for development
    # job_name = "uiq-nonvee-info"
    # aws_env = "dev"
    # opco = "{opco}"
    # work_bucket = f'aep-datalake-work-{aws_env}'
    # archive_bucket = f'aep-datalake-raw-{aws_env}'
    # batch_start_dttm_str = None
    # batch_end_dttm_str = None
    # skip_archive = None

    _time = time.time()
    print(f'===== creating spark session =====')
    spark = SparkSession.builder \
        .appName(job_name) \
        .enableHiveSupport() \
        .getOrCreate()
    print(f'completed creating spark session in {str(timedelta(seconds=time.time() - _time))}')
    print(f'===== spark application-id: {spark.sparkContext.applicationId} =====')

    current_dttm_ltz = datetime.now(timezone.utc).astimezone()
    local_tz = current_dttm_ltz.tzinfo
    if batch_start_dttm_str:
        batch_start_dttm_ntz = datetime.strptime(batch_start_dttm_str, "%Y-%m-%d %H:%M:%S")
        batch_start_dttm_ltz = batch_start_dttm_ntz.replace(tzinfo=local_tz)
    else:
        last_process_dttm_file = f'info_last_proc_dt_do_not_remove_{opco}.txt'
        last_process_dttm_key = f'{parms_prefix}/{last_process_dttm_file}'
        last_process_dttm_str = get_s3_file_contents(s3_bucket=work_bucket, s3_key=last_process_dttm_key).strip()
        last_process_dttm_ntz = datetime.strptime(last_process_dttm_str, "%Y-%m-%d %H:%M:%S")
        last_process_dttm_ltz = last_process_dttm_ntz.replace(tzinfo=local_tz)
        batch_start_dttm_ltz = last_process_dttm_ltz
    if batch_end_dttm_str:
        batch_end_dttm_ntz = datetime.strptime(batch_end_dttm_str, "%Y-%m-%d %H:%M:%S")
        batch_end_dttm_ltz = batch_end_dttm_ntz.replace(tzinfo=local_tz)
    else:
        batch_end_dttm_ltz = current_dttm_ltz
    base_prefix = f'{work_prefix}/{data_files_dir}/{opco}'
    scratch_dir = "temp-nb"
    scratch_path = f'hdfs:///tmp/{scratch_dir}/{str(uuid.uuid4())}'
    print(f'===== batch start dttm: {batch_start_dttm_ltz} =====')
    print(f'===== batch end dttm: {batch_end_dttm_ltz} =====')
    print(f'===== batch base prefix: {base_prefix} =====')
    print(f'===== batch scratch path: {scratch_path} =====')

    filtered_data_files_map = get_filtered_data_files_map(
        s3_bucket=work_bucket,
        s3_prefix=base_prefix,
        marker_file_name=marker_file_name,
        batch_start_dttm_ltz=batch_start_dttm_ltz,
        batch_end_dttm_ltz=batch_end_dttm_ltz,
        marker_file_max_depth=marker_file_max_depth,
        data_file_min_size=data_file_min_size,
        data_file_ext=data_file_ext,
    )

    _time = time.time()
    print(f'===== reading files =====')
    files_count = 0
    dfs = []
    for k, files in filtered_data_files_map.items():
        files_count += len(files)
        files_str = ",".join([f's3://{work_bucket}/{f}' for f in files])
        df = spark.read.format("com.databricks.spark.xml") \
            .option("rowTag", xml_row_tag) \
            .option("mode", xml_read_mode) \
            .option("inferSchema", xml_infer_schema) \
            .option("valueTag", xml_tag_value_col_name) \
            .option("ignoreNamespace", xml_tag_ignore_namespace) \
            .option("timezone", xml_data_timezone) \
            .option("attributePrefix", xml_attribute_prefix) \
            .option("nullValue", xml_null_value) \
            .schema(info_xml_schema) \
            .load(files_str)
        dfs.append(df)
    print(f'===== read {files_count} files in {len(dfs)} dirs =====')
    df_raw = reduce(lambda a, b: a.union(b), dfs)
    df_raw.printSchema()
    print(f'completed reading in {str(timedelta(seconds=time.time() - _time))}')

    _time = time.time()
    print(f'===== staging raw data =====')
    raw_staging_path = f'{scratch_path}/raw_stage/'
    df_raw.coalesce(1000).write.mode("overwrite").parquet(raw_staging_path)
    print(f'completed staging in {str(timedelta(seconds=time.time() - _time))}')
    staged_raw_df = spark.read.parquet(raw_staging_path)

    _time = time.time()
    print(f'===== counting staged_df records =====')
    staged_raw_df_count = staged_raw_df.count()
    staged_raw_df_partition_count = staged_raw_df.rdd.getNumPartitions()
    print(f'counted {staged_raw_df_count} record(s) in {staged_raw_df_partition_count} partition(s)')
    print(f'completed counting in {str(timedelta(seconds=time.time() - _time))}')

    # transform and write to consume here

    part_date = batch_end_dttm_ltz.strftime("%Y%m%d_%H%M")

    #Create UOM mapping DataFrame 
    uom_mapping_df = spark.createDataFrame(uom_data, uom_schema)

    print(f'===== Step 1: Rename columns =====')
    #rename columns 
    interval_data_files_src_df = (
        staged_raw_df
        .withColumnRenamed("_MeterName", "metername")              
        .withColumnRenamed("_UtilDeviceID", "utildeviceid")       
        .withColumnRenamed("_MacID", "macid")                     
        .withColumnRenamed("IntervalReadData", "interval_reading") 
        .withColumn("part_date", f.lit(part_date))                
    )

    # Reference: stg_nonvee.interval_data_files_{opco}_src_vw
    # Source: scripts/ddl/stg_nonvee.interval_data_files_{opco}_src_vw.ddl
    # Called by: scripts/source_interval_data_files.sh
    print(f'===== Step 2: 1st EXPLODE (intervals) =====')
    #1st EXPLODE: Flatten interval_reading array
    interval_exploded_df = (
        interval_data_files_src_df
        .withColumn("filename", f.input_file_name())
        .withColumn("exp_interval", f.explode("interval_reading.Interval"))
        .select(
            f.col("filename"),
            f.col("metername").alias("MeterName"),
            f.col("utildeviceid").alias("UtilDeviceID"),
            f.col("macid").alias("MacID"),
            f.col("interval_reading._IntervalLength").alias("IntervalLength"),
            f.col("interval_reading._StartTime").alias("blockstarttime"),
            f.col("interval_reading._EndTime").alias("blockendtime"),
            f.col("interval_reading._NumberIntervals").alias("NumberIntervals"),
            f.col("part_date"),
            f.col("exp_interval._EndTime").alias("int_endtime"),
            f.col("exp_interval._GatewayCollectedTime").alias("int_gatewaycollectedtime"),
            f.col("exp_interval._BlockSequenceNumber").alias("int_blocksequencenumber"),
            f.col("exp_interval._IntervalSequenceNumber").alias("int_intervalsequencenumber"),
            f.col("exp_interval.Reading").alias("int_reading")
        )
        .filter(
            f.date_format(
                f.to_timestamp(f.substring("blockstarttime", 1, 10), "yyyy-MM-dd"),
                "yyyyMMdd"
            ).cast("int") >= 20150301
        )
    )

    # Step 3: 2nd EXPLODE - Flatten int_reading + time calculations (notebook cell 13)
    print(f'===== Step 3: 2nd EXPLODE (readings) =====')
    reading_exploded_df = (
        interval_exploded_df
        .withColumn("exp_reading", f.explode("int_reading"))
        .select(
            f.col("filename"),
            f.col("MeterName"),
            f.col("UtilDeviceID"),
            f.col("MacID"),
            f.col("IntervalLength"),
            f.col("blockstarttime"),
            f.col("blockendtime"),
            f.col("NumberIntervals"),
            f.from_unixtime(
                f.unix_timestamp(f.substring("int_endtime", 1, 19), "yyyy-MM-dd'T'HH:mm:ss")
                - (f.col("IntervalLength").cast("int") * 60),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).alias("starttime"),
            f.from_unixtime(
                f.unix_timestamp(f.substring("int_endtime", 1, 19), "yyyy-MM-dd'T'HH:mm:ss"),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).alias("endtime"),
            f.substring("int_endtime", -6, 6).alias("interval_epoch"),
            f.col("int_gatewaycollectedtime"),
            f.col("int_blocksequencenumber"),
            f.col("int_intervalsequencenumber"),
            f.col("exp_reading._Channel").alias("channel"),
            f.col("exp_reading._RawValue").alias("rawvalue"),
            f.col("exp_reading._Value").alias("value"),
            f.col("exp_reading._UOM").alias("uom"),
            f.col("part_date")
        )
        .filter(
            f.col("channel").isNotNull() &
            f.col("rawvalue").isNotNull() &
            f.col("value").isNotNull() &
            f.col("uom").isNotNull()
        )
    )

    # LEFT JOIN with uom_mapping 
    print(f'===== Step 4: LEFT JOIN with UOM mapping =====')
    interval_data_files_src_vw_df = (reading_exploded_df
        .join(uom_mapping_df, reading_exploded_df.channel == uom_mapping_df.aep_channel_id, "left")
        .select(
            f.col("filename"),
            f.trim(f.col("MeterName")).alias("MeterName"),
            f.col("UtilDeviceID"),
            f.col("MacID"),
            f.col("IntervalLength"),
            f.col("blockstarttime"),
            f.col("blockendtime"),
            f.col("NumberIntervals"),
            f.concat(f.col("starttime"), f.col("interval_epoch")).alias("starttime"),
            f.concat(f.col("endtime"), f.col("interval_epoch")).alias("endtime"),
            f.col("interval_epoch"),
            f.col("int_gatewaycollectedtime"),
            f.col("int_blocksequencenumber"),
            f.col("int_intervalsequencenumber"),
            f.col("channel"),
            f.col("rawvalue"),
            f.col("value"),
            f.lower(f.trim(f.col("uom"))).alias("uom"),
            f.col("part_date"),
            f.coalesce(f.col("aep_derived_uom"), f.lit("UNK")).alias("aep_uom"),
            f.when(
                f.col("name_register").isNotNull(),
                f.regexp_replace(f.col("name_register"), "mtr_ivl_len", f.col("IntervalLength"))
            ).otherwise(f.lit("UNK")).alias("name_register"),
            f.coalesce(f.col("aep_srvc_qlty_idntfr"), f.lit("UNK")).alias("aep_sqi"),
            f.col("value_mltplr_flg")
        )
    )

    # Reference: stg_nonvee.interval_data_files_{opco}_stg
    # Source: scripts/ddl/stg_nonvee.interval_data_files_{opco}_stg.ddl
    #         scripts/stage_interval_xml_files.sh (Lines 194-272)
    # LOGIC:
    # 1. Read from interval_data_files_{opco}_src_vw_df 
    # 2. Read MACS reference table from Glue catalog
    # 3. LEFT JOIN on metername + time window validation
    # 4. Rename/transform columns to match DDL (41 columns)
    # 5. Load MACS reference table

    print(f'===== Step 5: Load MACS reference =====')
    target_companies = OPCO_TO_CO_CD.get(opco, [])

    macs_df = (
        spark.table("stg_nonvee.meter_premise_macs_ami")
        .filter(f.col("co_cd_ownr").isin(target_companies))
        .select(
            f.col("prem_nb"),
            f.col("bill_cnst"),
            f.col("acct_clas_cd"),
            f.col("acct_type_cd"),
            f.col("devc_cd"),
            f.col("pgm_id_nm"),
            f.concat(
                f.coalesce(f.col("tx_mand_data"), f.lit("")),
                f.coalesce(f.col("doe_nb"), f.lit("")),
                f.coalesce(f.col("serv_deliv_id"), f.lit(""))
            ).alias("sd"),
            f.col("mfr_devc_ser_nbr"),
            f.col("mtr_pnt_nb"),
            f.col("tarf_pnt_nb"),
            f.lit(None).cast(StringType()).alias("cmsg_mtr_mult_nb"),
            f.col("mtr_inst_ts"),
            f.col("mtr_rmvl_ts"),
            f.unix_timestamp("mtr_inst_ts", "yyyy-MM-dd HH:mm:ss").alias("unix_mtr_inst_ts"),
            f.when(
                f.col("mtr_rmvl_ts") == "9999-01-01",
                f.unix_timestamp("mtr_rmvl_ts", "yyyy-MM-dd")
            ).otherwise(
                f.unix_timestamp("mtr_rmvl_ts", "yyyy-MM-dd HH:mm:ss")
            ).alias("unix_mtr_rmvl_ts"),
            f.unix_timestamp("acct_turn_on_dt", "yyyy-MM-dd").alias("unix_acct_turn_on_dt"),
            f.unix_timestamp("acct_turn_off_dt", "yyyy-MM-dd").alias("unix_acct_turn_off_dt"),
            f.col("serv_city_ad").alias("aep_city"),
            f.col("serv_zip_ad").alias("aep_zip"),
            f.col("state_cd").alias("aep_state")
        )
    )

    # Step 6: LEFT JOIN with MACS
    print(f'===== Step 6: LEFT JOIN with MACS =====')
    interval_data_files_stg_df = (
        interval_data_files_src_vw_df
        .withColumn("unix_endtime", f.unix_timestamp("endtime", "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .join(
            macs_df,
            (f.col("MeterName") == f.col("mfr_devc_ser_nbr")) &
            (f.col("unix_endtime").between(f.col("unix_mtr_inst_ts"), f.col("unix_mtr_rmvl_ts"))) &
            (f.col("unix_endtime").between(f.col("unix_acct_turn_on_dt"), f.col("unix_acct_turn_off_dt"))),
            "left"
        )
        .select(
            f.col("filename"),
            f.lit(opco).alias("aep_opco"),
            f.col("MeterName").alias("serialnumber"),
            f.col("UtilDeviceID").alias("utildeviceid"),
            f.col("MacID").alias("macid"),
            f.col("IntervalLength").alias("intervallength"),
            f.col("blockstarttime"),
            f.col("blockendtime"),
            f.col("NumberIntervals").alias("numberintervals"),
            f.col("starttime"),
            f.col("endtime"),
            f.col("interval_epoch"),
            f.col("int_gatewaycollectedtime"),
            f.col("int_blocksequencenumber"),
            f.col("int_intervalsequencenumber"),
            f.col("channel"),
            f.col("value").alias("aep_raw_value"),
            f.col("value"),
            f.col("uom").alias("aep_raw_uom"),
            f.upper(f.col("aep_uom")).alias("aep_derived_uom"),
            f.col("name_register"),
            f.col("aep_sqi").alias("aep_srvc_qlty_idntfr"),
            f.col("prem_nb").alias("aep_premise_nb"),
            f.col("bill_cnst"),
            f.col("acct_clas_cd").alias("aep_acct_cls_cd"),
            f.col("acct_type_cd").alias("aep_acct_type_cd"),
            f.col("devc_cd").alias("aep_devicecode"),
            f.col("pgm_id_nm").alias("aep_meter_program"),
            f.col("sd").alias("aep_srvc_dlvry_id"),
            f.col("mtr_pnt_nb").alias("aep_mtr_pnt_nb"),
            f.col("tarf_pnt_nb").alias("aep_tarf_pnt_nb"),
            f.col("cmsg_mtr_mult_nb").alias("aep_comp_mtr_mltplr"),
            f.col("mtr_rmvl_ts").alias("aep_mtr_removal_ts"),
            f.col("mtr_inst_ts").alias("aep_mtr_install_ts"),
            f.col("aep_city"),
            f.substring(f.col("aep_zip"), 1, 5).alias("aep_zip"),
            f.col("aep_state"),
            f.lit("info-insert").alias("hdp_update_user"),
            f.col("part_date"),
            f.col("value_mltplr_flg"),
            f.substring(f.trim(f.col("MeterName")), -2, 2).alias("aep_meter_bucket")
        )
    )

    # Reference: default.iceberg_interval_data_files_{opco}_stg_vw
    # Source: scripts/ddl/stg_nonvee.interval_data_files_{opco}_stg_vw.ddl
    # Called by: xfrm_interval_data_files.sh (Line 190)
    # Input: interval_data_files_{opco}_stg_df (41 columns)
    # Output: interval_data_files_{opco}_stg_vw_df (43 columns)

    # Transform staged data by adding business logic:
    # - Add literals (source, isvirtual flags, timezone, usage_type)
    # - Compute aep_service_point from premise/tarf/mtr concatenation
    # - Calculate aep_sec_per_intrvl (interval length in seconds)
    # - Apply value multiplier logic (value * bill_cnst if flag='Y')
    # - Convert endtime to UTC unix timestamp
    # - Derive authority and aep_usage_dt from existing columns
    # - Add audit timestamps (hdp_insert_dttm, hdp_update_dttm)

    # Step 7: STG_VW transformations
    print(f'===== Step 7: STG_VW transformations =====')
    interval_data_files_stg_vw_df = (
        interval_data_files_stg_df
        .select(
            f.col("serialnumber"),
            f.lit("nonvee-hes").alias("source"),
            f.col("aep_devicecode"),
            f.lit("N").alias("isvirtual_meter"),
            f.col("interval_epoch").alias("timezoneoffset"),
            f.col("aep_premise_nb"),
            f.when(
                f.col("aep_premise_nb").isNotNull() &
                f.col("aep_tarf_pnt_nb").isNotNull() &
                f.col("aep_mtr_pnt_nb").isNotNull(),
                f.concat(f.col("aep_premise_nb"), f.lit("-"), f.col("aep_tarf_pnt_nb"), f.lit("-"), f.col("aep_mtr_pnt_nb"))
            ).otherwise(f.lit(None)).alias("aep_service_point"),
            f.col("aep_srvc_dlvry_id"),
            f.col("name_register"),
            f.lit("N").alias("isvirtual_register"),
            f.col("aep_derived_uom"),
            f.col("aep_raw_uom"),
            f.col("aep_srvc_qlty_idntfr"),
            f.col("channel").alias("aep_channel_id"),
            (f.col("intervallength").cast("int") * 60).alias("aep_sec_per_intrvl"),
            f.lit(None).cast("string").alias("aep_meter_alias"),
            f.col("aep_meter_program"),
            f.lit("interval").alias("aep_usage_type"),
            f.lit("US/Eastern").alias("aep_timezone_cd"),
            f.col("endtime").alias("endtimeperiod"),
            f.col("starttime").alias("starttimeperiod"),
            f.when(
                f.col("value_mltplr_flg") == "Y",
                f.col("value").cast("float") * f.col("bill_cnst").cast("float")
            ).otherwise(f.col("value").cast("float")).alias("value"),
            f.col("aep_raw_value"),
            f.col("bill_cnst").alias("scalarfloat"),
            f.col("aep_acct_cls_cd"),
            f.col("aep_acct_type_cd"),
            f.col("aep_mtr_pnt_nb"),
            f.col("aep_tarf_pnt_nb"),
            f.col("aep_comp_mtr_mltplr").cast("double").alias("aep_comp_mtr_mltplr"),
            f.unix_timestamp(
                f.concat(f.col("endtime"), f.col("interval_epoch")),
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            ).cast("string").alias("aep_endtime_utc"),
            f.col("aep_mtr_removal_ts"),
            f.col("aep_mtr_install_ts"),
            f.col("aep_city"),
            f.col("aep_zip"),
            f.col("aep_state"),
            f.from_unixtime(f.unix_timestamp(f.current_timestamp())).alias("hdp_insert_dttm"),
            f.from_unixtime(f.unix_timestamp(f.current_timestamp())).alias("hdp_update_dttm"),
            f.col("hdp_update_user"),
            f.substring(f.col("aep_premise_nb"), 1, 2).alias("authority"),
            f.substring(f.col("starttime"), 1, 10).alias("aep_usage_dt"),
            f.lit("new").alias("data_type"),
            f.col("aep_opco"),
            f.col("aep_meter_bucket")
        )
    )

    # Reference: iceberg_catalog.stg_nonvee.interval_data_files_{opco}_xfrm
    # Source: scripts/xfrm_interval_data_files.sh (Lines 140-191)
    #         scripts/ddl/stg_nonvee.interval_data_files_{opco}_xfrm.ddl
    #         scripts/ddl/stg_nonvee.interval_data_files_{opco}_xfrm_vw.ddl
    # Input: interval_data_files_{opco}_stg_vw_df (43 columns)
    # Output: interval_data_files_{opco}_xfrm_df (50 columns, deduplicated)

    # Prepare data for Iceberg xfrm table by:
    # - Combined both queries like (stg_nonvee.interval_data_files_{opco}_xfrm.ddl+interval_data_files_{opco}_xfrm_vw.ddl)
    # - Adding 5 NULL columns (toutier, toutiername, aep_billable_ind, aep_data_quality_cd, aep_data_validation)
    # - Casting to match DDL types (double, float, timestamp)
    # - Deduplication using ROW_NUMBER() partitioned by (serialnumber, endtimeperiod, aep_channel_id, aep_raw_uom)

    # Step 8: Deduplication + XFRM transformations
    print(f'===== Step 8: Deduplication =====')
    
    dedup_window = Window.partitionBy(
        "serialnumber",
        "endtimeperiod",
        "aep_channel_id",
        "aep_raw_uom"
    ).orderBy(f.col("hdp_insert_dttm").desc())

    interval_data_files_xfrm_df = (
        interval_data_files_stg_vw_df
        .select(
            f.col("serialnumber"),
            f.col("source"),
            f.col("aep_devicecode"),
            f.col("isvirtual_meter"),
            f.col("timezoneoffset"),
            f.col("aep_premise_nb"),
            f.col("aep_service_point"),
            f.col("aep_mtr_install_ts"),
            f.col("aep_mtr_removal_ts"),
            f.col("aep_srvc_dlvry_id"),
            f.col("aep_comp_mtr_mltplr").cast("double"),
            f.col("name_register"),
            f.col("isvirtual_register"),
            f.lit(None).cast("string").alias("toutier"),
            f.lit(None).cast("string").alias("toutiername"),
            f.col("aep_srvc_qlty_idntfr"),
            f.col("aep_channel_id"),
            f.col("aep_raw_uom"),
            f.col("aep_sec_per_intrvl").cast("double"),
            f.col("aep_meter_alias"),
            f.col("aep_meter_program"),
            f.lit(None).cast("string").alias("aep_billable_ind"),
            f.col("aep_usage_type"),
            f.col("aep_timezone_cd"),
            f.col("endtimeperiod"),
            f.col("starttimeperiod"),
            f.col("value").cast("float"),
            f.col("aep_raw_value").cast("float"),
            f.col("scalarfloat").cast("float"),
            f.lit(None).cast("string").alias("aep_data_quality_cd"),
            f.lit(None).cast("string").alias("aep_data_validation"),
            f.col("aep_acct_cls_cd"),
            f.col("aep_acct_type_cd"),
            f.col("aep_mtr_pnt_nb"),
            f.col("aep_tarf_pnt_nb"),
            f.col("aep_endtime_utc"),
            f.col("aep_city"),
            f.col("aep_zip"),
            f.col("aep_state"),
            f.col("hdp_update_user"),
            f.col("hdp_insert_dttm").cast("timestamp"),
            f.col("hdp_update_dttm").cast("timestamp"),
            f.col("authority"),
            f.col("aep_derived_uom"),
            f.col("data_type"),
            f.col("aep_opco"),
            f.col("aep_usage_dt"),
            f.col("aep_meter_bucket")
        )
        .withColumn("row_num", f.row_number().over(dedup_window))
        .filter(f.col("row_num") == 1)
        .drop("row_num")
    )

    # Step 9: Checkpoint + MERGE into Iceberg
    print(f'===== Step 9: Checkpoint + MERGE =====')
    
    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(f"s3://{work_bucket}/temp/checkpoint/")
    
    # Materialize the DataFrame (saves dedup result to disk)
    materialized_df = interval_data_files_xfrm_df.checkpoint()
    
    # Create temp view from materialized data
    materialized_df.createOrReplaceTempView("interval_data_files_xfrm_vw")
    
    # Get distinct usage dates for partition pruning
    usage_dates_df = materialized_df.select("aep_usage_dt").distinct()
    usage_dates_list = [row.aep_usage_dt for row in usage_dates_df.collect()]
    usage_dates_str = ",".join([f"'{d}'" for d in usage_dates_list])
    
    print(f'===== Usage dates: {usage_dates_str} =====')
    
    # MERGE SQL
    merge_sql = f"""
    MERGE INTO iceberg_catalog.usage_nonvee.reading_ivl_nonvee_{opco} AS t
    USING (
        SELECT
            serialnumber, source, aep_devicecode, isvirtual_meter, timezoneoffset,
            aep_premise_nb, aep_service_point, aep_srvc_dlvry_id, name_register,
            isvirtual_register, toutier, toutiername, aep_derived_uom, aep_raw_uom,
            aep_srvc_qlty_idntfr, aep_channel_id, aep_sec_per_intrvl, aep_meter_alias,
            aep_meter_program, aep_billable_ind, aep_usage_type, aep_timezone_cd,
            endtimeperiod, starttimeperiod, value, aep_raw_value, scalarfloat,
            aep_data_quality_cd, aep_data_validation, aep_acct_cls_cd, aep_acct_type_cd,
            aep_mtr_pnt_nb, aep_tarf_pnt_nb, aep_comp_mtr_mltplr, aep_endtime_utc,
            aep_mtr_removal_ts, aep_mtr_install_ts, aep_city, aep_zip, aep_state,
            hdp_update_user, hdp_insert_dttm, hdp_update_dttm, authority, aep_opco,
            aep_usage_dt, aep_meter_bucket
        FROM interval_data_files_xfrm_vw
    ) AS s
    ON t.aep_usage_dt IN ({usage_dates_str})
       AND t.aep_usage_dt = s.aep_usage_dt
       AND t.aep_meter_bucket = s.aep_meter_bucket
       AND t.serialnumber = s.serialnumber
       AND t.aep_channel_id = s.aep_channel_id
       AND t.aep_raw_uom = s.aep_raw_uom
       AND t.endtimeperiod = s.endtimeperiod
    WHEN MATCHED THEN
        UPDATE SET
            t.aep_srvc_qlty_idntfr = s.aep_srvc_qlty_idntfr,
            t.aep_channel_id = s.aep_channel_id,
            t.aep_sec_per_intrvl = s.aep_sec_per_intrvl,
            t.aep_meter_alias = s.aep_meter_alias,
            t.aep_meter_program = s.aep_meter_program,
            t.aep_usage_type = s.aep_usage_type,
            t.aep_timezone_cd = s.aep_timezone_cd,
            t.endtimeperiod = s.endtimeperiod,
            t.starttimeperiod = s.starttimeperiod,
            t.value = s.value,
            t.aep_raw_value = s.aep_raw_value,
            t.scalarfloat = s.scalarfloat,
            t.aep_acct_cls_cd = s.aep_acct_cls_cd,
            t.aep_acct_type_cd = s.aep_acct_type_cd,
            t.aep_mtr_pnt_nb = s.aep_mtr_pnt_nb,
            t.aep_comp_mtr_mltplr = s.aep_comp_mtr_mltplr,
            t.aep_endtime_utc = s.aep_endtime_utc,
            t.aep_mtr_removal_ts = s.aep_mtr_removal_ts,
            t.aep_mtr_install_ts = s.aep_mtr_install_ts,
            t.aep_city = s.aep_city,
            t.aep_zip = s.aep_zip,
            t.aep_state = s.aep_state,
            t.hdp_update_user = 'info-update',
            t.hdp_update_dttm = s.hdp_update_dttm,
            t.authority = s.authority
    WHEN NOT MATCHED THEN
        INSERT (
            serialnumber, source, aep_devicecode, isvirtual_meter, timezoneoffset,
            aep_premise_nb, aep_service_point, aep_srvc_dlvry_id, name_register,
            isvirtual_register, toutier, toutiername, aep_derived_uom, aep_raw_uom,
            aep_srvc_qlty_idntfr, aep_channel_id, aep_sec_per_intrvl, aep_meter_alias,
            aep_meter_program, aep_billable_ind, aep_usage_type, aep_timezone_cd,
            endtimeperiod, starttimeperiod, value, aep_raw_value, scalarfloat,
            aep_data_quality_cd, aep_data_validation, aep_acct_cls_cd, aep_acct_type_cd,
            aep_mtr_pnt_nb, aep_tarf_pnt_nb, aep_comp_mtr_mltplr, aep_endtime_utc,
            aep_mtr_removal_ts, aep_mtr_install_ts, aep_city, aep_zip, aep_state,
            hdp_update_user, hdp_insert_dttm, hdp_update_dttm, authority, aep_opco,
            aep_usage_dt, aep_meter_bucket
        ) VALUES (
            s.serialnumber, s.source, s.aep_devicecode, s.isvirtual_meter, s.timezoneoffset,
            s.aep_premise_nb, s.aep_service_point, s.aep_srvc_dlvry_id, s.name_register,
            s.isvirtual_register, s.toutier, s.toutiername, s.aep_derived_uom, s.aep_raw_uom,
            s.aep_srvc_qlty_idntfr, s.aep_channel_id, s.aep_sec_per_intrvl, s.aep_meter_alias,
            s.aep_meter_program, s.aep_billable_ind, s.aep_usage_type, s.aep_timezone_cd,
            s.endtimeperiod, s.starttimeperiod, s.value, s.aep_raw_value, s.scalarfloat,
            s.aep_data_quality_cd, s.aep_data_validation, s.aep_acct_cls_cd, s.aep_acct_type_cd,
            s.aep_mtr_pnt_nb, s.aep_tarf_pnt_nb, s.aep_comp_mtr_mltplr, s.aep_endtime_utc,
            s.aep_mtr_removal_ts, s.aep_mtr_install_ts, s.aep_city, s.aep_zip, s.aep_state,
            'info-insert', s.hdp_insert_dttm, s.hdp_update_dttm, s.authority, s.aep_opco,
            s.aep_usage_dt, s.aep_meter_bucket
        )
    """
    
    print(f'===== Executing MERGE =====')
    spark.sql(merge_sql)
    print(f'===== MERGE completed =====')

    # Step 10: DELETE old + INSERT new into downstream incremental table
    print(f'===== Step 10: DELETE old + INSERT into downstream table =====')
    
    # Calculate cutoff date (8 days ago)
    cutoff_date = (datetime.now() - timedelta(days=8)).strftime("%Y%m%d_%H%M%S")
    current_run_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Step 10a: DELETE old data (older than 8 days)
    delete_sql = f"""
    DELETE FROM iceberg_catalog.xfrm_interval.reading_ivl_nonvee_incr
    WHERE aep_opco = '{opco}' AND run_date < '{cutoff_date}'
    """
    print(f'===== Deleting old data (run_date < {cutoff_date}) =====')
    spark.sql(delete_sql)
    print(f'===== DELETE completed =====')
    
    # Step 10b: INSERT new data
    insert_downstream_sql = f"""
    INSERT INTO iceberg_catalog.xfrm_interval.reading_ivl_nonvee_incr_test
    SELECT
        serialnumber,
        source,
        aep_devicecode,
        isvirtual_meter,
        timezoneoffset,
        aep_premise_nb,
        aep_service_point,
        aep_mtr_install_ts,
        aep_mtr_removal_ts,
        aep_srvc_dlvry_id,
        aep_comp_mtr_mltplr,
        name_register,
        isvirtual_register,
        toutier,
        toutiername,
        aep_srvc_qlty_idntfr,
        aep_channel_id,
        aep_raw_uom,
        aep_sec_per_intrvl,
        aep_meter_alias,
        aep_meter_program,
        aep_billable_ind,
        aep_usage_type,
        aep_timezone_cd,
        endtimeperiod,
        starttimeperiod,
        value,
        aep_raw_value,
        scalarfloat,
        aep_data_quality_cd,
        aep_data_validation,
        aep_acct_cls_cd,
        aep_acct_type_cd,
        aep_mtr_pnt_nb,
        aep_tarf_pnt_nb,
        aep_endtime_utc,
        aep_city,
        aep_zip,
        aep_state,
        hdp_update_user,
        hdp_insert_dttm,
        hdp_update_dttm,
        authority,
        aep_usage_dt,
        aep_derived_uom,
        aep_opco,
        '{current_run_date}' as run_date
    FROM interval_data_files_xfrm_vw
    WHERE data_type = 'new' AND aep_opco = '{opco}'
    """
    
    print(f'===== Inserting new data (run_date = {current_run_date}) =====')
    spark.sql(insert_downstream_sql)
    print(f'===== INSERT downstream completed =====')

    # update last_process_dttm_file
    # Step 11: Update last_process_dttm file
    print(f'===== Step 11: Update last_process_dttm =====')
    s3 = boto3.client("s3")
    last_process_dttm_file = f'info_last_proc_dt_do_not_remove_{opco}.txt'
    last_process_dttm_key = f'{parms_prefix}/{last_process_dttm_file}'
    new_last_process_dttm = batch_end_dttm_ltz.strftime("%Y-%m-%d %H:%M:%S")
    
    s3.put_object(
        Bucket=work_bucket,
        Key=last_process_dttm_key,
        Body=new_last_process_dttm.encode("utf-8")
    )
    print(f'===== Updated {last_process_dttm_key} to {new_last_process_dttm} =====')

    if not skip_archive == "true":
        # archive raw xml files
        pass

    # delete scratch directory
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(raw_staging_path)
    if fs.exists(path):
        print(f"Deleting HDFS path: {path}")
        fs.delete(path, True)

    spark.stop()
    print(f'completed job in {str(timedelta(seconds=time.time() - _bgn_time))}')


if __name__ == "__main__":
    main()
