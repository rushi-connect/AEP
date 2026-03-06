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

import pyspark.sql.functions as F


parms_prefix = "aws_analytics_applications/aep_dl_util_ami_nonvee_auth/parms"
work_prefix = "raw"
archive_prefix = "util"
data_files_dir = "intervals/nonvee/uiq_auth"

marker_file_name = "index.txt"
marker_file_max_depth = 2
data_file_ext = ".xml"
data_file_min_size = 0

xml_row_tag = "MeterData"
xml_read_mode = "FAILFAST"
xml_infer_schema = "true"
xml_tag_ignore_namespace = "true"
xml_data_timezone = "America/New_York"
xml_tag_value_col_name = "tag_value"

raw_xml_schema_str = """
`_MeterName` STRING,
`_UtilDeviceID` STRING,
`_MacID` STRING,
`IntervalReadData` ARRAY<
    STRUCT<
        `_LpSetId` STRING,
        `_NumberIntervals` STRING,
        `_EndTime` STRING,
        `_StartTime` STRING,
        `_IntervalLength` STRING,
        `Interval` ARRAY<
            STRUCT<
                `_GatewayCollectedTime` STRING,
                `_IntervalSequenceNumber` STRING,
                `_BlockSequenceNumber` STRING,
                `_EndTime` STRING,
                `Reading` ARRAY<
                    STRUCT<
                        `_UOM` STRING,
                        `_BlockEndValue` STRING,
                        `_Value` STRING,
                        `_RawValue` STRING,
                        `_Channel` STRING
                    >
                >
            >
        >
    >
>
"""


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
    # job_name = "uiq-nonvee-auth"
    # aws_env = "dev"
    # opco = "oh"
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
        last_process_dttm_file = f'auth_last_proc_dt_do_not_remove_{opco}.txt'
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
            .schema(raw_xml_schema_str) \
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
    # transform and write to consume here
    # transform and write to consume here
    # transform and write to consume here
    # transform and write to consume here
    # transform and write to consume here

    # update last_process_dttm_file

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
