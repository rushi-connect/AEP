"""
spark-submit \
  --deploy-mode client \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=glue \
  --conf spark.sql.catalog.spark_catalog.glue.id=889415020100 \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3://aep-datalake-apps-dev/emr/warehouse/nonvee-lg-tx \
  --conf spark.sql.session.timeZone=America/New_York \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=America/New_York \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=America/New_York \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --driver-cores 4 \
  --driver-memory 32g \
  --conf spark.driver.maxResultSize=16g \
  --executor-cores 1 \
  --executor-memory 5g \
  s3://aep-datalake-apps-dev/hdpapp/aep_dl_util_ami_nonvee_lg/pyspark/nonvee_lg_job.py \
  --job_name nonvee-lg-tx \
  --opco tx \
  --data_bucket aep-datalake-work-dev \
  --batch_start_dttm_str 2026-02-01 00:00:00 \
  --batch_end_dttm_str 2026-02-02 00:00:00 \
  --batch_run_dttm_str 2026-02-02 00:00:00
"""
import time
import uuid
import boto3
import argparse

from typing import Dict, List
from textwrap import dedent
from functools import reduce
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

import pyspark.sql.functions as F


data_prefix = "raw/intervals/nonvee/lg"
marker_file_name = "index.txt"
marker_file_max_depth = 2
data_file_ext = ".txt"
data_file_min_size = 0

co_cd_ownr_map = {
    "tx": ["94", "97"]
}

csv_read_mode = "PERMISSIVE"
csv_infer_schema = "false"
csv_enforce_schema = "true"
csv_header_row = "true"
csv_field_separator = "~"
csv_quote_char = '"'
csv_escape_char = "\\"
csv_null_value = ""
csv_data_timezone = "America/New_York"

raw_csv_schema_str = """
record_type string,
record_version string,
time_stamp string,
premise_id string,
esiid string,
provisioned string,
meter_id string,
purpose string,
commodity string,
units string,
calculation_constant string,
cinterval string,
count string,
firstintervaldatetime string,
t1 string,
v1 string,
t2 string,
v2 string,
t3 string,
v3 string,
t4 string,
v4 string,
t5 string,
v5 string,
t6 string,
v6 string,
t7 string,
v7 string,
t8 string,
v8 string,
t9 string,
v9 string,
t10 string,
v10 string,
t11 string,
v11 string,
t12 string,
v12 string,
t13 string,
v13 string,
t14 string,
v14 string,
t15 string,
v15 string,
t16 string,
v16 string,
t17 string,
v17 string,
t18 string,
v18 string,
t19 string,
v19 string,
t20 string,
v20 string,
t21 string,
v21 string,
t22 string,
v22 string,
t23 string,
v23 string,
t24 string,
v24 string,
t25 string,
v25 string,
t26 string,
v26 string,
t27 string,
v27 string,
t28 string,
v28 string,
t29 string,
v29 string,
t30 string,
v30 string,
t31 string,
v31 string,
t32 string,
v32 string,
t33 string,
v33 string,
t34 string,
v34 string,
t35 string,
v35 string,
t36 string,
v36 string,
t37 string,
v37 string,
t38 string,
v38 string,
t39 string,
v39 string,
t40 string,
v40 string,
t41 string,
v41 string,
t42 string,
v42 string,
t43 string,
v43 string,
t44 string,
v44 string,
t45 string,
v45 string,
t46 string,
v46 string,
t47 string,
v47 string,
t48 string,
v48 string,
t49 string,
v49 string,
t50 string,
v50 string,
t51 string,
v51 string,
t52 string,
v52 string,
t53 string,
v53 string,
t54 string,
v54 string,
t55 string,
v55 string,
t56 string,
v56 string,
t57 string,
v57 string,
t58 string,
v58 string,
t59 string,
v59 string,
t60 string,
v60 string,
t61 string,
v61 string,
t62 string,
v62 string,
t63 string,
v63 string,
t64 string,
v64 string,
t65 string,
v65 string,
t66 string,
v66 string,
t67 string,
v67 string,
t68 string,
v68 string,
t69 string,
v69 string,
t70 string,
v70 string,
t71 string,
v71 string,
t72 string,
v72 string,
t73 string,
v73 string,
t74 string,
v74 string,
t75 string,
v75 string,
t76 string,
v76 string,
t77 string,
v77 string,
t78 string,
v78 string,
t79 string,
v79 string,
t80 string,
v80 string,
t81 string,
v81 string,
t82 string,
v82 string,
t83 string,
v83 string,
t84 string,
v84 string,
t85 string,
v85 string,
t86 string,
v86 string,
t87 string,
v87 string,
t88 string,
v88 string,
t89 string,
v89 string,
t90 string,
v90 string,
t91 string,
v91 string,
t92 string,
v92 string,
t93 string,
v93 string,
t94 string,
v94 string,
t95 string,
v95 string,
t96 string,
v96 string,
t97 string,
v97 string,
t98 string,
v98 string,
t99 string,
v99 string,
t100 string,
v100 string
"""


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
                and batch_start_dttm_ltz <= obj_last_modified < batch_end_dttm_ltz
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


def get_macs_ami_table(
    spark: SparkSession,
    table_name: str = "stg_nonvee.meter_premise_macs_ami",
    target_co_cd_ownr: List[str] = None,
) -> DataFrame:
    """
    Extracts meter_premise_macs_ami data from the `stg_nonvee` layer
    and filters for the specified OPCO company codes.

    updated legacy timestamp to yyyy-MM-dd HH:mm:ss[.SSSSSS]
    """
    if target_co_cd_ownr is None:
        target_co_cd_ownr = ["94", "97"]

    print(f"===== Reading table {table_name} =====")
    ami_df = spark.table(table_name).filter(
        F.col("co_cd_ownr").isin(target_co_cd_ownr)
    ).select(
        F.col("prem_nb"),
        F.col("bill_cnst"),
        F.col("acct_clas_cd"),
        F.col("acct_type_cd"),
        F.col("devc_cd"),
        F.col("pgm_id_nm"),
        F.concat(
            F.coalesce(F.col("tx_mand_data"), F.lit("")),
            F.coalesce(F.col("doe_nb"), F.lit("")),
            F.coalesce(F.col("serv_deliv_id"), F.lit("")),
        ).alias("sd"),
        F.col("mfr_devc_ser_nbr"),
        F.col("mtr_pnt_nb"),
        F.col("tarf_pnt_nb"),
        F.lit(None).cast(StringType()).alias("cmsg_mtr_mult_nb"),
        F.col("mtr_inst_ts"),
        F.col("mtr_rmvl_ts"),
        F.to_timestamp(
            F.col("mtr_inst_ts"), "yyyy-MM-dd HH:mm:ss[.SSSSSS]"
        ).cast("Long").alias("unix_mtr_inst_ts"),
        F.to_timestamp(
            F.col("mtr_rmvl_ts"), "yyyy-MM-dd"
        ).cast("Long").alias("unix_mtr_rmvl_ts"),
        F.to_timestamp(
            F.col("acct_turn_on_dt"), "yyyy-MM-dd"
        ).cast("Long").alias("unix_acct_turn_on_dt"),
        F.to_timestamp(
            F.col("acct_turn_off_dt"), "yyyy-MM-dd"
        ).cast("Long").alias("unix_acct_turn_off_dt"),
        F.col("serv_city_ad"),
        F.col("serv_zip_ad"),
        F.col("state_cd"),
    )
    print("===== Table read successfully =====")
    return ami_df


def get_uom_mapping_table(
    spark: SparkSession,
    opco: str,
    table_name: str = "usage_nonvee.uom_mapping",
) -> DataFrame:
    """
    Extracts uom_mapping data from the `usage_nonvee` layer
    and filters for specific OPCO data
    """

    print(f"===== Reading table {table_name} =====")
    uom_mapping_df = spark.table(table_name).filter(
        F.col("aep_opco") == opco
    ).select(
        F.col("aep_opco"),
        F.col("aep_channel_id"),
        F.col("aep_derived_uom"),
        F.col("name_register"),
        F.col("aep_srvc_qlty_idntfr"),
        F.lower(F.trim(F.col("aep_raw_uom"))).alias("aep_raw_uom"),
        F.col("value_mltplr_flg"),
    )
    print("===== Table read successfully =====")
    return uom_mapping_df


def delete_old_data(
        spark: SparkSession,
        table_name: str,
        opco: str,
        days: int = 8,
    ):
    """
    Deletes records older than a specified number of days for a give opco.
    """
    try:
        # Calculate the cutoff date (8 days before today)
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d_%H%M%S")

        delete_query = f"""
        DELETE FROM {table_name}
        WHERE aep_opco = '{opco}' AND run_date < '{cutoff_date}'
        """
        print(f"===== Executing deletion query: {delete_query} =====")
        spark.sql(delete_query)
        print(
            f"===== Data older than {days} days for OPCO '{opco}' deleted successfully. ====="
        )
    except AnalysisException as e:
        print(f"===== ERROR: Failed to delete data from {table_name}: {e} =====")


def main():
    print(f'===== starting job =====')
    _bgn_time = time.time()

    _time = time.time()
    print(f'===== started parsing job args =====')
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--opco", required=True)
    parser.add_argument("--data_bucket", required=True)
    parser.add_argument("--batch_start_dttm_str", required=True)
    parser.add_argument("--batch_end_dttm_str", required=True)
    parser.add_argument("--batch_run_dttm_str", required=True)
    args = parser.parse_args()
    print(f'completed parsing in {str(timedelta(seconds=time.time() - _time))}')
    print(f'===== args [{type(args)}]: {args} =====')

    print(f'===== init vars =====')
    job_name = args.job_name
    opco = args.opco
    data_bucket = args.data_bucket
    batch_start_dttm_str = args.batch_start_dttm_str
    batch_end_dttm_str = args.batch_end_dttm_str
    batch_run_dttm_str = args.batch_run_dttm_str

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
    batch_start_dttm_ntz = datetime.strptime(batch_start_dttm_str, "%Y-%m-%d %H:%M:%S")
    batch_start_dttm_ltz = batch_start_dttm_ntz.replace(tzinfo=local_tz)
    batch_end_dttm_ntz = datetime.strptime(batch_end_dttm_str, "%Y-%m-%d %H:%M:%S")
    batch_end_dttm_ltz = batch_end_dttm_ntz.replace(tzinfo=local_tz)
    batch_run_dttm_ntz = datetime.strptime(batch_run_dttm_str, "%Y-%m-%d %H:%M:%S")
    batch_run_dttm_ltz = batch_run_dttm_ntz.replace(tzinfo=local_tz)
    base_prefix = f'{data_prefix}/{opco}'
    scratch_path = f'hdfs:///tmp/scratch/{str(uuid.uuid4())}'
    print(f'===== batch start dttm: {batch_start_dttm_ltz} =====')
    print(f'===== batch end dttm: {batch_end_dttm_ltz} =====')
    print(f'===== batch run dttm: {batch_run_dttm_ltz} =====')
    print(f'===== batch base prefix: {base_prefix} =====')
    print(f'===== batch scratch path: {scratch_path} =====')

    filtered_data_files_map = get_filtered_data_files_map(
        s3_bucket=data_bucket,
        s3_prefix=base_prefix,
        marker_file_name=marker_file_name,
        batch_start_dttm_ltz=batch_start_dttm_ltz,
        batch_end_dttm_ltz=batch_end_dttm_ltz,
        marker_file_max_depth=marker_file_max_depth,
        data_file_min_size=data_file_min_size,
        data_file_ext=data_file_ext,
    )

    files_count = sum(len(f) for f in filtered_data_files_map.values())
    if files_count == 0:
        print(f'0 files selected. exiting ...')
        spark.stop()
        print(f'completed job in {str(timedelta(seconds=time.time() - _bgn_time))}')
        exit(0)

    _time = time.time()
    print(f'===== reading files =====')
    files_count = 0
    dfs = []
    for k, files in filtered_data_files_map.items():
        files_count += len(files)
        files_str = ",".join([f's3://{data_bucket}/{f}' for f in files])
        df = spark.read.format("csv") \
            .option("mode", csv_read_mode) \
            .option("inferSchema", csv_infer_schema) \
            .option("enforceSchema", csv_enforce_schema) \
            .option("header", csv_header_row) \
            .option("sep", csv_field_separator) \
            .option("quote", csv_quote_char) \
            .option("escape", csv_escape_char) \
            .option("nullValue", csv_null_value) \
            .option("timezone", csv_data_timezone) \
            .schema(raw_csv_schema_str) \
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

    # read macs_ami and uom_mapping tables
    ami_df = get_macs_ami_table(spark, target_co_cd_ownr=co_cd_ownr_map[opco])
    uom_mapping_df = get_uom_mapping_table(spark, opco)

    # Starting transformations
    # Step 1: Preparing Source Data
    value_cols = [f"v{i}" for i in range(1,101)]

    staged_raw_filtered_df = staged_raw_df.filter(
        (F.substring("record_type", 1, 5) == "MEPMD")
    ).filter(
        (F.to_date(F.substring("firstintervaldatetime", 1, 8), "MMddyyyy") >= F.lit("2015-03-01"))
    )
    print("===== Source data is filtered for MEPMD and >= 2015-03-01 =====")

    interval_ts = F.to_timestamp("firstintervaldatetime", "MMddyyyyhhmmssa").cast("long")

    # join source data with macs_ami
    interval_ami_df = staged_raw_filtered_df.alias("src").join(
        ami_df.alias("ami"),
        on=(F.col("src.meter_id") == F.col("ami.mfr_devc_ser_nbr")),
        how="left",
    ).select(
        F.col("src.record_type"),
        F.col("src.time_stamp"),
        F.when(
            F.isnull(F.trim(F.col("src.premise_id"))) | (F.trim(F.col("src.premise_id")) == ""),
            F.col("ami.prem_nb")
        ).otherwise(F.split(F.col("src.premise_id"), "-")[0]).alias("premise_id"),
        F.when(
            F.isnull(F.trim(F.col("src.premise_id"))) | (F.trim(F.col("src.premise_id")) == ""),
            F.concat_ws("-", F.col("ami.prem_nb"), F.col("ami.mtr_pnt_nb"), F.col("ami.tarf_pnt_nb"))
        ).otherwise(F.col("src.premise_id")).alias("premise_service_point"),
        F.col("src.meter_id"),
        F.lower(F.trim("src.units")).alias("units"),
        F.col("src.cinterval"),
        F.col("src.cinterval").cast("int").cast("string").alias("intervallength"),
        F.col("src.firstintervaldatetime"),
        F.col("ami.bill_cnst"),
        F.col("ami.acct_clas_cd"),
        F.col("ami.acct_type_cd"),
        F.col("ami.devc_cd"),
        F.col("ami.pgm_id_nm"),
        F.col("ami.sd"),
        F.split(F.concat_ws(",", *value_cols), ",").alias("value"),
        F.col("ami.mtr_pnt_nb"),
        F.col("ami.tarf_pnt_nb"),
        F.col("ami.cmsg_mtr_mult_nb"),
        F.col("ami.mtr_inst_ts"),
        F.col("ami.mtr_rmvl_ts"),
        F.col("ami.serv_city_ad"),
        F.col("ami.serv_zip_ad"),
        F.col("ami.state_cd"),
        F.when(
            F.isnull("ami.mfr_devc_ser_nbr"),
            F.lit(0.5)
        ).when(
            (
                (interval_ts - F.lit(3600)).between(
                    F.col("ami.unix_mtr_inst_ts"),
                    F.col("ami.unix_mtr_rmvl_ts")
                )
            )
            & (
                (interval_ts - F.lit(3600)).between(
                    F.col("ami.unix_acct_turn_on_dt"),
                    F.col("ami.unix_acct_turn_off_dt"),
                )
            ),
            F.lit(1),
        ).otherwise(F.lit(0)).alias("include_check"),
        F.col("src.cinterval").cast("int").alias("aep_sec_per_intrvl"),
        interval_ts.alias("interval_unix_datetime"),
        F.when(
            F.trim(F.from_unixtime(interval_ts - F.lit(3600), "XXX")) == "-05:00",
            F.lit("-06:00"),
        ).otherwise(F.lit("-05:00")).alias("interval_epoch"),
    )
    print("===== Data enrichment completed: interval left join with meter_premise =====")

    # Step 3: Deduplicate data by partioning on "premise_id", "meter_id", "units", "firstintervaldatetime"
    window_spec = Window.partitionBy(
        "premise_id", "meter_id", "units", "firstintervaldatetime"
    ).orderBy(F.desc("include_check"))
    dedup_interval_ami_df = interval_ami_df.withColumn(
        "row_num", F.row_number().over(window_spec)
    ).filter(F.col("row_num") == 1).drop("row_num")
    print("===== Data Depuplication 1 completed =====")

    # ============================================================
    # Source Data Enrichment 2 : LEFT JOIN, Premise Mapping & Time normalization (interval_data_files_tx_src_vw.ddl )
    # ============================================================
    # - Left join joined_df data wih uom_mapping data
    # - Use include_check column to derive few columns
    # - Explode `value` to derive `row_ord` & `aep_raw_value`
    # - Dropped `value` column which is array of t & v (originally this is excluded in stage_interval_csv_files.sh)
    # ============================================================

    # Step 4: Join above transformed data with uom_mapping, apply transformation
    include_condition = F.col("include_check") == 1

    interval_ami_uom_df = dedup_interval_ami_df.join(
        F.broadcast(uom_mapping_df),
        on=(F.col("units")) == (F.col("aep_raw_uom")),
        how="left",
    ).select(
        F.lit(opco).alias("aep_opco"),
        F.col("timestamp"),
        F.col("premise_id").alias("aep_premise_nb"),
        F.col("premise_service_point").alias("aep_service_point"),
        F.trim("meter_id").alias("serialnumber"),
        F.lit(None).cast("string").alias("aep_channel_id"),
        F.col("units").alias("aep_raw_uom"),
        F.upper(F.coalesce("aep_derived_uom", F.lit("UNK"))).alias("aep_derived_uom"),
        F.when(
            F.isnotnull("name_register"),
            F.expr("translate(name_register, 'mtr_ivl_len', intervallength)"),
        ).otherwise("UNK").alias("name_register"),
        F.coalesce("aep_srvc_qlty_idntfr", F.lit("UNK")).alias("aep_srvc_qlty_idntfr"),
        F.col("aep_sec_per_intrvl"),
        F.col("firstintervaldatetime"),
        F.posexplode("value").alias("row_ord", "aep_raw_value"),
        F.when(include_condition, F.col("bill_cnst")).alias("scalarfloat"),
        F.when(include_condition, F.col("acct_clas_cd")).alias("aep_acct_cls_cd"),
        F.when(include_condition, F.col("acct_type_cd")).alias("aep_acct_type_cd"),
        F.when(include_condition, F.col("devc_cd")).alias("aep_devicecode"),
        F.lit(None).alias("aep_meter_alias"),
        F.when(include_condition, F.col("pgm_id_nm")).alias("aep_meter_program"),
        F.when(include_condition, F.col("sd")).alias("aep_srvc_dlvry_id"),
        F.when(include_condition, F.col("mtr_pnt_nb")).alias("aep_mtr_pnt_nb"),
        F.when(include_condition, F.col("tarf_pnt_nb")).alias("aep_tarf_pnt_nb"),
        F.when(
            include_condition,
            F.col("cmsg_mtr_mult_nb").cast("double")
        ).alias("aep_comp_mtr_mltplr"),
        F.lit(None).alias("toutier"),
        F.lit(None).alias("toutiername"),
        F.lit(None).alias("aep_billable_ind"),
        F.lit(None).alias("aep_data_quality_cd"),
        F.lit(None).alias("aep_data_validation"),
        F.col("interval_unix_datetime"),
        F.col("interval_epoch"),
        F.col("mtr_inst_ts").alias("aep_mtr_install_ts"),
        F.col("mtr_rmvl_ts").alias("aep_mtr_removal_ts"),
        F.col("serv_city_ad").alias("aep_city"),
        F.substring(F.col("serv_zip_ad"), 1, 5).alias("aep_zip"),
        F.col("state_cd").alias("aep_state"),
        F.col("value_mltplr_flg"),
        F.right(F.trim(F.col("meter_id")), "2").alias("aep_meter_bucket"),
    )
    print("===== Data enrichment completed: joined_df left join with uom_mapping =====")

    # ============================================================
    # Staging Transformations :  (interval_data_files_tx_stg_vw.ddl )
    # ============================================================
    # - Deduce timezone offset
    # - Derive `aep_usage_dt` using `interval_unix_datetime`
    # ============================================================

    # Step 5: Apply Transformations to derive timezone offset, aep_usage_dt - added this code to xfrm_df
#     stg_df = (
# interval_ami_uom_df.withColumn("source", F.lit("nonvee-hes"))
#         .withColumn("isvirtual_meter", F.lit("N"))
#         .withColumn(
#             "timezoneoffset",
#             F.when(
#                 F.trim(
#                     F.date_format(
#                         F.from_unixtime(
#                             F.col("interval_unix_datetime")
#                             + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
#                         ),
#                         "XXX",
#                     )
#                 )
#                 == "-05:00",
#                 F.lit("-06:00"),
#             ).otherwise(F.lit("05:00")),
#         )
#         .withColumn("isvirtual_register", F.lit("N"))
#         .withColumn("aep_sec_per_intrvl", F.col("aep_sec_per_intrvl") * 60)
#         .withColumn("aep_usage_type", F.lit("interval"))
#         .withColumn("aep_timezone_cd", F.lit("US/Central"))
#         .withColumn(
#             "endtimeperiod",
#             F.concat(
#                 F.date_format(
#                     F.from_unixtime(
#                         F.col("interval_unix_datetime")
#                         + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
#                     ),
#                     "yyyy-MM-dd'T'HH:mm:ss",
#                 ),
#                 F.when(
#                     F.trim(
#                         F.date_format(
#                             F.from_unixtime(
#                                 F.col("interval_unix_datetime")
#                                 + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
#                             ),
#                             "XXX",
#                         )
#                     )
#                     == "-05:00",
#                     F.lit("-06:00"),
#                 ).otherwise(F.lit("-05:00")),
#             ),
#         )
#         .withColumn(
#             "starttimeperiod",
#             F.concat(
#                 F.date_format(
#                     F.from_unixtime(
#                         F.col("interval_unix_datetime")
#                         + (F.col("aep_sec_per_intrvl") * (F.col("row_ord") - 1))
#                     ),
#                     "yyyy-MM-dd'T'HH:mm:ss",
#                 ),
#                 F.when(
#                     F.trim(
#                         F.date_format(
#                             F.from_unixtime(
#                                 F.col("interval_unix_datetime")
#                                 + (F.col("aep_sec_per_intrvl") * (F.col("row_ord") - 1))
#                             ),
#                             "XXX",
#                         )
#                     )
#                     == "-05:00",
#                     F.lit("-06:00"),
#                 ).otherwise(F.lit("-05:00")),
#             ),
#         )
#         .withColumn("scalarfloat", F.col("scalarfloat").cast("float"))
#         .withColumn("aep_raw_value", F.col("aep_raw_value").cast("float"))
#         .withColumn(
#             "value",
#             F.when(
#                 F.col("value_mltplr_flg") == "Y",
#                 F.col("aep_raw_value") * F.col("scalarfloat"),
#             ).otherwise(F.col("aep_raw_value")),
#         )
#         .withColumn(
#             "aep_endtime_utc",
#             (F.col("interval_unix_datetime") + 3600)
#             + (F.col("aep_sec_per_intrvl") * F.col("row_ord")),
#         )
#         .withColumn("hdp_insert_dttm", F.current_timestamp())
#         .withColumn("hdp_update_dttm", F.current_timestamp())
#         .withColumn("hdp_update_user", F.lit(HDP_UPDATE_USER))
#         .withColumn("data_type", F.lit("new"))
#         .withColumn("authority", F.substring(F.col("aep_premise_nb"), 1, 2))
#         .withColumn(
#             "aep_usage_dt",
#             F.substring(
#                 F.date_format(
#                     F.from_unixtime(F.col("interval_unix_datetime")),
#                     "yyyy-MM-dd'T'HH:mm:ss",
#                 ),
#                 1,
#                 10,
#             ),
#         )
#     )

    # ============================================================
    # Staging Layer :  (interval_data_files_tx_xfrm_vw.ddl )
    # ============================================================
    # - Columns toutier, toutiername, aep_billable_ind, aep_data_quality_cd,
    #   aep_data_validation : added in interval_ami_uom_df
    # - Data depulication based on serialnumber, endtimeperiod, aep_channel_id, aep_raw_uom
    # - xfrm_df - used in xfrm_vw_df & iceberg_catalog.xfrm_interval.reading_ivl_nonvee_incr
    # - Creating temporary view of xfrm_df: interval_data_files_tx_xfrm_vw
    # ============================================================
    # interval_data_files_tx_xfrm - below df is equivalent to default.interval_data_files_tx_xfrm_vw
    xfrm_vw = f"interval_data_files_{opco}_xfrm_vw"

    xfrm_df = interval_ami_uom_df.select(
        F.col("serialnumber"),
        F.lit("nonvee-hes").alias("source"),
        F.col("aep_devicecode"),
        F.lit("N").alias("isvirtual_meter"),
        F.when(
            F.trim(
                F.date_format(
                    F.from_unixtime(
                        F.col("interval_unix_datetime")
                        + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
                    ),
                    "XXX",
                )
            ) == "-05:00",
            F.lit("-06:00"),
        ).otherwise(F.lit("05:00")).alias("timezoneoffset"),
        F.col("aep_premise_nb"),
        F.col("aep_service_point"),
        F.col("aep_mtr_install_ts"),
        F.col("aep_mtr_removal_ts"),
        F.col("aep_srvc_dlvry_id"),
        F.col("aep_comp_mtr_mltplr"),
        F.col("name_register"),
        F.lit("N").alias("isvirtual_register"),
        F.col("toutier"),
        F.col("toutiername"),
        F.col("aep_srvc_qlty_idntfr"),
        F.col("aep_channel_id"),
        F.col("aep_raw_uom"),
        (F.col("aep_sec_per_intrvl") * F.lit(60)).alias("aep_sec_per_intrvl"),
        F.col("aep_meter_alias"),
        F.col("aep_meter_program"),
        F.col("aep_billable_ind"),
        F.lit("interval").alias("aep_usage_type"),
        F.lit("US/Central").alias("aep_timezone_cd"),
        F.concat(
            F.date_format(
                F.from_unixtime(
                    F.col("interval_unix_datetime")
                    + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
                ),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
            F.when(
                F.trim(
                    F.date_format(
                        F.from_unixtime(
                            F.col("interval_unix_datetime")
                            + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))
                        ),
                        "XXX",
                    )
                )
                == "-05:00",
                F.lit("-06:00"),
            ).otherwise(F.lit("-05:00")),
        ).alias("endtimeperiod"),
        F.concat(
            F.date_format(
                F.from_unixtime(
                    F.col("interval_unix_datetime")
                    + (F.col("aep_sec_per_intrvl") * (F.col("row_ord") - F.lit(1)))
                ),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
            F.when(
                F.trim(
                    F.date_format(
                        F.from_unixtime(
                            F.col("interval_unix_datetime")
                            + (F.col("aep_sec_per_intrvl") * (F.col("row_ord") - F.lit(1)))
                        ),
                        "XXX",
                    )
                ) == "-05:00",
                F.lit("-06:00"),
            ).otherwise(F.lit("-05:00")),
        ).alias("starttimeperiod"),
        F.when(
            F.col("value_mltplr_flg") == "Y",
            F.col("aep_raw_value") * F.col("scalarfloat"),
        ).otherwise(F.col("aep_raw_value")).alias("value"),
        F.col("aep_raw_value").cast("float").alias("aep_raw_value"),
        F.col("scalarfloat").cast("float").alias("scalarfloat"),
        F.col("aep_data_quality_cd"),
        F.col("aep_data_validation"),
        F.col("aep_acct_cls_cd"),
        F.col("aep_acct_type_cd"),
        F.col("aep_mtr_pnt_nb"),
        F.col("aep_tarf_pnt_nb"),
        ((F.col("interval_unix_datetime") + 3600) + (F.col("aep_sec_per_intrvl") * F.col("row_ord"))).alias("aep_endtime_utc"),
        F.col("aep_city"),
        F.col("aep_zip"),
        F.col("aep_state"),
        F.current_timestamp().alias("hdp_insert_dttm"),
        F.current_timestamp().alias("hdp_update_dttm"),
        F.substring(F.col("aep_premise_nb"), 1, 2).alias("authority"),
        F.col("aep_derived_uom"),
        F.lit("new").alias("data_type"),
        F.col("aep_opco"),
        F.substring(
            F.date_format(
                F.from_unixtime(F.col("interval_unix_datetime")),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
            1,
            10,
        ).alias("aep_usage_dt"),
        F.col("aep_meter_bucket"),
    )

    print(f'===== staging xfrm_df data =====')
    _time = time.time()
    xfrm_staging_path = f'{scratch_path}/xfrm_stage/'
    xfrm_df.write.mode("overwrite").partitionBy("aep_usage_dt").parquet(xfrm_staging_path)
    print(f'completed writing in {str(timedelta(seconds=time.time() - _time))}')
    staged_xfrm_df = spark.read.parquet(xfrm_staging_path)

    _time = time.time()
    print(f'===== counting staged_xfrm_df records =====')
    staged_xfrm_df_count = staged_xfrm_df.count()
    staged_xfrm_df_partition_count = staged_xfrm_df.rdd.getNumPartitions()
    print(f'counted {staged_xfrm_df_count} record(s) in {staged_xfrm_df_partition_count} partition(s)')
    print(f'completed counting in {str(timedelta(seconds=time.time() - _time))}')

    # Deduplication
    latest_upd_w = Window.partitionBy([
        "aep_usage_dt", "aep_meter_bucket", "serialnumber", "aep_raw_uom", "endtimeperiod",
    ]).orderBy(F.desc("hdp_insert_dttm"))
    deduped_xfrm_df = staged_xfrm_df.select(
        "*", F.row_number().over(latest_upd_w).alias("upd_rownum")
    ).filter(F.col("upd_rownum") == 1).drop("upd_rownum")

    print(f'===== staging deduped_staged_xfrm_df data =====')
    _time = time.time()
    deduped_xfrm_staging_path = f'{scratch_path}/dedup_xfrm_stage/'
    deduped_xfrm_df.repartition("aep_usage_dt").sortWithinPartitions([
        "aep_meter_bucket",
        "serialnumber",
        "aep_raw_uom",
        "endtimeperiod",
    ]).write.mode(
        "overwrite"
    ).partitionBy("aep_usage_dt").parquet(deduped_xfrm_staging_path)
    print(f'completed writing in {str(timedelta(seconds=time.time() - _time))}')
    staged_deduped_xfrm_df = spark.read.parquet(deduped_xfrm_staging_path)

    _time = time.time()
    print(f'===== counting staged_deduped_xfrm_df records =====')
    staged_deduped_xfrm_df_count = staged_deduped_xfrm_df.count()
    staged_deduped_xfrm_df_partition_count = staged_deduped_xfrm_df.rdd.getNumPartitions()
    print(f'counted {staged_deduped_xfrm_df_count} record(s) in {staged_deduped_xfrm_df_partition_count} partition(s)')
    print(f'completed counting in {str(timedelta(seconds=time.time() - _time))}')

    # ============================================================
    # Consume Layer :  (usage_nonvee.reading_ivl_nonvee.dml)
    # ============================================================
    # - Merge data into iceberg table iceberg_catalog.usage_nonvee.reading_ivl_nonvee_tx
    # - Merge Keys: aep_usage_dt, aep_meter_bucket, serialnumber,
    #   aep_raw_uom, endtimeperiod
    # ============================================================
    raw_dates = staged_deduped_xfrm_df.select("aep_usage_dt").distinct().collect()
    distinct_usage_dates = [r.aep_usage_dt for r in raw_dates]
    usage_dates_str = ",".join([f"'{d}'" for d in distinct_usage_dates])

    t_alias = "t"
    s_alias = "s"
    target_table_name = f'usage_nonvee.reading_ivl_nonvee_{opco}'
    source_view_name = f'interval_data_xfrm_{opco}_vw'
    sql_text = dedent(f"""
    MERGE INTO {target_table_name} AS {t_alias}
    USING {source_view_name} AS {s_alias}
       ON {t_alias}.aep_usage_dt in ({usage_dates_str})
      AND {t_alias}.aep_usage_dt = {s_alias}.aep_usage_dt
      AND {t_alias}.aep_meter_bucket = {s_alias}.aep_meter_bucket
      AND {t_alias}.serialnumber = {s_alias}.serialnumber
      AND {t_alias}.aep_raw_uom = {s_alias}.aep_raw_uom
      AND {t_alias}.endtimeperiod = {s_alias}.endtimeperiod
     WHEN MATCHED THEN UPDATE SET
          {t_alias}.aep_srvc_qlty_idntfr = {s_alias}.aep_srvc_qlty_idntfr
        , {t_alias}.aep_channel_id = {s_alias}.aep_channel_id
        , {t_alias}.aep_sec_per_intrvl = {s_alias}.aep_sec_per_intrvl
        , {t_alias}.aep_meter_alias = {s_alias}.aep_meter_alias
        , {t_alias}.aep_meter_program = {s_alias}.aep_meter_program
        , {t_alias}.aep_usage_type = {s_alias}.aep_usage_type
        , {t_alias}.aep_timezone_cd = {s_alias}.aep_timezone_cd
        , {t_alias}.endtimeperiod = {s_alias}.endtimeperiod
        , {t_alias}.starttimeperiod = {s_alias}.starttimeperiod
        , {t_alias}.value = {s_alias}.value
        , {t_alias}.aep_raw_value = {s_alias}.aep_raw_value
        , {t_alias}.scalarfloat = {s_alias}.scalarfloat
        , {t_alias}.aep_acct_cls_cd = {s_alias}.aep_acct_cls_cd
        , {t_alias}.aep_acct_type_cd = {s_alias}.aep_acct_type_cd
        , {t_alias}.aep_mtr_pnt_nb = {s_alias}.aep_mtr_pnt_nb
        , {t_alias}.aep_comp_mtr_mltplr = {s_alias}.aep_comp_mtr_mltplr
        , {t_alias}.aep_endtime_utc = {s_alias}.aep_endtime_utc
        , {t_alias}.aep_mtr_removal_ts = {s_alias}.aep_mtr_removal_ts
        , {t_alias}.aep_mtr_install_ts = {s_alias}.aep_mtr_install_ts
        , {t_alias}.aep_city = {s_alias}.aep_city
        , {t_alias}.aep_zip = {s_alias}.aep_zip
        , {t_alias}.aep_state = {s_alias}.aep_state
        , {t_alias}.hdp_update_user = 'info-update'
        , {t_alias}.hdp_update_dttm = {s_alias}.hdp_update_dttm
        , {t_alias}.authority = {s_alias}.authority
     WHEN NOT MATCHED THEN INSERT (
          serialnumber
        , source
        , aep_devicecode
        , isvirtual_meter
        , timezoneoffset
        , aep_premise_nb
        , aep_service_point
        , aep_srvc_dlvry_id
        , name_register
        , isvirtual_register
        , toutier
        , toutiername
        , aep_derived_uom
        , aep_raw_uom
        , aep_srvc_qlty_idntfr
        , aep_channel_id
        , aep_sec_per_intrvl
        , aep_meter_alias
        , aep_meter_program
        , aep_billable_ind
        , aep_usage_type
        , aep_timezone_cd
        , endtimeperiod
        , starttimeperiod
        , value
        , aep_raw_value
        , scalarfloat
        , aep_data_quality_cd
        , aep_data_validation
        , aep_acct_cls_cd
        , aep_acct_type_cd
        , aep_mtr_pnt_nb
        , aep_tarf_pnt_nb
        , aep_comp_mtr_mltplr
        , aep_endtime_utc
        , aep_mtr_removal_ts
        , aep_mtr_install_ts
        , aep_city
        , aep_zip
        , aep_state
        , hdp_update_user
        , hdp_insert_dttm
        , hdp_update_dttm
        , authority
        , aep_opco
        , aep_usage_dt
        , aep_meter_bucket
        ) VALUES (
          {s_alias}.serialnumber
        , {s_alias}.source
        , {s_alias}.aep_devicecode
        , {s_alias}.isvirtual_meter
        , {s_alias}.timezoneoffset
        , {s_alias}.aep_premise_nb
        , {s_alias}.aep_service_point
        , {s_alias}.aep_srvc_dlvry_id
        , {s_alias}.name_register
        , {s_alias}.isvirtual_register
        , {s_alias}.toutier
        , {s_alias}.toutiername
        , {s_alias}.aep_derived_uom
        , {s_alias}.aep_raw_uom
        , {s_alias}.aep_srvc_qlty_idntfr
        , {s_alias}.aep_channel_id
        , {s_alias}.aep_sec_per_intrvl
        , {s_alias}.aep_meter_alias
        , {s_alias}.aep_meter_program
        , {s_alias}.aep_billable_ind
        , {s_alias}.aep_usage_type
        , {s_alias}.aep_timezone_cd
        , {s_alias}.endtimeperiod
        , {s_alias}.starttimeperiod
        , {s_alias}.value
        , {s_alias}.aep_raw_value
        , {s_alias}.scalarfloat
        , {s_alias}.aep_data_quality_cd
        , {s_alias}.aep_data_validation
        , {s_alias}.aep_acct_cls_cd
        , {s_alias}.aep_acct_type_cd
        , {s_alias}.aep_mtr_pnt_nb
        , {s_alias}.aep_tarf_pnt_nb
        , {s_alias}.aep_comp_mtr_mltplr
        , {s_alias}.aep_endtime_utc
        , {s_alias}.aep_mtr_removal_ts
        , {s_alias}.aep_mtr_install_ts
        , {s_alias}.aep_city
        , {s_alias}.aep_zip
        , {s_alias}.aep_state
        , 'info-insert'
        , {s_alias}.hdp_insert_dttm
        , {s_alias}.hdp_update_dttm
        , {s_alias}.authority
        , {s_alias}.aep_opco
        , {s_alias}.aep_usage_dt
        , {s_alias}.aep_meter_bucket
        )
    """)
    print(f'===== writing with =====')
    print(sql_text)
    _time = time.time()
    staged_deduped_xfrm_df.createOrReplaceTempView(source_view_name)
    spark.sql(sql_text)
    print(f'completed writing in {str(timedelta(seconds=time.time() - _time))}')

    print(f"===== Merge completed on table {target_table_name} =====")

    # ============================================================
    # Consume Layer:  (xfrm_interval_data_to_downstream.py & insert_reading_ivl_nonvee_incr_info.dml)
    # ============================================================
    # - Delete more then 8 days old data from iceberg_catalog.xfrm_interval.reading_ivl_nonvee_incr
    #   (keep last 7 days data based on run_date)
    # - Append data to table iceberg_catalog.xfrm_interval.reading_ivl_nonvee_incr
    # ============================================================

    # Step 8: Append data to the table reading_ivl_nonvee_incr
    target_incr_table = "xfrm_interval.reading_ivl_nonvee_incr"

    delete_old_data(spark, target_incr_table, opco)

    reading_ivl_nonvee_incr_df = xfrm_df.filter(
        (F.col("data_type") == "new") & (F.col("aep_opco") == opco)
    ).select(
        "serialnumber",
        "source",
        "aep_devicecode",
        "isvirtual_meter",
        "timezoneoffset",
        "aep_premise_nb",
        "aep_service_point",
        "aep_mtr_install_ts",
        "aep_mtr_removal_ts",
        "aep_srvc_dlvry_id",
        "aep_comp_mtr_mltplr",
        "name_register",
        "isvirtual_register",
        "toutier",
        "toutiername",
        "aep_srvc_qlty_idntfr",
        "aep_channel_id",
        "aep_raw_uom",
        "aep_sec_per_intrvl",
        "aep_meter_alias",
        "aep_meter_program",
        "aep_billable_ind",
        "aep_usage_type",
        "aep_timezone_cd",
        "endtimeperiod",
        "starttimeperiod",
        "value",
        "aep_raw_value",
        "scalarfloat",
        "aep_data_quality_cd",
        "aep_data_validation",
        "aep_acct_cls_cd",
        "aep_acct_type_cd",
        "aep_mtr_pnt_nb",
        "aep_tarf_pnt_nb",
        "aep_endtime_utc",
        "aep_city",
        "aep_zip",
        "aep_state",
        F.lit("info-insert").cast("string").alias("hdp_update_user"),
        F.date_format("hdp_insert_dttm", "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp").alias("hdp_insert_dttm"),
        F.date_format("hdp_update_dttm", "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp").alias("hdp_update_dttm"),
        "authority",
        "aep_usage_dt",
        "aep_derived_uom",
        "aep_opco",
        F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss").alias("run_date"),
    )

    reading_ivl_nonvee_incr_df.writeTo(target_incr_table).append()
    print(f"===== Data is successfully appended on table: {target_incr_table} =====")

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
