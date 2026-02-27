LEFT JOIN interval data with meter_premise_macs_ami table to enrich meter readings with premise/account details (address, device code, meter program, install/removal timestamps) using metername match and time window validation.
# ============================================================================
# NONVEE UIQ INFO PIPELINE - FILE PROCESSING (OH OPCO)
# ============================================================================
# REFERENCE FILES:
#   Shell Script: source_interval_data_files.sh
#   DDL: stg_nonvee.interval_data_files_oh_src.ddl
#   DDL: stg_nonvee.interval_data_files_oh_src_vw.ddl
#   Reference Table: usage_nonvee.uom_mapping
# ============================================================================

# Cell 1: Configure spark-xml package
%%configure -f
{
    "conf": {
        "spark.jars.packages": "com.databricks:spark-xml_2.12:0.18.0"
    }
}

# Cell 2: Imports and Spark Session
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# Cell 3: Configuration
opco = "oh"
run_datetime = "20260219_213100"

# Source XML files
source_paths = [
    "s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2131/20260219-8394453c-d45c-4040-b489-bca288e7c734-1-1.xml.gz",
    "s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2131/20260219-8394453c-d45c-4040-b489-bca288e7c734-1-10.xml.gz",
    "s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2131/20260219-8394453c-d45c-4040-b489-bca288e7c734-1-100.xml.gz"
]

paths = ",".join(source_paths)

# Cell 4: Define XML Schema
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src.ddl
# This schema matches the INFO XML structure
# ============================================================================

# ------------------------------------------------------------
# Schema Definition (Matching DDL Lines 3-10 - ALL StringType)
# ------------------------------------------------------------
# ============================================================
# SCRIPT 1: stg_nonvee.interval_data_files_oh_src
# Following auth-pipeline approach with proper datatypes
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, TimestampType, DoubleType

# ------------------------------------------------------------
# Schema Definition (auth-pipeline style datatypes)
# ------------------------------------------------------------
# ------------------------------------------------------------
# Schema Definition (INFO DDL structure with proper datatypes)
# DDL Line 10: interval_reading array<struct<endtime,blocksequencenumber,
#              gatewaycollectedtime,intervalsequencenumber,
#              `interval`:array<struct<channel,rawvalue,value,uom,blockendvalue>>>>
# ------------------------------------------------------------
# Schema with DDL column names (all lowercase as per DDL Line 10)
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

xml_paths = "s3://aep-datalake-work-dev/util/intervals/nonvee/oh/src/202602/20260201_0431/*.xml.gz"
part_date = "202602/20260201_0431"

interval_data_files_oh_src_df = (
    spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "MeterData")
    .option("attributePrefix", "_")
    .option("valueTag", "_VALUE")
    .option("nullValue", "")
    .option("mode", "PERMISSIVE")
    .schema(info_xml_schema)
    .load(paths)
)

# Rename to match DDL column names (Lines 3-10)
interval_data_files_oh_src_df = (
    interval_data_files_oh_src_df
    .withColumnRenamed("_MeterName", "metername")              # DDL Line 3
    .withColumnRenamed("_UtilDeviceID", "utildeviceid")        # DDL Line 4
    .withColumnRenamed("_MacID", "macid")                      # DDL Line 5
    .withColumnRenamed("IntervalReadData", "interval_reading") # DDL Line 10
    .withColumn("part_date", f.lit(part_date))                 # DDL Line 12
)

interval_data_files_oh_src_df.printSchema()


# ============================================================
# SCRIPT 2: stg_nonvee.interval_data_files_oh_src_vw
# Source: scripts/ddl/stg_nonvee.interval_data_files_oh_src_vw.ddl
# Called by: scripts/source_interval_data_files.sh
# ============================================================
# 
# LOGIC:
# 1. 1st EXPLODE: Flatten interval_reading array → get int_endtime, int_reading, etc.
# 2. 2nd EXPLODE: Flatten int_reading array → get channel, rawvalue, value, uom
# 3. Time Calculations: starttime = endtime - (IntervalLength * 60 seconds)
# 4. Filter: Remove records where channel/rawvalue/value/uom is NULL
# 5. LEFT JOIN: Join with uom_mapping to get aep_derived_uom, name_register, etc.
#
# ============================================================

from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------
# UOM Mapping Reference Table
# ------------------------------------------------------------
uom_data = [
    ("oh", "1", "KWH", "kwh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "2", "KW", "kw", "kW_del_mtr_ivl_len_min", "DEMAND", "N"),
    ("oh", "3", "KVARH", "kvarh", "kVArh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "4", "KVAR", "kvar", "kVAr_del_mtr_ivl_len_min", "DEMAND", "N"),
    ("oh", "5", "KVAH", "kvah", "kVAh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "100", "KWH", "wh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "Y"),
    ("oh", "101", "KWH", "kwh", "kWh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "102", "KWH", "kwh(rec)", "kWh_rec_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "108", "KVARH", "kvarh", "kVArh_del_mtr_ivl_len_min", "INTERVAL", "N"),
    ("oh", "109", "KVAH", "kvah", "kVAh_del_mtr_ivl_len_min", "INTERVAL", "N"),
]

uom_schema = StructType([
    StructField("aep_opco", StringType()),
    StructField("aep_channel_id", StringType()),
    StructField("aep_derived_uom", StringType()),
    StructField("aep_raw_uom", StringType()),
    StructField("name_register", StringType()),
    StructField("aep_srvc_qlty_idntfr", StringType()),
    StructField("value_mltplr_flg", StringType())
])

uom_mapping_df = spark.createDataFrame(uom_data, uom_schema)

# ------------------------------------------------------------
# 1st EXPLODE: Flatten interval_reading array
# ------------------------------------------------------------
interval_exploded_df = (
    interval_data_files_oh_src_df
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

# ------------------------------------------------------------
# 2nd EXPLODE: Flatten int_reading array + time calculations
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# LEFT JOIN with uom_mapping + final SELECT
# ------------------------------------------------------------
interval_data_files_oh_src_vw_df = (
    reading_exploded_df
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

interval_data_files_oh_src_vw_df.printSchema()

# ============================================================
# SCRIPT 3: stg_nonvee.interval_data_files_oh_stg
# Source: scripts/ddl/stg_nonvee.interval_data_files_oh_stg.ddl
#         scripts/stage_interval_xml_files.sh (Lines 194-272)
# ============================================================
#
# LOGIC:
# 1. Read from interval_data_files_oh_src_vw_df (Script 2)
# 2. Read MACS reference table from Glue catalog
# 3. LEFT JOIN on metername + time window validation
# 4. Rename/transform columns to match DDL (41 columns)
#
# ============================================================

from pyspark.sql import functions as f
from pyspark.sql.types import StringType

# Parameters
VAR_OPCO = "oh"
HDP_UPDATE_USER = "info-insert"
target_companies = ["10", "07"]

# ------------------------------------------------------------
# MACS Reference Table (from Glue Catalog)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# LEFT JOIN with MACS + SELECT (41 columns)
# ------------------------------------------------------------
interval_data_files_oh_stg_df = (
    interval_data_files_oh_src_vw_df
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
        f.lit(VAR_OPCO).alias("aep_opco"),
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
        f.lit(HDP_UPDATE_USER).alias("hdp_update_user"),
        f.col("part_date"),
        f.col("value_mltplr_flg"),
        f.substring(f.trim(f.col("MeterName")), -2, 2).alias("aep_meter_bucket")
    )
)

interval_data_files_oh_stg_df.printSchema()
