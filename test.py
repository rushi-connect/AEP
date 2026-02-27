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
# STEP 1: 1st EXPLODE on interval_reading
# ------------------------------------------------------------
step1_df = (
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
# STEP 2: 2nd EXPLODE on int_reading + time calculations
# ------------------------------------------------------------
step2_df = (
    step1_df
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
# STEP 3: LEFT JOIN with uom_mapping + final SELECT
# ------------------------------------------------------------
interval_data_files_oh_src_vw_df = (
    step2_df
    .join(uom_mapping_df, step2_df.channel == uom_mapping_df.aep_channel_id, "left")
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
