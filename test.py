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


# Cell 7: Create df_uom_mapping (Reference Table)
# ============================================================================
# Reference: usage_nonvee.uom_mapping
# Used In: stg_nonvee.interval_data_files_oh_src_vw.ddl (lines 86-88)
# Purpose: Maps channel to derived UOM, register name, service quality
# ============================================================================

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

df_uom_mapping = spark.createDataFrame(uom_data, uom_schema)
df_uom_mapping.show(truncate=False)

# Cell 8: First EXPLODE - Interval Array
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (line 68)
# Transformation: LATERAL VIEW EXPLODE(a.Interval_reading) exp_interval
# Purpose: Explode the Interval array (middle level) - one row per interval
# ============================================================================

df_interval_exploded = (
    df_raw
    .select(
        F.trim(F.col("MeterName")).alias("MeterName"),
        F.col("UtilDeviceID"),
        F.col("MacID"),
        F.col("IntervalReadData._IntervalLength").alias("IntervalLength"),
        F.col("IntervalReadData._StartTime").alias("blockstarttime"),
        F.col("IntervalReadData._EndTime").alias("blockendtime"),
        F.col("IntervalReadData._NumberIntervals").alias("NumberIntervals"),
        F.col("IntervalReadData.Interval").alias("interval_arr")
    )
    # LATERAL VIEW EXPLODE - First level
    .withColumn("interval_exploded", F.explode(F.col("interval_arr")))
    .select(
        F.col("MeterName"),
        F.col("UtilDeviceID"),
        F.col("MacID"),
        F.col("IntervalLength"),
        F.col("blockstarttime"),
        F.col("blockendtime"),
        F.col("NumberIntervals"),
        F.col("interval_exploded._EndTime").alias("int_endtime"),
        F.col("interval_exploded._GatewayCollectedTime").alias("int_gatewaycollectedtime"),
        F.col("interval_exploded._BlockSequenceNumber").alias("int_blocksequencenumber"),
        F.col("interval_exploded._IntervalSequenceNumber").alias("int_intervalsequencenumber"),
        F.col("interval_exploded.Reading").alias("reading_arr")
    )
)

df_interval_exploded.printSchema()
df_interval_exploded.show(5, truncate=False)

# Cell 9: Second EXPLODE - Reading Array
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (line 73)
# Transformation: LATERAL VIEW EXPLODE(t.int_reading) exp_reading
# Purpose: Explode the Reading array (inner level) - one row per channel reading
# ============================================================================

df_reading_exploded = (
    df_interval_exploded
    # LATERAL VIEW EXPLODE - Second level (Reading/Channel)
    .withColumn("reading_exploded", F.explode(F.col("reading_arr")))
    .select(
        F.col("MeterName"),
        F.col("UtilDeviceID"),
        F.col("MacID"),
        F.col("IntervalLength"),
        F.col("blockstarttime"),
        F.col("blockendtime"),
        F.col("NumberIntervals"),
        F.col("int_endtime"),
        F.col("int_gatewaycollectedtime"),
        F.col("int_blocksequencenumber"),
        F.col("int_intervalsequencenumber"),
        F.col("reading_exploded._Channel").alias("channel"),
        F.col("reading_exploded._RawValue").alias("rawvalue"),
        F.col("reading_exploded._Value").alias("value"),
        F.col("reading_exploded._UOM").alias("uom")
    )
)

df_reading_exploded.printSchema()
df_reading_exploded.show(10, truncate=False)

# Cell 10: Apply NOT NULL Filter
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (lines 76-81)
# Transformation: WHERE channel IS NOT NULL AND rawvalue IS NOT NULL 
#                 AND value IS NOT NULL AND uom IS NOT NULL
# Purpose: Drop garbage/incomplete records
# ============================================================================

df_filtered = (
    df_reading_exploded
    .filter(
        F.col("channel").isNotNull() &
        F.col("rawvalue").isNotNull() &
        F.col("value").isNotNull() &
        F.col("uom").isNotNull()
    )
)

df_filtered.show(10, truncate=False)

# Cell 11: Calculate starttime, endtime, interval_epoch
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (lines 39-41)
# Transformations:
#   - starttime = from_unixtime(unix_timestamp(endtime) - (IntervalLength * 60))
#   - endtime = from_unixtime(unix_timestamp(int_endtime))
#   - interval_epoch = SUBSTR(int_endtime, -6, 6) (timezone offset)
# ============================================================================

df_with_times = (
    df_filtered
    # Convert timestamp to string for manipulation
    .withColumn("int_endtime_str", F.date_format(F.col("int_endtime"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    # interval_epoch = last 6 characters (timezone offset like -05:00)
    # Since timestamp doesn't have timezone in spark, we'll use a default
    .withColumn("interval_epoch", F.lit("-05:00"))
    
    # starttime = endtime - (IntervalLength * 60 seconds)
    .withColumn(
        "starttime",
        F.from_unixtime(
            F.unix_timestamp(F.col("int_endtime_str"), "yyyy-MM-dd'T'HH:mm:ss")
            - (F.col("IntervalLength").cast("int") * 60),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )
    
    # endtime formatted
    .withColumn(
        "endtime",
        F.from_unixtime(
            F.unix_timestamp(F.col("int_endtime_str"), "yyyy-MM-dd'T'HH:mm:ss"),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )
)

df_with_times.select("MeterName", "IntervalLength", "starttime", "endtime", "interval_epoch").show(5, truncate=False)

# Cell 12: Join with UOM Mapping
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (lines 86-88)
# Transformation: LEFT JOIN usage_nonvee.uom_mapping um 
#                 ON reading.channel = um.aep_channel_id
# Purpose: Get derived UOM, register name, service quality identifier
# ============================================================================

df_with_uom = (
    df_with_times.alias("r")
    .join(
        df_uom_mapping.alias("u"),
        F.col("r.channel").cast("string") == F.col("u.aep_channel_id"),
        "left"
    )
    .select(
        F.col("r.MeterName"),
        F.col("r.UtilDeviceID"),
        F.col("r.MacID"),
        F.col("r.IntervalLength"),
        F.col("r.blockstarttime"),
        F.col("r.blockendtime"),
        F.col("r.NumberIntervals"),
        F.col("r.starttime"),
        F.col("r.endtime"),
        F.col("r.interval_epoch"),
        F.col("r.int_gatewaycollectedtime"),
        F.col("r.int_blocksequencenumber"),
        F.col("r.int_intervalsequencenumber"),
        F.col("r.channel"),
        F.col("r.rawvalue"),
        F.col("r.value"),
        F.lower(F.trim(F.col("r.uom"))).alias("uom"),
        # UOM Mapping fields with NVL defaults
        F.coalesce(F.col("u.aep_derived_uom"), F.lit("UNK")).alias("aep_uom"),
        F.when(
            F.col("u.name_register").isNotNull(),
            F.regexp_replace(F.col("u.name_register"), "mtr_ivl_len", F.col("r.IntervalLength").cast("string"))
        ).otherwise(F.lit("UNK")).alias("name_register"),
        F.coalesce(F.col("u.aep_srvc_qlty_idntfr"), F.lit("UNK")).alias("aep_sqi"),
        F.col("u.value_mltplr_flg")
    )
)

df_with_uom.show(10, truncate=False)

# Cell 13: Apply Date Filter
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (line 70)
# Transformation: WHERE from_unixtime(substr(starttime,0,10)) >= '20150301'
# Purpose: Filter records with starttime >= 2015-03-01
# ============================================================================

df_source_vw = (
    df_with_uom
    .filter(
        F.date_format(F.to_date(F.substring(F.col("starttime"), 1, 10), "yyyy-MM-dd"), "yyyyMMdd").cast("int") >= 20150301
    )
)

df_source_vw.show(10, truncate=False)

# Cell 14: Final Source View - Equivalent to stg_nonvee.interval_data_files_oh_src_vw
# ============================================================================
# Reference: stg_nonvee.interval_data_files_oh_src_vw.ddl (complete view)
# This is the final flattened/exploded/filtered DataFrame
# Ready for next step: Stage (join with meter_premise_macs_ami)
# ============================================================================

df_source_vw.printSchema()
df_source_vw.count()

# Cell 15: View sample data
df_source_vw.select(
    "MeterName",
    "IntervalLength", 
    "starttime",
    "endtime",
    "channel",
    "rawvalue",
    "value",
    "uom",
    "aep_uom",
    "name_register",
    "aep_sqi"
).show(20, truncate=False)
