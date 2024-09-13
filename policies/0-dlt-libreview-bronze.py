# Databricks notebook source
import dlt
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, input_file_name, udf, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ByteType, ShortType, TimestampType 
import re

# COMMAND ----------

# """

# DeviceTypeIDs:

 

# DeviceType (40066, 40067): LibreLink

# DeviceType (40026, 40027): LDD

# DeviceType (90001, 90002): ConnectedDevice

# DeviceType (40068) : FSL3App

# DeviceType (40031) : FSL3Reader

# """

# device_type_set = (40067,40066,40068,40026,40027,40031,90001,90002)



# COMMAND ----------

"""
File Path Batch Parsing
"""
pattern = "\d{4}/\d+/\d+/[\d\-]+"

@udf
def extract_batch_udf(file_path):

    res = re.search(pattern, file_path)

    return None if not res else res.group()

# COMMAND ----------

# MAGIC   %fs ls /Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output/

# COMMAND ----------

landingzonePath = "/Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output/"  # spark.conf.get("sourceBucket")


# COMMAND ----------


device_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("DeviceTypeID", IntegerType(), True),
        StructField("SerialNumber", StringType(), True),
        StructField("ModelName", StringType(), True),
        StructField("FirmwareVersion", StringType(), True),
        StructField("SystemTypeNum", ByteType(), True),
        StructField("DeviceTime", StringType(), True),
        StructField("Manufacturer", StringType(), True),
        StructField("Settings_ModelName", StringType(), True),
        StructField("LocalModelName", StringType(), True),
        StructField("Settings_SerialNumber", StringType(), True),
        StructField("HardwareRevision", StringType(), True),
        StructField("FirmwareRevision", StringType(), True),
        StructField("SoftwareRevision", StringType(), True),
        StructField("BLESoftwareRevision", StringType(), True),
        StructField("BLEProtocolRevision", StringType(), True),
        StructField("DefaultInsulinType", StringType(), True),
        StructField("DefaultInsulinBrand", StringType(), True),
    ]
)


user_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("DOB", StringType(), True),
    ]
)


measurement_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("DeviceID", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Type", ByteType(), True),
        StructField("SubType", ByteType(), True),
        StructField("FactoryTimestamp", StringType(), True),
        StructField("RecordNumber", StringType(), True),
        StructField("W1", ShortType(), True),
        StructField("W2", ShortType(), True),
        StructField("Created", TimestampType(), True),
        StructField("D", StringType(), True),
        StructField("ND", StringType(), True),
    ]
)


# COMMAND ----------

# try:

#     loadDate = spark.conf.get("loadDate")

# except Exception as e:

#     loadDate = datetime.utcnow().strftime("%Y/%-m/%-d")


# COMMAND ----------


# for the purposes of this demo, collect those into a list of dicts

schemas = [
    {"table": "measurement", "schema": measurement_schema},
    {"table": "device", "schema": device_schema},
    {"table": "user", "schema": user_schema},
]


# set some basic 'not null' quality expectations for each table

# you can build this out later


m_list_of_notnull_fields = [
    "PatientID",
    "DeviceID",
    "Timestamp",
    "Type",
    "FactoryTimestamp",
    "RecordNumber",
    "Prefix",
]

u_list_of_notnull_fields = ["ID", "Country", "DOB", "Prefix"]

d_list_of_notnull_fields = [
    "PatientID",
    "ID",
    "DeviceTypeID",
    "SerialNumber",
    "FirmwareVersion",
    "Prefix",
]


expectations_measurement = {
    "valid measurement": " IS NOT NULL AND ".join(m_list_of_notnull_fields)
    + " IS NOT NULL"
}

expectations_device = {
    "valid_device": " IS NOT NULL AND ".join(d_list_of_notnull_fields) + " IS NOT NULL"
}

# expectations_device["valid_device_type"] = " OR ".join(
#     [f"DeviceTypeID = {x}" for x in device_type_set]
# )

expectations_user = {
    "valid_user": " IS NOT NULL AND ".join(u_list_of_notnull_fields) + " IS NOT NULL"
}


dedup_measurement = ["PatientID", "DeviceID", "Type", "SubType", "FactoryTimestamp"]

dedup_device = ["PatientID", "ID"]

dedup_user = ["ID"]


expectations = [
    {"table": "measurement", "rules": expectations_measurement},
    {"table": "device", "rules": expectations_device},
    {"table": "user", "rules": expectations_user},
]


deduplications = [
    {"table": "measurement", "dedup": dedup_measurement},
    {"table": "device", "dedup": dedup_device},
    {"table": "user", "dedup": dedup_user},
]



# COMMAND ----------


def generate_bronze_table(tableType, schema):
    @dlt.table(
        name=f"bronze_{tableType}"
        # table_properties={"delta.feature.timestampNtz":"supported"}
    )
    def read_files():

        readPath = "/".join([landingzonePath, tableType])

        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("pathGlobfilter", f"*.csv")
                .schema(schema) 
                .load(readPath)
                .withColumn("inputFilename", col("_metadata.file_name"))
                .withColumn("fullFilePath", col("_metadata.file_path"))
                .withColumn("fileMetadata", col("_metadata"))
                .withColumn("bronze_prefix", extract_batch_udf(col("_metadata").getField("file_path")))
                .select( 
                    col("*")
                    #,lit(table.target_table_name).alias("datasource")
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                )
            )

# COMMAND ----------


tableTypes = ["measurement", "device", "user"]

for tableType in tableTypes:
    schema = list(filter(lambda s: s["table"] == tableType, schemas))[0]["schema"]
    expect = list(filter(lambda s: s["table"] == tableType, expectations))[0]["rules"]
    dedup = list(filter(lambda s: s["table"] == tableType, deduplications))[0]["dedup"]
    generate_bronze_table(tableType, schema)

# COMMAND ----------


# def generate_silver_table(tableType, expect, dedup):
#     @dlt.table(
#         name=f"silver_{tableType}"
#         # table_properties={"delta.feature.timestampNtz":"supported"},
#     )
#     @dlt.expect_all_or_drop(expect)
#     def read_from_bronze():

#         bronze_table = dlt.readStream(f"bronze_{tableType}")

#         fin_lookup = spark.read.table(
#             f"{superiorCatalog}.`rwe-landing`.fin_lookup_table"
#         )

#         return (
#             (
#                 bronze_table.alias("b")
#                 .join(
#                     fin_lookup.alias("f"),
#                     bronze_table["bronze_prefix"] == fin_lookup["prefix"],
#                     how="left",
#                 )
#                 .select("b.*", "f.prefix")
#                 .drop("bronze_prefix")
#             )
#             .filter(col("f.prefix").isNotNull())
#             .dropDuplicates(dedup)
#         )


# def generate_quarantine_table(tableType, expect):
#     @dlt.table(name=f"quarantine_{tableType}")
#     @dlt.expect_all_or_drop(expect)
#     def read_from_bronze_q():

#         bronze_table = dlt.readStream(f"bronze_{tableType}")

#         fin_lookup = spark.read.table(
#             f"{superiorCatalog}.`rwe-landing`.fin_lookup_table"
#         )

#         return (
#             bronze_table.alias("b")
#             .join(
#                 fin_lookup.alias("f"),
#                 bronze_table["bronze_prefix"] == fin_lookup["prefix"],
#                 how="left",
#             )
#             .select("b.*", "f.prefix")
#             .drop("bronze_prefix")
#         ).filter(
#             col("f.prefix").isNotNull()
#         )  # Should this include the records where there is no fin file? Y

#         # return dlt.readStream(f"bronze_{tableType}").withColumnRenamed("bronze_prefix", "Prefix")


# # Use for loop to iterate through the target tables

# tableTypes = ["measurements", "devices", "users"]

# for tableType in tableTypes:

#     schema = list(filter(lambda s: s["table"] == tableType, schemas))[0]["schema"]

#     expect = list(filter(lambda s: s["table"] == tableType, expectations))[0]["rules"]

#     dedup = list(filter(lambda s: s["table"] == tableType, deduplications))[0]["dedup"]

#     generate_bronze_table(tableType, schema)

#     generate_silver_table(tableType, expect, dedup)


# # COMMAND ----------


# # MAGIC %md

# # MAGIC ## Quarantining

# # MAGIC

# # MAGIC The idea of quarantining is that bad records will be written to a separate location. This allows good data to processed efficiently, while additional logic and/or manual review of erroneous records can be defined and executed away from the main pipeline. Assuming that records can be successfully salvaged, they can be easily backfilled into the silver table they were deferred from.

# # MAGIC

# # MAGIC For simplicity, we won't check for duplicate records as we insert data into the quarantine table.


# # COMMAND ----------


# quarantine_measurement_rules = {
#     "invalid measurement": " IS NULL OR ".join(m_list_of_notnull_fields) + " IS NULL"
# }

# quarantine_device_rules = {
#     "invalid device": " IS NULL OR ".join(d_list_of_notnull_fields) + " IS NULL"
# }

# quarantine_device_rules["invalid_device_type"] = " AND ".join(
#     [f"DeviceTypeID != {x}" for x in device_type_set]
# )

# quarantine_user_rules = {
#     "invalid user": " IS NULL OR ".join(u_list_of_notnull_fields) + " IS NULL"
# }


# expectations_q = [
#     {"table": "measurements", "rules": quarantine_measurement_rules},
#     {"table": "devices", "rules": quarantine_device_rules},
#     {"table": "users", "rules": quarantine_user_rules},
# ]


# tableTypes = ["measurements", "devices", "users"]

# for tableType in tableTypes:

#     expect = list(filter(lambda s: s["table"] == tableType, expectations_q))[0]["rules"]

#     generate_quarantine_table(tableType, expect)

# COMMAND ----------


