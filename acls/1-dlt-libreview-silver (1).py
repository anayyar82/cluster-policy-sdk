# Databricks notebook source
import dlt
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, input_file_name, udf, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ByteType, ShortType, TimestampType 
import re

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

device_setting_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("deviceuuid", StringType(), True),
        StructField("DeviceTypeID", IntegerType(), True),
        StructField("readertype", StringType(), True),
        StructField("devicetype", StringType(), True),
        StructField("firmwareversion", StringType(), True),
        StructField("systemtype", StringType(), True),
        StructField("uploadsequece", StringType(), True),
        StructField("uploaddate", StringType(), True),
        StructField("devicenationaility", StringType(), True),  
       ]
)


intermediate_schema = StructType(
    [
        StructField("devicenationality", StringType(), True),
        StructField("readertype", StringType(), True),
        StructField("devicetype", StringType(), True),
        StructField("libretype", StringType(), True),
        StructField("firmwareversion", StringType(), True),
        StructField("settings_SerialNumber", StringType(), True),
        StructField("uploadsequence", StringType(), True),
        StructField("uploaddate", StringType(), True),
        StructField("accountID", StringType(), True),
        StructField("deviceuuid", StringType(), True),
        StructField("userrecorded", StringType(), True),
        StructField("Type", ByteType(), True),
        StructField("SubType", ByteType(), True),
        StructField("factoryrecorded", StringType(), True),
        StructField("RecordNumber", StringType(), True),
        StructField("W1", ShortType(), True),
        StructField("W2", ShortType(), True),
        StructField("Created", TimestampType(), True),
        StructField("D", StringType(), True),
        StructField("ND", StringType(), True),

 ]
)


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
    "RecordNumber"
    ]

u_list_of_notnull_fields = ["ID", "Country", "DOB"]

d_list_of_notnull_fields = [
    "PatientID",
    "ID",
    "DeviceTypeID",
    "SerialNumber",
    "FirmwareVersion"
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


def generate_silver_table(tableType, expect):
    @dlt.table(
        name=f"silver_{tableType}"
        # table_properties={"delta.feature.timestampNtz":"supported"},
    )
    @dlt.expect_all_or_drop(expect)
    def read_from_bronze():

        bronze_table = spark.readStream.table(f"uc_demos_ankur_nayyar.default.bronze_{tableType}")
        

        return (
            (
                bronze_table.select("*")
                .withColumn("sequence_by", col("fileMetadata.file_modification_time"))
            )
        )
# Use for loop to iterate through the target tables

tableTypes = ["measurement", "device", "user"]

for tableType in tableTypes:
    schema = list(filter(lambda s: s["table"] == tableType, schemas))[0]["schema"]
    expect = list(filter(lambda s: s["table"] == tableType, expectations))[0]["rules"]
    generate_silver_table(tableType, expect)

# COMMAND ----------

def generate_quarantine_table(tableType, expect):
    @dlt.table(name=f"quarantine_{tableType}")
    @dlt.expect_all_or_drop(expect)
    def read_from_bronze_q():

        bronze_table = spark.readStream.table(f"uc_demos_ankur_nayyar.default.bronze_{tableType}")

        return (
            (
                bronze_table.select("*")
                .withColumn("sequence_by", col("fileMetadata.file_modification_time"))
            )
        )

# COMMAND ----------


quarantine_measurement_rules = {
    "invalid measurement": " IS NULL OR ".join(m_list_of_notnull_fields) + " IS NULL"
}

quarantine_device_rules = {
    "invalid device": " IS NULL OR ".join(d_list_of_notnull_fields) + " IS NULL"
}

# quarantine_device_rules["invalid_device_type"] = " AND ".join(
#     [f"DeviceTypeID != {x}" for x in device_type_set]
# )

quarantine_user_rules = {
    "invalid user": " IS NULL OR ".join(u_list_of_notnull_fields) + " IS NULL"
}


expectations_q = [
    {"table": "measurement", "rules": quarantine_measurement_rules},
    {"table": "device", "rules": quarantine_device_rules},
    {"table": "user", "rules": quarantine_user_rules},
]


tableTypes = ["measurement", "device", "user"]

for tableType in tableTypes:
    expect = list(filter(lambda s: s["table"] == tableType, expectations_q))[0]["rules"]
    generate_quarantine_table(tableType, expect)

# COMMAND ----------



# COMMAND ----------

dlt.create_streaming_table(
  name = "user_silver_cdc",
  comment = "Target for CDC ingestion.",
)
dlt.apply_changes(
  target = "user_silver_cdc",
  source = "silver_user",
  keys = ["ID"],
  sequence_by = "ingestTime"
)

# COMMAND ----------

dlt.create_streaming_table(
  name = "measurement_silver_cdc",
  comment = "Target for CDC ingestion.",
)
dlt.apply_changes(
  target = "measurement_silver_cdc",
  source = "silver_measurement",
  keys = ["PatientID"],
  sequence_by = "ingestTime"
)

# COMMAND ----------

dlt.create_streaming_table(
  name = "device_silver_cdc",
  comment = "Target for CDC ingestion.",
)
dlt.apply_changes(
  target = "device_silver_cdc",
  source = "silver_device",
  keys = ["PatientID"],
  sequence_by = "ingestTime"
)

# COMMAND ----------

from pyspark.sql import functions as F

@dlt.table(name="device_setting",
                  comment="table join between users and device for further analysis")
def device_setting():
    return (dlt.read("device_silver_cdc")
            .join(dlt.read("user_silver_cdc"), ["id"], "left")
            .select(
            "device_silver_cdc.PatientID", "device_silver_cdc.DeviceTypeID",
            # "device_silver_cdc.DeviceTypeID",
            # "device_silver_cdc.readertype",
            # "device_silver_cdc.devicetype",
            # "device_silver_cdc.firmwareversion",
            # "device_silver_cdc.systemtype",
            # "device_silver_cdc.uploadsequece",
            # "device_silver_cdc.uploaddate",
            # "device_silver_cdc.devicenationaility"
            )
            # .withColumn("user_silver_cdc.ingestTime", F.date_format("user_silver_cdc.ingestTime", "yyyy-MM-dd"))

            )
    
    # 'device_silver_cdc.readertype, 'device_silver_cdc.devicetype, firmwareversion#13703, 'device_silver_cdc.systemtype, 'device_silver_cdc.uploadsequece, 'device_silver_cdc.uploaddate, 'device_silver_cdc.devicenationaility]
#     PatientID	deviceuuid	DeviceTypeID	readertype	devicetype	firmwareversion	systemtype	uploadsequece	 uploaddate	devicenationaility

# COMMAND ----------


