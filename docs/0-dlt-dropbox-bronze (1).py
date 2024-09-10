# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Autoloader Ingestion - Dropbox
# MAGIC ***
# MAGIC
# MAGIC This notebook automatically streams in any files that have been landed in the configured volume as a full text key value pair brone quality table with the following schema:  
# MAGIC
# MAGIC * **fullFilePath** 
# MAGIC * **datasource**
# MAGIC * **inputFileName**
# MAGIC * **ingestTime**
# MAGIC * **ingestDate**
# MAGIC * **value**
# MAGIC * **fileMetadata**

# COMMAND ----------

# MAGIC %md
# MAGIC *** 
# MAGIC
# MAGIC Import dlt and the operations and classes defined for the pipeline.  

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable
import os
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import *

##########################
####### operations #######
##########################


# COMMAND ----------

import sys, os
#sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC For development purposes only, uncommment the below code to manually set the Spark Conf Variables using Databricks Widgets.  

# COMMAND ----------

# # used for active development, but not run during DLT execution, use DLT configurations instead
dbutils.widgets.text(name = "volume_path", defaultValue="/Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output", label="Volume Path")

spark.conf.set("workflow_inputs.volume_path", dbutils.widgets.get(name = "volume_path"))

landing_zone_datasource = "/Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output"

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Retreive inputs for the DLT run from the Spark Conf. 

# COMMAND ----------

volume_path = spark.conf.get("workflow_inputs.volume_path")
print(f"""
    volume_path = {volume_path}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Initialize the pipeline as an IngestionDLT class object.  

# COMMAND ----------

# MAGIC %fs ls /Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = spark.conf.get("workflow_inputs.volume_path")
)

# COMMAND ----------

Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Ingest the raw files into a key-value pair bronze table.  

# COMMAND ----------

Pipeline.ingest_raw_to_bronze(
    table_name="row_csv_bronze"
    ,table_comment="A full text record of every file that has landed in our raw synthea landing folder."
    ,table_properties={"quality":"bronze", "phi":"True", "pii":"True", "pci":"False"}
    ,source_folder_path_from_volume=""
    ,file_type = "csv"
)

# COMMAND ----------

Pipeline.list_dropbox_files(
  bronze_table = "row_csv_bronze"
)

# COMMAND ----------

import json
from datetime import datetime, time
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

landing_zone_datasource = "/Volumes/uc_demos_ankur_nayyar/airlinedata/synthetic_files_raw/output"
# landing_zone_datasource = spark.conf.get("landing_zone_datasource")

pipeline_list = pd.DataFrame(
    [
        {'source_folder': table.path.split(':')[1][:-1], 'target_table_name':table.name[:-1]} 
        for table in dbutils.fs.ls(landing_zone_datasource) if table.name[-1]=="/"
    ]
    )

# COMMAND ----------

full_paths = main.recursive_ls(f"{volume_path}")
shortened_names = []
for full_path in full_paths:
    filename = full_path.split(":")[-1]
    shortened_names.append(filename)
filenames_list = list(set(shortened_names))
filenames_list

# COMMAND ----------


sources = {"device":f"{landing_zone_datasource}/output/device",
           "measurement":f"{landing_zone_datasource}/output/measurement",
           "user":f"{landing_zone_datasource}/output/user"           }
sources 

# COMMAND ----------

#For each hospital, read the landing zone for this device table
for source in sources.keys():
  print(source)
  print(sources[source])
  
    

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*), inputFileName from 	uc_demos_ankur_nayyar.default.row_csv_bronze group by inputFileName 

# COMMAND ----------

display(pipeline_list)

# COMMAND ----------

def generate_pipeline(table, table_schema_bronze):
    
    # ingest the data using dataloader
    @dlt.table(
        name = f"{table.target_table_name}_bronze",
        comment = f"this is the table ingested from Libreview",
        table_properties={
            "quality": "bronze",
        }    
    )
    def ingest_data():
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("pathGlobfilter", f"*.csv")
                #.schema(table_schema_bronze)
                .load(table.source_folder)
                .withColumn("inputFilename", col("_metadata.file_name"))
                .withColumn("fullFilePath", col("_metadata.file_path"))
                .withColumn("fileMetadata", col("_metadata"))
                .select( 
                    col("*")
                    ,lit(table.target_table_name).alias("datasource")
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                )
            )

# COMMAND ----------

# generate the pipeline
for table in pipeline_list.itertuples():
    print(table)
    generate_pipeline(table, table.target_table_name+"_schema_bronze")

# COMMAND ----------


