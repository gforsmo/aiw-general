# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2bd3ae2-9957-40d9-88cc-b3637c54a12c",
# META       "default_lakehouse_name": "DA001_LH_MachineData",
# META       "default_lakehouse_workspace_id": "8c7c0d3f-ea97-4cf4-92a8-066dc54fa435",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2bd3ae2-9957-40d9-88cc-b3637c54a12c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # DA001 🟢 Fundamentals of Data Agents
# 
# >  **Note**: this notebook is provided for educational purposes, for members of the [Fabric AI Workflows community](https://skool.com/fabricai). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# ## Purpose
# In this notebook, we will extract data from an Azure storage account, and transform it ready for a Fabric Data Agent. The purpose of this lesson is not to learn Fabric Data Engineering, we are just using this notebook to prepare some data for our agent. 
# 
# ## Prerequisites
# - You should have created a Lakehouse (without Schemas), and attached it to this notebook. 
# - In that Lakehouse, you should have uploaded the sas_credentials.json file to the Files area. 

# MARKDOWN ********************

# ## Step 1: Set up and Authentication
# #### Step 1.1: Read SAS token from Lakehouse


# CELL ********************

import json 
content = notebookutils.fs.head("Files/sas_credentials.json")
sas_token = json.loads(content)["token"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1.2: Add HDFS configuration setting to authenticate with the storage account

# CELL ********************

storage_account = "adlswfsdm"
container = "wfsdatamart" 
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set(
    f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net",
    sas_token
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: DimMachines dataset
# 
# #### Step 2.1 Read DimMachines from the source, and display

# CELL ********************

blob_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/DimMachines"

dim_machines_raw = spark.read.parquet(blob_path)

display(dim_machines_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2.2 Write DimMachines to Lakehouse table

# CELL ********************

from pyspark.sql import functions as F

dim_machines_current = (
    dim_machines_raw
        .filter(F.col("IsCurrent") == F.lit(True))
        .select(
            "MachineKey",
            "SerialNumberNK",
            "Denomination",
            "PayoutPercent",
            "PositionId",
            "BankId",
            "DesirabilityTier",
            "ThemeName"
        )
)

(
    dim_machines_current.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dimmachines")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: factmachineplay dataset
# #### Step 3.1: Read FactPlays from source, and display

# CELL ********************

blob_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/FactPlays"

fact_plays_raw = spark.read.parquet(blob_path)

display(fact_plays_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3.2: Transform raw FactPlay dataset

# CELL ********************

# Current machines from Lakehouse dimmachines
dim_current = spark.table("dimmachines").select("MachineKey", "PayoutPercent").distinct()

# Anchor the window on the max PlayCalendarDate in FactPlays
max_play_date = fact_plays_raw.agg(F.max("PlayCalendarDate")).first()[0]

# Define 30-day window
period_end_date = F.lit(max_play_date)
period_start_date = F.date_sub(period_end_date, 30)

# Perform transformation
factmachinedayplay = (
    fact_plays_raw
    .join(dim_current, on="MachineKey", how="inner")
    .filter(F.col("PlayCalendarDate").between(period_start_date, period_end_date))
    .groupBy("MachineKey", "PlayCalendarDate", "PayoutPercent")
    .agg(
        F.lit(1).alias("MachineDays"),
        F.sum("BetAmount").alias("TotalBetAmount")
    )
    .withColumn(
        "TotalTheoHoldAmount",
        F.col("TotalBetAmount") * (F.lit(1) - F.col("PayoutPercent"))
    )
    .drop("PayoutPercent")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3.3 Write transformed FactMachinePlay dataset to Lakehouse table

# CELL ********************

(
    factmachinedayplay.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("factmachinedayplay")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary 
# By this point, you should have two tables in your Lakehouse: 
# - ✅ dimmachines
# - ✅ factmachinedayplay
