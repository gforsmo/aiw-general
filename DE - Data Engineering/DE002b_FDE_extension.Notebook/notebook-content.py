# Fabric notebook source


# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("office_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("num_employees", IntegerType(), True),
    StructField("opened_date", StringType(), True)
])
rows = [
    (1, "Main HQ", "100 Main St", "Seattle", "WA", "98101", 120, "2010-01-15"),
    (2, "West Office", "200 West St", "San Francisco", "CA", "94105", 45, "2015-06-01"),
    (3, "East Office", "300 East Ave", "Boston", "MA", "02110", 30, "2018-09-20"),
    (4, "EMEA Office", "400 Euro Rd", "London", "UK", "EC1A1AA", 60, "2012-03-12"),
    (5, "APAC Office", "500 Asia Blvd", "Singapore", "SG", "018989", 25, "2019-11-05")
]
office_df = spark.createDataFrame(rows, schema=schema)
office_df.createOrReplaceTempView("office_df_view")
display(office_df)

# CELL ********************

# Write office_df to OneLake ABFS path using OVERWRITE mode
output_path = "abfss://6bc8e025-b5b1-4351-bc66-23426e1cbfd9@onelake.dfs.fabric.microsoft.com/adf0d382-fbd1-4cca-9364-29ff29826f06/Tables/dbo/office_locations"
office_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
print(f"Wrote office_df to {output_path}")
# Optional: read back a sample to verify
display(spark.read.format("delta").load(output_path).limit(5))

# CELL ********************


# CELL ********************


# CELL ********************

