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
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # DE008: Transform Customer Reviews with Fabric AI Functions
# 
# >  **Note**: this notebook is provided for educational purposes, for members of the [Fabric AI Workflows community](https://skool.com/fabricai). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# This notebook loads customer reviews from the World Famous Slots (WFS) casino, then uses Fabric AI Functions with PySpark to:
# 
# 1. Detect sentiment for each review
# 2. Generate a drafted response to the review based on Casino Manager guidelines.
# 3. Create English companion fields so Casino staff can read all reviews regardless of original language. 
# 
# ## Prerequisites
# Have you already completed DA003 - Fundamentals of Data Agents? If so, you should have already completed these Prerequisite steps. If not, then please complete them, before starting this workflow. 
# 
# *Prerequisite Requirements*: 
# - Create a Lakehouse in the Workspace, and attach it to this notebook. 
# - In that Lakehouse, you should have uploaded the sas_credentials.json file to the Files area. If you have already completed DA001 - then it's the same SAS Token. 
# 
# The reason we do this is to ensure the SAS Token is not written directly into the notebook. **In an enterprise deployment, it is recommended you use Azure Key Vault for storage of such tokens.** 
# 
# 
# **Get SAS Token:**
# 
# Once you have done this, run this cell to load the sas_token into a variable we can use in this Notebook. 


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

# **Import Libraries**
# 
# Import the Fabric AI Functions module and the Spark SQL functions used in the transformations. The `synapse.ml.aifunc` module registers the `.ai` accessor on Spark DataFrames, which is what enables the single-line AI function calls.

# CELL ********************

import synapse.ml.spark.aifunc as aifunc
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load WFS Customer Reviews
# 
# The WFS Reviews are stored as a Parquet file in the WFS data warehouse on ADLS. The code below reads the reviews from ADLS into a spark dataframe.
# 
# After loading, the notebook takes a random sample of reviews for us to work with in this tutorial.  
# 
# The Reviews table has three columns:
# - *review_id*: Unique identifier for each review
# - *review_text*: The free-text review body (may be in any language)
# - *review_date*: The date the review was posted

# MARKDOWN ********************

# #### Step 1.1: Configure connection to an external tenant (where the WFS datamart data lives)

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

# #### Step 1.2: Read WFS data into Spark dataframe

# CELL ********************

blob_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/FactReviews"

reviews_df = (
    spark.read.parquet(blob_path)
    .orderBy(F.rand())
    .limit(20)
)

print(f"Loaded {reviews_df.count()} reviews")
display(reviews_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Determine Review Sentiment
# 
# Before deciding how to respond to each review, the notebook assigns each review one of three sentiment labels:
# 
# - **Positive**
# - **Neutral**
# - **Negative**
# 
# This step uses Fabric's PySpark `ai.analyze_sentiment()` function to create a new sentiment field on the reviews_df dataframe.
# 
# Also we calculate the CU cost of running sentiment analysis following the MS Fabric Consumption Rate card for Text Analytics. 

# CELL ********************

reviews_df = reviews_df.ai.analyze_sentiment(
    input_col="review_text",
    output_col="sentiment",
    labels=["Positive", "Neutral", "Negative"]
)

print("Sentiment stats:")
sentiment_cu_seconds = (reviews_df.count() / 1000) * 33613.45
print(f"Sentiment CU seconds: {sentiment_cu_seconds:.2f}")


print("Sentiment distribution:")
display(reviews_df.groupBy("sentiment").count().orderBy("sentiment"))

display(reviews_df.select("review_id", "review_text", "sentiment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Generate Draft Responses
# 
# With each review assigned a sentiment, it now drafts a customer-facing response directly from that sentiment:
# 
# - **Positive**: Thank the guest and provide a coupon
# - **Neutral**: Thank the guest for the feedback, explain it helps WFS improve, and provide a coupon
# - **Negative**: Apologize, offer $50 in free slot play, and ask the guest to come back and give WFS another chance
# 
# The drafted response is generated in the **same language as the original review**.

# CELL ********************

reviews_df = reviews_df.withColumn(
    "response_prompt",
    F.when(
        F.col("sentiment") == "Positive",
        F.concat(
            F.lit(
                "Write a brief, warm customer response to this casino review. "
                "Thank the guest for their positive feedback and offer them a coupon for a free meal at the bar. "
                "Keep it to 2-3 sentences. Write the response in the same language as the review. "
                "Sign it from 'The World Famous Slots Team'.\nReview: "
            ),
            F.col("review_text")
        )
    ).when(
        F.col("sentiment") == "Neutral",
        F.concat(
            F.lit(
                "Write a brief, appreciative customer response to this casino review. "
                "Thank the guest for their feedback, explain that it helps World Famous Slots improve, "
                "and offer them a coupon for a free meal at the bar for a future visit. Keep it to 2-3 sentences. "
                "Write the response in the same language as the review. "
                "Sign it from 'The World Famous Slots Team'.\nReview: "
            ),
            F.col("review_text")
        )
    ).otherwise(
        F.concat(
            F.lit(
                "Write a brief, sincere customer recovery response to this casino review. "
                "Acknowledge the guest's frustration, apologize clearly, offer $50 in free slot play, "
                "and ask them to come back and give World Famous Slots another chance. "
                "Keep it to 3-4 sentences. Write the response in the same language as the review. "
                "Sign it from 'The World Famous Slots Team'.\nReview: "
            ),
            F.col("review_text")
        )
    )
)

display(reviews_df.select("review_id", "sentiment", "response_prompt"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews_df = reviews_df.ai.generate_response(
    prompt="{response_prompt}",
    is_prompt_template=True,
    output_col="drafted_response"
)

print("Generate response stats:")
stats = reviews_df.ai.stats.collect()[0]
input_tokens = stats["input_tokens"]
output_tokens = stats["output_tokens"]
cu_seconds = (input_tokens * 13.45 + output_tokens * 53.78) / 1000
print(f"Generate Response CU seconds: {cu_seconds:.2f}")


reviews_df = reviews_df.drop("response_prompt")

display(reviews_df.select("review_id", "review_text", "sentiment", "drafted_response"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Create English Companion Fields and Display Final Results
# 
# The customer-facing response stays in the same language as the original review. To make the results easier for casino managers to read, it creates English companion fields:
# 
# - **review_english**: English translation of the original review text
# - **drafted_response_english**: English translation of the drafted customer response
# 
# The stats for CU usage to translate are displayed.  
# 
# Finally, the DataFrame is displayed, where you can review the draft responses.  From this point the responses could be stored and used in an additional workflow that responds to each review.   

# CELL ********************

review_english_df = (
    reviews_df
    .select("review_id", "review_text")
    .ai.translate(
        to_lang="english",
        input_col="review_text",
        output_col="review_english"
    )
    .select("review_id", "review_english")
)

drafted_response_english_df = (
    reviews_df
    .select("review_id", "drafted_response")
    .ai.translate(
        to_lang="english",
        input_col="drafted_response",
        output_col="drafted_response_english"
    )
    .select("review_id", "drafted_response_english")
)

reviews_df = (
    reviews_df
    .join(review_english_df, on="review_id", how="left")
    .join(drafted_response_english_df, on="review_id", how="left")
)

reviews_df.cache()

print("Translate stats:")
# Count characters for each translated column
review_text_chars = reviews_df.select(F.sum(F.length("review_text"))).collect()[0][0] or 0
drafted_response_chars = reviews_df.select(F.sum(F.length("drafted_response"))).collect()[0][0] or 0
total_translate_chars = review_text_chars + drafted_response_chars
translate_cu = total_translate_chars  * (336134.45 / 1000000)
print(f"Translate CU seconds: {translate_cu:.2f}")

display(
    reviews_df.select(
        "review_id",
        "review_text",
        "review_english",
        "sentiment",
        "drafted_response",
        "drafted_response_english"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next Steps
# 
# Head [back into Skool](https://www.skool.com/fabricai/classroom/817354a5?md=be2e349e5b2845f6a13b792b49ac2468) to complete Task 3 for this exercise, and fort the Reflection. 
