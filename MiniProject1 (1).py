# Databricks notebook source
spark

# COMMAND ----------

storage_account = ""
application_id = ""
directory_id = ""

spark.conf.set("fs.azure.account.auth.type." + storage_account + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account + ".dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account + ".dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account + ".dfs.core.windows.net", 
               "JeW8Q~XLiDOZVAdXVXiflvVsNshQjVycuRCP2c~Y")  # NOTE: Replace this with your secure secret in real use
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account + ".dfs.core.windows.net", 
               "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")


# COMMAND ----------

customer_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_customers_dataset.csv")

display(customer_df)


# COMMAND ----------

geolocation_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv")

order_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_order_items_dataset.csv")
    

payments_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_order_payments.csv")
    

reviews_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv")
    

# COMMAND ----------

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_orders_dataset.csv")

products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_products_dataset.csv")

sellers_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")\
    .load("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/bronze/olist_sellers_dataset.csv")

# COMMAND ----------

pip install pymongo

# COMMAND ----------


from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "i99val.h.filess.io"
database = "OlistNOSQL_landtruth"
port = "27018"
username = "OlistNOSQL_landtruth"
password = "38bd04e723f0f93854ffa3a6b17724ea4f2d1b0e"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection=mydatabase['product_categories']
mongo_data=pd.DataFrame(list(collection.find()))
mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Cleaning the data**

# COMMAND ----------

from pyspark.sql.functions import col,to_date, datediff,current_date
def clean_dataframe(df,name):
    print("Cleaning"+name)
    return df.dropDuplicates().na.drop("all")

orders_df=clean_dataframe(orders_df,"orders")
display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

orders_df=orders_df.withColumn("order_purchase_timestamp",to_date("order_purchase_timestamp")).withColumn("order_approved_at",to_date("order_approved_at")).withColumn("order_delivered_carrier_date",to_date("order_delivered_carrier_date"))
orders_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import year, month
orders_df=orders_df.withColumn("order_purchase_year",year("order_purchase_timestamp"))
orders_df.display(5)

# COMMAND ----------

orders_df=orders_df.drop("order_purchase_date")

# COMMAND ----------

orders_df.display(5)

# COMMAND ----------

display(orders_df.tail(5))

# COMMAND ----------

products_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINING TABLES

# COMMAND ----------

def rename_columns(df: DataFrame, prefix: str, exclude: list = []) -> DataFrame:
    for col_name in df.columns:
        if col_name not in exclude:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
    return df


# COMMAND ----------

customer_df = rename_columns(customer_df, "cust", exclude=["order_id"])
order_df = rename_columns(order_df, "order", exclude=["order_id"])
products_df = rename_columns(products_df, "prod", exclude=["order_id"])
sellers_df = rename_columns(sellers_df, "seller", exclude=["order_id"])
payments_df = rename_columns(payments_df, "pay", exclude=["order_id"])


# COMMAND ----------

orders_with_customers = orders_df.join(customer_df, on="order_id", how="inner")
orders_with_items = orders_with_customers.join(order_df, on="order_id", how="left")
orders_items_products = orders_with_items.join(products_df, on="order_id", how="left")
orders_items_products_sellers = orders_items_products.join(sellers_df, on="order_id", how="left")
orders_items_products_sellers_payments = orders_items_products_sellers.join(payments_df, on="order_id", how="left")


# COMMAND ----------

clean_df = drop_duplicate_columns(orders_items_products_sellers_payments)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SENDING DATA TO SILVER
# MAGIC

# COMMAND ----------

clean_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/silver/")


# COMMAND ----------

df = spark.read.parquet("abfss://olistdata@olistdatastoragejyo.dfs.core.windows.net/silver/")
df.display()


# COMMAND ----------

