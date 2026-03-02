import dlt
from pyspark.sql.functions import *
@dlt.table(
    name="customers_bronze",
)
def customers_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/customers")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
name="orders_bronze"
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/orders")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
    name="order_items_bronze"
)
def order_items_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/order_items")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
    name="products_bronze"
)
def products_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/products")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
    name="sellers_bronze"
)
def sellers_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/sellers")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
    name="payments_bronze"
)
def payments_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/order_payments")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

@dlt.table(
    name="reviews_bronze"
)
def reviews_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/order_reviews")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )

import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="product_category_name_translation_bronze"
)
def product_category_name_translation_bronze():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/dev_catalog/bronze/landing_zone/product_category_name_translation")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .drop("_rescued_data")
    )