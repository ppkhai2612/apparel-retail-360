from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create `bronze.bronze_sales` table
@dp.table(name=BRONZE_SALES, comment = "Raw sales data")
@dp.expect_or_fail("valid_transaction_id", "transaction_id is not null")
def bronze_sales():
    return (
        spark.readStream.format("delta").load(RAW_SALES_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("transaction_id", F.col("transaction_id").cast("int"))
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("payment_method", F.col("payment_method").cast("string"))
        .withColumn("discount_applied", F.col("discount_applied").cast("double"))
        .withColumn("tax_amount", F.col("tax_amount").cast("double"))
    )


# Create `bronze.bronze_customers` table
@dp.table(name=BRONZE_CUSTOMERS, comment="Raw customers data from landing zone")
@dp.expect_or_fail("valid_customer_id", "customer_id is not null")
def bronze_customers():
    return (
        spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("address", F.col("address").cast("string"))
        .withColumn("join_date", F.col("join_date").cast("timestamp"))
        .withColumn("loyalty_points", F.col("loyalty_points").cast("int"))
        .withColumn("phone_number", F.col("phone_number").cast("string"))
        .withColumn("age", F.col("age").cast("int"))
        .withColumn("gender", F.col("gender").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )


# Create `bronze.bronze_products` table
@dp.table(name=BRONZE_PRODUCTS, comment="Raw products data")
@dp.expect_or_fail("valid_product_id", "product_id is not null")
def bronze_products():
    return (
        spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("category", F.col("category").cast("string"))
        .withColumn("brand", F.col("brand").cast("string"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
        .withColumn("size", F.col("size").cast("string"))
        .withColumn("color", F.col("color").cast("string"))
        .withColumn("description", F.col("description").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )


# Create `bronze.bronze_stores` table
@dp.table(name=BRONZE_STORES, comment="Raw stores data")
@dp.expect_or_fail("valid_store_id", "store_id is not null")
def bronze_stores():
    return (
        spark.readStream.format("delta").load(RAW_STORES_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("address", F.col("address").cast("string"))
        .withColumn("manager", F.col("manager").cast("string"))
        .withColumn("open_date", F.col("open_date").cast("timestamp"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("phone_number", F.col("phone_number").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )