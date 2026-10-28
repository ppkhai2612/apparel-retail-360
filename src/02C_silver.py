from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create the `silver.silver_sales_transactions` table
@dp.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
@dp.expect_or_drop("valid_discount", "discount_applied >= 0")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_sales_transactions():
    return (
        spark.readStream.table(SALES_CLEANED_STREAM)
        .filter(F.col("quantity") > 0)
    )


# Create the `silver.silver_returns_transactions` table
@dp.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
@dp.expect_or_drop("valid_discount", "discount_applied >= 0")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_returns_transactions():
    return (
        spark.readStream.table(SALES_CLEANED_STREAM)
        .filter(F.col("quantity") < 0)
        .withColumn("returned_quantity", F.abs(F.col("quantity")))
        .withColumn("returned_amount", F.abs(F.col("total_amount")))
        .drop("quantity", "total_amount")
    )