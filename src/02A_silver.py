from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create the `customers_cleaned_stream` view
@dp.temporary_view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
@dp.expect("realistic_age", "age >= 18 AND age <= 100")
@dp.expect("realistic_loyalty_points", "loyalty_points >= 0")
@dp.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
def customers_cleaned_stream():
    return spark.readStream.table(BRONZE_CUSTOMERS)


# Create the `products_cleaned_stream` view
@dp.temporary_view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_product_price", "price > 0 AND price < 1000")
@dp.expect("non_negative_stock", "stock_quantity >= 0")
@dp.expect("valid_category", "category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')")
@dp.expect("valid_brand", "brand IS NOT NULL")
def products_cleaned_stream():
    return spark.readStream.table(BRONZE_PRODUCTS)


# Create the `stores_cleaned_stream` view
@dp.temporary_view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect("manager_assigned", "manager <> 'Unknown'")
@dp.expect("valid_status", "status IN ('Open', 'Under Renovation')")
def stores_cleaned_stream():
    return spark.readStream.table(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))


# Create the `sales_cleaned_stream` view
@dp.temporary_view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
@dp.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')")
@dp.expect("realistic_discount", "discount_applied >= 0")
def sales_cleaned_stream():
    return (
        spark.readStream.table(BRONZE_SALES)
        .withColumn("date", F.col("event_time").cast("date"))
        .withWatermark("event_time", "10 minutes")
        # Data delay is controlled by LATENCY_MAX_S in data generator
    )