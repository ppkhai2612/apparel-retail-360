from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create the `silver.silver_customers` table
dp.create_streaming_table(SILVER_CUSTOMERS)
dp.create_auto_cdc_flow(
    target=SILVER_CUSTOMERS,
    source=CUSTOMERS_CLEANED_STREAM,
    keys=["customer_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2,
    track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender']
)


# Create the `silver.silver_products` table
dp.create_streaming_table(SILVER_PRDUCTS)
dp.create_auto_cdc_flow(
    target=SILVER_PRDUCTS,
    source=PRDUCTS_CLEANED_STREAM,
    keys=["product_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2,
    track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size']
)


# Create the `silver.silver_stores` table
dp.create_streaming_table(SILVER_STORES)
dp.create_auto_cdc_flow(
    target=SILVER_STORES,
    source=STORES_CLEANED_STREAM,
    keys=["store_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2,
    track_history_column_list=['name', 'address', 'manager', 'status']
)