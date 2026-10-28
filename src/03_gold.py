from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create `gold.denormalized_sales_facts` streaming table
@dp.table(
    name=GOLD_DENORMALIZED_SALES_FACTS,
    comment="Sales joined to the current customer, product and store"
)
def denormalized_sales_facts():
    sales = spark.readStream.table(SILVER_SALES_TRANSACTIONS)
    customers = spark.read.table(SILVER_CUSTOMERS_CURRENT)
    products = spark.read.table(SILVER_PRODUCTS_CURRENT)
    stores = spark.read.table(SILVER_STORES_CURRENT)

    df = (
        sales
        .join(stores, "store_id", left)
        .join(stores, "store_id", left)
        .join(stores, "store_id", left)
        .select(
            sales.transaction_id,
            sales.event_time,
            sales["date"].alias("sale_date"),
            sales.customer_id,
            customers.name.alias("customer_name"),
            sales.product_id,
            products.name.alias("product_name"),
            products.category.alias("product_category"),
            sales.store_id,
            stores.name.alias("store_name"),
            sales.quantity,
            sales.unit_price,
            sales.total_amount,
            sales.payment_method,
            sales.discount_applied,
            sales.tax_amount,
        )
    )
    return df


# Create the `gold.gold_daily_sales_by_store` materialized view
@dp.materialized_view(
    name=GOLD_DAILY_SALES_BY_STORE,
    comment="Daily store sales summary"
)
def gold_daily_sales_by_store():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    stores = spark.read.table(SILVER_STORES_CURRENT).select("store_id", F.col("name").alias("store_name"))
    agg = (
        df.groupBy("sale_date", "store_id")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("transaction_id").alias("total_transactions"),
                F.sum("quantity").alias("total_items_sold"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
            .join(stores, "store_id", "left")
            .select(
                "sale_date",
                "store_id",
                "store_name",
                "total_revenue",
                "total_transactions",
                "total_items_sold",
                "unique_customers"
            )
    )
    return agg


# Create the `gold.gold_product_performance` materialized view
@dp.materialized_view(
    name=GOLD_PRODUCT_PERFORMANCE,
    comment="Product sales performance metrics"
)
def gold_product_performance():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    products = spark.read.table(SILVER_PRODUCTS_CURRENT).select(
        "product_id",
        F.col("name").alias("product_name"),
        F.col("category").alias("product_category")
    )
    agg = (
        df.groupBy("product_id")
          .agg(
              F.round(F.sum("total_amount"), 2).alias("total_revenue"),
              F.sum("quantity").alias("total_quantity_sold"),
              F.count("transaction_id").alias("total_orders")
          )
          .join(products, "product_id", "left")
          .select(
              "product_id",
              "product_name",
              "product_category",
              "total_revenue",
              "total_quantity_sold",
              "total_orders"
          )
    )
    return agg


# Create the `gold.gold_customer_lifetime_value` materialized view
@dp.materialized_view(
    name=GOLD_CUSTOMER_LIFETIME_VALUE,
    comment="Customer lifetime value metrics"
)
def gold_customer_lifetime_value():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    customers = spark.read.table(SILVER_CUSTOMERS_CURRENT).select("customer_id", F.col("name").alias("customer_name"))
    agg = (
        df.groupBy("customer_id")
          .agg(
              F.round(F.sum("total_amount"), 2).alias("total_spend"),
              F.countDistinct("transaction_id").alias("total_orders"),
              F.min("event_time").cast("date").alias("first_purchase_date"),
              F.max("event_time").cast("date").alias("last_purchase_date"),
              F.round(F.avg("total_amount"), 2).alias("avg_order_value")
          )
          .join(customers, "customer_id", "left")
          .select(
              "customer_id",
              "customer_name",
              "total_spend",
              "total_orders",
              "first_purchase_date",
              "last_purchase_date",
              "avg_order_value"
          )
    )
    return agg