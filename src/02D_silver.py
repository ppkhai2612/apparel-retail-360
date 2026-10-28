from pyspark import pipelines as dp
from pyspark.sql import functions as F

from variables import *


# Create `silver_customers_current` view
@dp.temporary_view(name=SILVER_CUSTOMERS_CURRENT, comment="Current customers (SCD2)")
def silver_customers_current():
    return spark.read.table(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())


# Create `silver_products_current` view
@dp.temporary_view(name=SILVER_PRODUCTS_CURRENT, comment="Current products (SCD2)")
def silver_products_current():
    return spark.read.table(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())


# Create `silver_stores_current` view
@dp.temporary_view(name=SILVER_STORES_CURRENT, comment="Current stores (SCD2)")
def silver_stores_current():
    return spark.read.table(SILVER_STORES).filter(F.col("__END_AT").isNull())