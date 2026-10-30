# Data Contract: Apparel Retail 360 Pipeline

This is what Lakeflow pipeline must produce: every table and view, the schema it lands in, its type, its key and the columns it outputs. Names and paths come from `src/variables.py`; everything lands in the `apparel_store` catalog. The four source streams (sales, customers, products, stores) and their raw schemas are documented in [SyntheticDataGenerator.md](SyntheticDataGenerator.md), so they are not repeated here.

Expectation actions used below: **fail** stops the update (`expect_or_fail`), **drop** removes the row (`expect_or_drop`), **warn** keeps the row and records the violation in the pipeline metrics (`expect`).

## Overview

| Name | Schema | Type | Reads from | Key |
|-|-|-|-|-|
| `bronze_sales` | `bronze` | Streaming table | `/Volumes/apparel_store/landing/streaming/sales` | `transaction_id` |
| `bronze_customers` | `bronze` | Streaming table | `/Volumes/apparel_store/landing/streaming/customers` | `customer_id` |
| `bronze_products` | `bronze` | Streaming table | `/Volumes/apparel_store/landing/streaming/products` | `product_id` |
| `bronze_stores` | `bronze` | Streaming table | `/Volumes/apparel_store/landing/streaming/stores` | `store_id` |
| `customers_cleaned_stream` | pipeline only | View | `bronze_customers` | `customer_id` |
| `products_cleaned_stream` | pipeline only | View | `bronze_products` | `product_id` |
| `stores_cleaned_stream` | pipeline only | View | `bronze_stores` | `store_id` |
| `sales_cleaned_stream` | pipeline only | View | `bronze_sales` | `transaction_id` |
| `silver_customers` | `silver` | Streaming table (SCD Type 2) | `customers_cleaned_stream` | `customer_id` + `__START_AT` |
| `silver_products` | `silver` | Streaming table (SCD Type 2) | `products_cleaned_stream` | `product_id` + `__START_AT` |
| `silver_stores` | `silver` | Streaming table (SCD Type 2) | `stores_cleaned_stream` | `store_id` + `__START_AT` |
| `silver_sales_transactions` | `silver` | Streaming table | `sales_cleaned_stream` | `transaction_id` |
| `silver_returns_transactions` | `silver` | Streaming table | `sales_cleaned_stream` | `transaction_id` |
| `silver_customers_current` | pipeline only | View | `silver_customers` | `customer_id` |
| `silver_products_current` | pipeline only | View | `silver_products` | `product_id` |
| `silver_stores_current` | pipeline only	| View | `silver_stores` | `store_id` |
| `denormalized_sales_facts` | `gold` | Streaming table | `silver_sales_transactions` + the three current views | `transaction_id` |
| `gold_daily_sales_by_store` | `gold` | Materialized view | `denormalized_sales_facts` + `silver_stores_current` | `sale_date` + `store_id` |
| `gold_product_performance` | `gold` | Materialized view | `denormalized_sales_facts` + `silver_products_current` | `product_id` |
| `gold_customer_lifetime_value` | `gold` | Materialized view | `denormalized_sales_facts` + `silver_customers_current` | `customer_id` |

## Bronze (`bronze`)


