# PROJECT PLAN

## Prerequisites

- Intermediate PySpark (DataFrame API).
- Intermediate Databricks knowledge: streaming table, Delta table, Lakeflow, AUTO CDC and stream-static joins.

### Recommended Settings

- **Disable Autocomplete when coding**: Open your profile menu, go to **Settings** -> **Developer**, and under **Code Editor** turn **OFF**:

    - **Automatic Genie Code Autocomplete**
    - **Autocomplete as you type**

## Information

**File Organization**: All the files live in `src/` folder:
- Setup scripts: `environment_setup.ipynb`, `environment_maintenance.ipynb`
- Configuration: `variables.py`
- Data generation: `data_generator.py`
- Source code: `1_bronze.py`, `2A_silver.py`, `2B_silver.py`, `2C_silver.py`, `2D_silver.py`, `3_gold.py`

**Configuration**: Many files in this repository rely on `src/variables.py` to configure your environment and determine where synthetic data is generated.

Resource limits (Free Edition): 




## Databricks Configuration Requirements

To run this project end-to-end, complete the following setup steps in your Databricks workspace:
1. **Create a Free Edition account and import this repository as a Git folder**.

2. **Run the `src/environment_setup.ipynb` notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in `src/variables.py` for creating appropriate objects in Unity Catalog.
3. **Run the synthetic data generator (`src/data_generator.py`) before you create the pipeline**

    - Open `src/data_generator.py` in the workspace editor and click **Run**. It installs faker first, then writes a batch to each of the four landing folders every 30 seconds and runs until you stop it. On its first run the sales stream also backfills 30 days of history (about 6,000 sales) before its first live batch of 500.


4. **Create and configure a Lakeflow pipeline**



## How to work on the Lakeflow pipeline

## Implementation

Tasks live in the six `src/0*.py` files: one bronze, four silver, one gold. Work through them in order.

The task order:
- `src/01_bronze.py` - Bronze: Ingest raw data from the landing folders (Tasks 1-4)
- `src/02A_silver.py` - Silver: cleaned views with data quality expectations for all four streams (Tasks 5-8)
- `src/02B_silver.py` - Silver: SCD Type 2 dimensions for customers, products and stores (Tasks 9-11)
- `src/02C_silver.py` - Silver: sales and returns fact tables (Tasks 12-13)
- `src/02D_silver.py` - Silver: current-version views of the three dimensions (Tasks 14-16)
- `src/03_gold.py` - Gold: business-ready aggregations (Tasks 17-20)

### Bronze Layer: Raw Data Ingestion

**Goal**: Ingest raw source data as streams, add metadata for lineage, and enforce critical schema rules. This layer is the foundation for all downstream processing and should guarantee that only structurally valid records enter the pipeline.

**Source Path**: `/Volumes/apparel_store/landing/streaming/`

**Schema**: `bronze`

#### Task 1: Create `bronze.bronze_sales` table

**Business Logic**: Ingest raw sales transactions as a stream, enforce schema by casting columns to correct types, and add metadata (`ingest_timestamp`, `source_file_path`) for auditability. Fail the update if any record is missing a `transaction_id`.

**Steps**:
- Read the landing folder as a Delta stream (the generator writes each folder as a Delta table, not as JSON or CSV files) - use the `RAW_SALES_PATH` variable.
- Add `comment`: "Raw sales data".
- Add `ingest_timestamp` (the time the micro-batch is processed,`current_timestamp()`) and `source_file_path` (`_metadata.file_path`) columns.

    - Why: When a bad row shows up in silver, `source_file_path` tells you which landing file to open and `ingest_timestamp` tells you which update loaded it.

- Cast all columns to their specified types (e.g., `transaction_id` to `int`, `event_time` to `timestamp`, `unit_price` to `double`).

    - Why: Every later layer joins and filters on these type (e.g., `unit_price` as double so price arithmetic works here; in production, money columns are usually `DECIMAL(10,2)` because double cannot store cents exactly).
    
- Set Failure Condition (`valid_transaction_id`): The pipeline must fail if `transaction_id` is NULL.

    - Why: A missing transaction ID means the sale cannot be tracked or joined to other data.

#### Task 2: Create `bronze.bronze_customers` table

**Business Logic**: Ingest raw customer records as a stream, enforce schema and add lineage metadata. Fail the update if any record is missing a `customer_id`.

**Steps**:
- Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_CUSTOMERS_PATH` variable.
- Add `comment`: "Raw customers data from landing zone".
- Add `ingest_timestamp` and `source_file_path` columns.

    - Why: Same lineage columns as Task 1.
    
- Cast all columns to their specified types.

    - Why: These types are the contract every later layer joins and filters on: int `customer_id` and `age`, timestamp `join_date` and `last_update_time`.

- Set Failure Condition (`valid_customer_id`): The pipeline must fail if `customer_id` is NULL.

    - Why: The SCD2 key in silver and every customer join in gold depend on this ID.

#### Task 3: Create `bronze.bronze_products` table

**Business Logic**: Ingest raw product records as a stream, enforce schema and add lineage metadata. Fail the update if any record is missing a `product_id`.

**Steps**:
- Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_PRODUCTS_PATH` variable.
- Add `comment`: "Raw products data".
- Add `ingest_timestamp` and `source_file_path` columns.
    
    - Why: Same lineage columns as Task 1.
    
- Cast all columns to their specified types.

    - Why: These types are the contract later layers filter on: `price` as double for the 0-1,000 price rule in Task 6 (DECIMAL in production, see Task 1), `stock_quantity` as int for the stock rule.
    
- Set Failure Condition (`valid_product_id`): The pipeline must fail if `product_id` is NULL.
        
    - Why: Product ID is required for all product-level analytics and joins.

#### Task 4: Create `bronze.bronze_stores` table

**Business Logic**: Ingest raw store records as a stream, enforce schema and add lineage metadata. Fail the update if any record is missing a `store_id`.

**Steps**:
- Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_STORES_PATH` variable.
- Add `comment`: "Raw stores data".
- Add `ingest_timestamp` and `source_file_path` columns.

    - Why: Same lineage columns as Task 1.

- Cast all columns to their specified types.

    - Why: As in Task 2: int `store_id`, timestamp `open_date` and `last_update_time`.
    
- Set Failure Condition (`valid_store_id`): The pipeline must fail if `store_id` is NULL.

    - Why: Store ID is the key for all store-level analytics and reporting.

### Silver Layer: Cleansed & Conformed Data

**Goal**: Apply incremental data quality rules, cleanse and conform records, and prepare for historical tracking and analytics. This layer improves data reliability and prepares dimensions and facts for business use.

**Schema**: `silver`

#### Task 5: Create the `customers_cleaned_stream` view

**Business Logic**: Cleanse customer data by enforcing valid IDs, and flag invalid emails, outlier ages, negative loyalty points, and invalid gender values. Only drop records with missing IDs. Questionable values stay in the table and show up in the data quality metrics for review.

**Steps**:
- Read the bronze customers stream with `spark.readStream.table` - use the `BRONZE_CUSTOMERS` variable (it holds the schema-qualified name `bronze.bronze_customers`; a bare `bronze_customers` resolves to the pipeline's default schema `silver` and is not found).
- Add `comment`: "QC for customers stream".
- Apply Expectations:

    - Drop (`valid_customer_id`) if `customer_id` is NULL.
    
        - Why: No customer ID means the record can't be used for analytics or marketing.
        
    - Warn (`valid_email`) if `email` is not NULL and does not match the pattern `%@%.%`.

        - Why: An address without '@' bounces every marketing email; a NULL email is allowed, because giving one is optional.
        
    - Warn (`realistic_age`) if `age` is not between 18 and 100.
          
        - Why: An age of 0, 7 or 142 is a birth year's digits typed into the age field; it lands in the wrong age band in marketing reports.
        
    - Warn (`realistic_loyalty_points`) if `loyalty_points` are less than 0.

        - Why: A negative balance is a points reversal applied twice; the loyalty team has to correct it at the source.
        
    - Warn (`valid_gender`) if `gender` is not one of ('Male', 'Female', 'Other'). 

        - Why: A 3-value allow-list catches legacy till codes ('M', 'F') and blanks that would otherwise split gender reports into extra groups.

#### Task 6: Create the `products_cleaned_stream` view

**Business Logic**: Cleanse product data by enforcing valid IDs and realistic prices, and warn on negative stock, invalid categories, or missing brands. Drop only records with a     missing ID or an out-of-range price; everything else is kept and flagged.

**Steps**:
- Read the bronze products stream - use the `BRONZE_PRODUCTS` variable (the schema-qualified name `bronze.bronze_products`).
- Add `comment`: "QC for products stream".
- Apply Expectations:

    - Drop (`valid_product_id`) if `product_id` is NULL.
        
        - Why: Product ID is required for all product-level analytics.
        
    - Keep only rows where 0 < `price` < 1000 (`valid_product_price`, drop the rest).

        - Why: Nothing in this catalog costs over $1,000; a higher price is almost always cents typed as dollars. Dropping a dimension row has a cost: a new product dropped here never reaches `silver_products`, so `gold_product_performance` shows its name as NULL until the source sends a corrected record, and the `denormalized_sales_facts` rows already joined keep a NULL `product_name` for good (Task 17). That is why only the price, which would corrupt revenue, is a drop rule.

    - Warn (`non_negative_stock`) if `stock_quantity` is less than 0.

        - Why: Negative stock is an oversold item; keep the row so merchandising sees it.
        
    - Warn (`valid_category`) if `category` is not one of ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear').

        - Why: 'Kidswear' is a real product line the reference list does not know yet. Warn and keep the row: dropping it would leave the product without a name in gold (see the price rule above), and the count tells merchandising to add the category.
        
    - Warn (`valid_brand`) if `brand` is NULL.

        - Why: A product without a brand still sells; keep it and flag the gap in the catalog feed.

#### Task 7: Create the `stores_cleaned_stream` view

**Business Logic**: Cleanse store data by enforcing valid IDs, and replace missing manager names with 'Unknown' for reporting consistency. Flag vacant manager positions and invalid status, but only drop records with missing IDs.

**Steps**:
- Read the bronze stores stream - use the `BRONZE_STORES` variable (the
        schema-qualified name `01_bronze.bronze_stores`).

- Add `comment`: "QC for stores stream".
- Apply Transformation: Replace NULL values in the `manager` column with the string 'Unknown'.

    - Why: Reports show 'Unknown' instead of a blank manager. The fill also turns a vacancy into a tracked change: `ignore_null_updates` in Task 11 never sees a NULL here, so a vacancy opens a new SCD2 version on purpose (a vacant position is a business event).

- Apply Expectations (post-transformation):

    - Drop (`valid_store_id`) if `store_id` is NULL.

        - Why: Store ID is required for all store-level analytics.

    - Warn (`manager_assigned`) if `manager` is 'Unknown' (the value you just filled in for a NULL).

        - Why: The expectation sees the view's output, after the fill, so it counts vacant positions by the default value; a `manager IS NOT NULL` check here could never fail. A vacant manager position is an accountability gap the operations team must see in the data quality metrics.
        
    - Warn (`valid_status`) if `status` is not one of ('Open', 'Under Renovation').
    
        - Why: A second store system sends 'open' in lowercase; any report that filters `status = 'Open'` silently misses that store, so the count tells you when to normalize the value at the source or here.

#### Task 8: Create the `sales_cleaned_stream` view

**Business Logic**: Cleanse sales data by flagging invalid payment methods and negative discounts (the sales and returns tables drop the negative discounts in Tasks 12-13), and add a derived date column for time-based analytics. Declare a watermark so later stateful operators on this stream have a lateness bound.

**Steps**:
- Read the bronze sales stream - use the `BRONZE_SALES` variable (the schema-qualified name `bronze.bronze_sales`).
- Add `comment`: "QC for sales stream (fact)".
- Apply Transformation: Add a `date` column by casting `event_time` to a date type.

    - Why: Every daily report groups by this date, so derive it once here. Gold carries it through as `sale_date` (Tasks 17-18).

- Apply Watermarking: Set a 10-minute watermark on the `event_time` column.

    - Why: A watermark bounds how late an event may be for STATEFUL streaming operators (windowed aggregations, dropDuplicates, stream-stream joins). Everything downstream here is stateless or a materialized view, so in this pipeline it changes no output; it is declared so that a streaming aggregation added on this view later has a lateness bound. The generator delays events by up to 60 s (`LATENCY_MAX_S`), so 10 minutes leaves headroom.

- Apply Expectations:

    - Warn (`valid_payment_method`) if `payment_method` is not one of ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card').

        - Why: 'CARD' and 'Apple Pay' come from a terminal whose codes are not mapped to the five reporting values yet. The sale is real, so the rule warns and keeps it, and the count tells you when to extend the mapping before those sales fall out of payment-mix reports.
        
    - Warn (`realistic_discount`) if `discount_applied` is less than 0.
        
        - Why: A negative discount is a sign error at the till; summed into a discount report, it cancels out real discounts. Warn here; Tasks 12-13 drop it.

### Silver Layer: SCD Type 2 Dimensions

### Silver Layer: Sales and Returns Facts

### Silver Layer: Current Dimension Views

### Gold Layer

### Working Approach

### Key Concepts to Practice


## Verification

Run these in a notebook or the SQL editor after the pipeline update for that file. Expected values come from the generator's rates (see [SyntheticDataGenerator.md](SyntheticDataGenerator.md)), so they are approximate: counts depend on how long the generator ran, and a 1% rule can still show 0 on a short run (run the generator longer for clearer counts). Stop the generator before you compare counts across tables.

### Bronze (`01_bronze.py`)

```sql
-- row_count: about 6,000 backfilled rows plus 500 per live batch.
-- return_share about 0.05, bad_discount_share about 0.02, bad_payment_share about 0.01.
SELECT COUNT(*) AS row_count,
       ROUND(COUNT_IF(quantity < 0) / COUNT(*), 3) AS return_share,
       ROUND(COUNT_IF(discount_applied < 0) / COUNT(*), 3) AS bad_discount_share,
       ROUND(COUNT_IF(payment_method NOT IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')) / COUNT(*), 3) AS bad_payment_share
FROM apparel_store.bronze.bronze_sales

-- Shows the table's column names and types
DESCRIBE TABLE apparel_store.bronze.bronze_sales
DESCRIBE TABLE apparel_store.bronze.bronze_customers
DESCRIBE TABLE apparel_store.bronze.bronze_products
DESCRIBE TABLE apparel_store.bronze.bronze_stores
```

### Silver data quality (02A views, checked after 02B and 02C)

### 
