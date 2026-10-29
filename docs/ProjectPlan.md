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
- Source code: `01_bronze.py`, `02A_silver.py`, `02B_silver.py`, `02C_silver.py`, `02D_silver.py`, `03_gold.py`

**Configuration**: Many files in this repository rely on `src/variables.py` to configure your environment and determine where synthetic data is generated.

**Resource limits (Free Edition)**: `src/data_generator.py` runs until you stop it. About 20 minutes of generator time (about 30 live batches) gives clear counts in [Verification](#verification); stop it after that. A transient "Resources exhausted" error clears after a short wait. If you exceed the daily compute quota, Free Edition shuts compute down for the rest of the day; your data and settings are kept.

## Databricks Configuration Requirements

To run this project end-to-end, complete the following setup steps in your Databricks workspace:
1. **Create a Free Edition account and import this repository as a Git folder**: [see README.nd > How to Start](../README.md#how-to-start).
2. **Run the `src/environment_setup.ipynb` notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in `src/variables.py` for creating appropriate objects in Unity Catalog.
3. **Run the synthetic data generator (`src/data_generator.py`) before you create the pipeline**

    - Open `src/data_generator.py` in the workspace editor and click **Run**. It installs `faker` first, then writes a batch to each of the four landing folders every 30 seconds and runs until you stop it. On its first run the sales stream also backfills 30 days of history (about 6,000 sales) before its first live batch of 500.
    - Nothing lands until `faker` installs and the first batches are written: about 5-6 minutes. Wait until the output shows `Sales stream: Ingested 500 new transactions.` at least twice.
    - Before you start the pipeline, run only the HEALTH CHECK cell of `src/environment_maintenance.ipynb` (the first code cell, right under the title; never use Run all, because the cells further down drop the tables and delete the generator's files): all four counts must be above 0 and `missing_customers`, `missing_products`, `missing_stores` at 0.
    - Keep the generator running about 20 minutes in total: the rare rules in the Silver checks and the NULL product names in the Gold checks need that long to show above 0.
    - If a pipeline update fails with `DELTA_MISSING_TRANSACTION_LOG` (or `DELTA_TABLE_NOT_FOUND` on a `/Volumes/.../landing/streaming/...` path), one landing folder had no data yet: run the generator longer and re-run the update.
    - **Tip**: [SyntheticDataGenerator.md](SyntheticDataGenerator.md) documents the table schemas and the planted data quality issues.

4. **Create and configure a Lakeflow pipeline**

    - In the sidebar click **New > ETL pipeline**, name it, and choose **Add existing assets** (not sample code: that adds its own sample tables to the pipeline). This creates a Lakeflow Spark Declarative Pipelines pipeline (formerly Delta Live Tables / DLT); the rest of this guide calls it the Lakeflow pipeline.
    - Under the pipeline name set **Default catalog** to `apparel_store` and **Default schema** to `silver`.
    - In the same dialog, set the **Pipeline root folder** to this repo's `src/` folder (to change it after the pipeline exists: **Settings > Code assets > Configure paths**). The root folder is added to the Python path, which is how every task file finds `variables.py`.
    - As **source code paths** add exactly these six files, one by one, NOT the whole `src/` folder (the folder also holds `data_generator.py` and the setup notebooks, which must not run inside the pipeline): `src/01_bronze.py`, `src/02A_silver.py`, `src/02B_silver.py`, `src/02C_silver.py`, `src/02D_silver.py`, `src/03_gold.py`. If the pipeline already lists a `transformations` folder or a sample file, remove it from the source code.
    - Free Edition runs pipelines on serverless compute; there is no cluster to configure.
    - The pipeline produces two dataset types: streaming tables (each new input row is processed once: appended, or for an AUTO CDC target applied as an insert or update) and materialized views (results recomputed from their inputs on each update).

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

**Goal**: Turn the cleaned customer, product and store views from 02A into SCD Type 2 history tables, so a past sale can be reported against the attributes valid at the time. The gold tables in this lab report current names (Tasks 17-20); the history answers point-in-time questions, such as who managed a store on a given day, which you can query yourself.

#### Task 9: Create the `silver.silver_customers` table

**Business Logic**: Keep every version of a customer's contact details, so a past sale can be read against the details valid at the time. Only the columns in `track_history_column_list` open a new version, technical columns stay out of the table, and a NULL in a change never overwrites a stored value.

**Steps**:
- Declare the target first with `dp.create_streaming_table(SILVER_CUSTOMERS)`.
        
    - Why: An AUTO CDC flow writes into a streaming table that must already exist in the pipeline graph; the flow itself creates nothing.

- Use `dp.create_auto_cdc_flow` with the `CUSTOMERS_CLEANED_STREAM` variable (`customers_cleaned_stream`) as the source, `keys` set to `["customer_id"]` and `sequence_by` set to `last_update_time`.

    - Why: Customer ID identifies the record whose changes are tracked; `last_update_time` orders the changes, so the most recent change wins even when an older one arrives later.

- Set `track_history_column_list` to `['name', 'email', 'address', 'phone_number', 'gender']`.

    - Why: A new address, email or phone number opens a version, so the history shows the contact details valid at any point in time; `loyalty_points`, `age` and `join_date` update in place.

- Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        
    - Why: `except_column_list` keeps these technical columns out of the target table entirely. History tracking is a separate setting (`track_history_column_list` above); a column that is in the table but not in that list is updated in place without opening a new version.

- Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.

    - Why: `stored_as_scd_type=2` keeps every version; `ignore_null_updates` keeps the stored value when an incoming change carries a NULL.

#### Task 10: Create the `silver.silver_products` table

**Business Logic**: Keep every price and description a product has had, so you can see what it cost on any date. Stock changes update the current version in place, so this table holds no stock history. Technical columns stay out of the table; NULL updates are ignored.

**Steps**:
- Declare the target first with `dp.create_streaming_table(SILVER_PRODUCTS)`.

    - Why: As in Task 9.

- Use `dp.create_auto_cdc_flow` with the `PRODUCTS_CLEANED_STREAM` variable (`products_cleaned_stream`) as the source, `keys` set to `["product_id"]` and `sequence_by` set to `last_update_time`.

    - Why: As in Task 9.

- Set `track_history_column_list` to `['name', 'category', 'brand', 'price', 'description', 'color', 'size']`.

    - Why: A price change (mostly a markdown) or a new description opens a version, so the history shows what a product cost at any point in time; `stock_quantity` updates in place, since a version per stock movement would bury the real changes.

- Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.

    - Why: As in Task 9.

- Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.

    - Why: As in Task 9, every version is kept and a NULL in an incoming change leaves the stored value in place.

#### Task 11: Create the `silver.silver_stores` table

**Business Logic**: Keep every manager and status a store has had, so you can see who ran it and whether it was trading on any date. Technical columns stay out of the table; NULL updates are ignored.

**Steps**:
- Declare the target first with `dp.create_streaming_table(SILVER_STORES)`.

    - Why: As in Task 9.

- Use `dp.create_auto_cdc_flow` with the `STORES_CLEANED_STREAM` variable (`stores_cleaned_stream`) as the source, `keys` set to `["store_id"]` and `sequence_by` set to `last_update_time`.
        
    - Why: As in Task 9.

- Set `track_history_column_list` to `['name', 'address', 'manager', 'status']`.

    - Why: A new manager or status opens a version, so the history shows who ran each store and whether it was trading at any point in time; `phone_number` and `open_date` update in place.

- Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        
    - Why: As in Task 9.

- Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.

    - Why: As in Task 9, every version is kept and a NULL in an incoming change leaves the stored value in place. A vacant manager still opens a new version: Task 7 already turned the NULL into 'Unknown', so `ignore_null_updates` never sees it, and a vacancy is a business event.

### Silver Layer: Sales and Returns Facts

**Goal**: Split the cleaned sales view from 02A into sales and returns fact tables; gold reads the sales one.

#### Task 12: Create the `silver.silver_sales_transactions` table

**Business Logic**: Filter sales records to include only positive quantities (actual sales), and drop records with invalid discounts or missing foreign keys.

**Steps**:
- Read `sales_cleaned_stream` as a stream - use the `SALES_CLEANED_STREAM` variable - and add `comment`: "Stream sales table (positive quantity)".
- Filter to include only records where `quantity > 0`.

    - Why: Negative or zero quantities are not valid sales.
    
- Apply Expectations:

    - Drop (`valid_discount`) if `discount_applied` is less than 0.

        - Why: This is the sign error Task 8 warned about; here it is dropped, so no fact table carries it.
        
    - Drop (`valid_store_id`) if `store_id` is NULL.

        - Why: Store ID is required for all store-level analytics.
        
    - Drop (`valid_customer_id`) if `customer_id` is NULL.
        
        - Why: Customer ID is required for all customer-level analytics.
        
    - Drop (`valid_product_id`) if `product_id` is NULL.

        - Why: Product ID is required for all product-level analytics.

#### Task 13: Create the `silver.silver_returns_transactions` table

**Business Logic**: Filter sales records to include only negative quantities (returns), transform to absolute values for reporting, and drop records with invalid discounts or missing foreign keys. Returns are kept apart so finance can report sales and refunds separately; the gold tables report sales after discounts but before returns.

**Steps**:
- Read `sales_cleaned_stream` as a stream - use the `SALES_CLEANED_STREAM` variable - and add `comment`: "Stream returns table (negative quantity)".
- Filter to include only records where `quantity < 0`.

    - Why: Only negative quantities should be considered returns.
    
- Apply Transformations:

    - Create `returned_quantity` column using the absolute value of `quantity`.

        - Why: A returned quantity reads as a positive count of units back.

    - Create `returned_amount` column using the absolute value of `total_amount`.

        - Why: Refund totals sum as positive amounts. `tax_amount` is left as the source sends it (negative on a return); refunded tax is its negation.
        
    - Drop the original `quantity` and `total_amount` columns.

        - Why: Removes ambiguity in reporting.
    
- Apply the same four "expect or drop" quality rules, with the same names, as the sales transactions table.
    
    - Why: A return with a wrong-sign discount or a missing key would corrupt refund totals just as it would sales.

### Silver Layer: Current Dimension Views

**Goal**: Expose the current version of each SCD2 dimension from 02B as a lookup for the
gold joins. Declare each with `@dp.temporary_view(name=...)`, as in 02A.

#### Task 14: Create `silver_customers_current` view

**Business Logic**: Filter SCD2 customer table for current records (no end date), providing the latest snapshot for analytics and lookups.

**Steps**:
- Read `silver.silver_customers` as a batch source (`spark.read.table`) - use the `SILVER_CUSTOMERS` variable.
- Add `comment`: "Current customers (SCD2)".
- Filter for records where the `__END_AT` column is NULL.

    - Why: Only records without an end date are considered current.

#### Task 15: Create `silver_products_current` view

**Business Logic**: Filter SCD2 product table for current records, ensuring analytics use the latest product attributes.

**Steps**:
- Read `silver.silver_products` as a batch source (`spark.read.table`) - use the `SILVER_PRODUCTS` variable.
- Add `comment`: "Current products (SCD2)".
- Filter for records where the `__END_AT` column is NULL.
    
    - Why: Only records without an end date are considered current.

#### Task 16: Create `silver_stores_current` view

**Business Logic**: Filter SCD2 store table for current records, ensuring analytics use the latest store attributes.

**Steps**:
- Read `silver.silver_stores` as a batch source (`spark.read.table`) - use the `SILVER_STORES` variable.
- Add `comment`: "Current stores (SCD2)".
- Filter for records where the `__END_AT` column is NULL.

    - Why: Only records without an end date are considered current.

### Gold Layer

**Goal**: Create denormalized and aggregated tables for direct use by analysts and BI tools.

**Schema**: `gold`

#### Task 17: Create `gold.denormalized_sales_facts` streaming table

**Business Logic**: Join each sale to the current customer, product and store, and alias the columns for BI tools.

**Steps**:
- Read `silver.silver_sales_transactions` as a stream - use the `SILVER_SALES_TRANSACTIONS` variable. Add `comment`: "Sales joined to the current customer, product and store".
- Read the three current views (`SILVER_CUSTOMERS_CURRENT`, `SILVER_PRODUCTS_CURRENT`, `SILVER_STORES_CURRENT`) as batch lookups (`spark.read.table`).

    - Why: Lookup tables provide the latest dimension attributes for each sale. In a stream-static join each micro-batch of sales is joined to the dimension snapshot as of that moment. A sale whose customer has not reached silver yet is written with a NULL customer_name and is never revisited, and the names on a fact row stay as they were at join time even if the dimension changes later. That is why the joins are left joins, why you may see a few NULL names right after a fresh start, and why Tasks 18-20 group by ID and take the current name from the current views.

- Join Logic: `left` join the sales stream with stores, then customers, then products on their respective IDs.
- Select & Alias Columns: select exactly the columns listed in [DataContract.md > denormalized_sales_facts](DataContract.md#denormalized_sales_facts-streaming-table), in that order (including the `date` from Task 8 carried as `sale_date`, which Task 18 groups by).

    - Why: Three dimensions all have a `name` column; aliasing them (`customer_name`, `product_name`, `store_name`) keeps them apart.

#### Task 18: Create `gold.gold_daily_sales_by_store` materialized view

**Business Logic**: Aggregate denormalized sales facts by store and day, calculating total revenue, transaction count, items sold, and unique customers.

Steps:
- Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch source (`spark.read.table`) and declare the table with `@dp.materialized_view`, which makes it recomputed on each update. Add `comment`: "Daily store sales summary".

    - Why: A distinct count over all history cannot be maintained by a plain streaming aggregation; a materialized view recomputes it correctly.

- Group by:

    - `sale_date`. Why: The date was derived once in Task 8 and carried through Task 17.
    - `store_id`. Why: Group by the ID only, so a store's day stays on one row even if its name changes.

- Calculate Aggregations:

    - `total_revenue`: `round(sum(total_amount), 2)`
    - `total_transactions`: `count(transaction_id)`
    - `total_items_sold`: `sum(quantity)`
    - `unique_customers`: `countDistinct(customer_id)`

    Why: These four numbers replace the store managers' noon email of yesterday's sales.
    
- Add the store name: left-join `silver_stores_current` (`SILVER_STORES_CURRENT`) on `store_id` and take its `name` as `store_name`.

    - Why: The name shown is the store's current one; it is attached after the aggregation so it cannot split a store's totals.

- Final Select: the columns listed in [DataContract.md > gold_daily_sales_by_store](DataContract.md##gold_daily_sales_by_store-materialized-view), in that order.

    - Why: Dashboards and exports are built against the data contract; matching its names and order means a `SELECT *` consumer never breaks.

#### Task 19: Create the `gold.gold_product_performance` materialized view

**Business Logic**: Merchandising's question from the README: which items sell. One row per product, ranked by `total_quantity_sold` or `total_revenue`.

**Steps**:
- Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch source (`spark.read.table`) and declare the table with `@dp.materialized_view`, which makes it recomputed on each update. Add `comment`: "Product sales performance metrics".

    - Why: A materialized view recomputes the totals from the full fact table on each update, so every sale that has landed is counted.

- Group by `product_id` only.

    - Why: A product's sales must stay on one row even if its name or category changes.

- Calculate Aggregations:

    - `total_revenue`: `round(sum(total_amount), 2)`
    - `total_quantity_sold`: `sum(quantity)`
    - `total_orders`: `count(transaction_id)`
    
    Why: `total_revenue` is revenue after discounts, not profit; there is no cost column.

- Add the product attributes: left-join `silver_products_current` (`SILVER_PRODUCTS_CURRENT`) on `product_id` and take its `name` as `product_name` and its `category` as `product_category`; select the columns listed in [DataContract.md > gold_product_performance](DataContract.md#gold_product_performance-materialized-view), in that order.

    - Why: The name and category shown are the product's current ones.

#### Task 20: Create the `gold.gold_customer_lifetime_value` materialized view

**Business Logic**: Marketing's customer ranking from the README, built from the sales instead of a spreadsheet.

**Steps**:
- Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch source (`spark.read.table`) and declare the table with `@dp.materialized_view`, which makes it recomputed on each update. Add `comment`: "Customer lifetime value metrics".

    - Why: A distinct count over all history cannot be maintained by a plain streaming aggregation; a materialized view recomputes it correctly.

- Group by `customer_id` only, then left-join `silver_customers_current` (`SILVER_CUSTOMERS_CURRENT`) on `customer_id` to add its `name` as `customer_name`; select the columns listed in [DataContract.md > gold_customer_lifetime_value](DataContract.md#gold_customer_lifetime_value-materialized-view), in that order.
    
    Why: A customer's spend must stay on one row when their name changes; the name shown is the current one.
    
- Calculate Aggregations:

    - `total_spend`: `round(sum(total_amount), 2)`
    - `total_orders`: `countDistinct(transaction_id)`
    - `first_purchase_date`: `min(event_time)` cast to date.
    - `last_purchase_date`: `max(event_time)` cast to date.
    - `avg_order_value`: `round(avg(total_amount), 2)`
    
    Why: `total_spend` ranks customers; `last_purchase_date` finds the ones who stopped coming back.

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
FROM apparel_store.bronze.bronze_sales;

-- Shows the table's column names and types
DESCRIBE TABLE apparel_store.bronze.bronze_sales;
DESCRIBE TABLE apparel_store.bronze.bronze_customers;
DESCRIBE TABLE apparel_store.bronze.bronze_products;
DESCRIBE TABLE apparel_store.bronze.bronze_stores;
```

### Silver data quality (02A views, checked after 02B and 02C)

Right after `02A_silver.py`, only confirm the update succeeds: the four views are pipeline-only, and nothing reads them until `02B_silver.py` (customers, products, stores) and `02C_silver.py` (sales). After 02B and 02C:
1. Run the table queries below. They are the check that counts.
2. Optionally, click a view (or the table that reads it) in the pipeline graph and open the **Data quality** tab. Metrics for a view may be missing, or may appear once per table that reads it (`sales_cleaned_stream` feeds two tables).

Warn rules keep the row, drop rules remove it. The rates below are over all of a view's records, new and changed together:
- `customers_cleaned_stream`: `valid_gender` fails on about 2% of records, `realistic_age` and `valid_email` on about 1%, `realistic_loyalty_points` on about 1% (only new records carry the gender and age defects).
- `products_cleaned_stream`: `valid_product_price` drops on about 1% (cents typed as dollars); `non_negative_stock` fails on about 2%, `valid_brand` on about 2%, `valid_category` on about 1%.
- `stores_cleaned_stream`: `manager_assigned` fails on 1 in 10 store records or more, because 1 in 5 manager changes leaves a vacancy and a vacant store reports 'Unknown' on every later record until it gets a new manager. `valid_status` fails on under 1%: only a new store or a status change can carry the lowercase status, so it usually shows 0. With about one store record per batch, `manager_assigned` can also show 0 after a short run.
- `sales_cleaned_stream`: `realistic_discount` fails on about 2% of sales, `valid_payment_method` on about 1%.

```sql
SELECT COUNT_IF(gender NOT IN ('Male', 'Female', 'Other')) AS bad_gender,
       COUNT_IF(email IS NOT NULL AND email NOT LIKE '%@%.%') AS bad_email
FROM apparel_store.silver.silver_customers;

SELECT COUNT_IF(price >= 1000) AS price_typos_kept,
       COUNT_IF(category = 'Kidswear') AS unknown_category,
       COUNT_IF(brand IS NULL) AS missing_brand
FROM apparel_store.silver.silver_products;

SELECT COUNT_IF(manager = 'Unknown') AS vacant_manager_rows,
       COUNT_IF(status NOT IN ('Open', 'Under Renovation')) AS bad_status
FROM apparel_store.silver.silver_stores;

SELECT COUNT_IF(payment_method NOT IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')) AS bad_payment
FROM apparel_store.silver.silver_sales_transactions;
```
- `bad_gender`, `bad_email`, `unknown_category` and `missing_brand` are warned and kept, but rare: they may show 0 after a short run (a 6-minute run writes only a few live batches) and are reliably above 0 after about 20 minutes of generator time (about 30 live batches). `price_typos_kept` is always 0 (dropped).
- `vacant_manager_rows`, `bad_status` and `bad_payment` are warn rules too, so these rows are kept: `vacant_manager_rows` is above 0 after a longer run, `bad_status` usually stays 0 (see `valid_status` above), and `bad_payment` is about 1% of the sales rows.
 
### Silver SCD2 dimensions (`02B_silver.py`)

```sql
SELECT customer_id, COUNT(*) AS versions, COUNT_IF(__END_AT IS NULL) AS current_rows
FROM apparel_store.silver.silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Rows come back once the generator has run a few batches (customers with history), and every `current_rows` is exactly 1. Pick one `customer_id` from the result (e.g., 3):

```sql
SELECT name, address, email, phone_number, __START_AT, __END_AT
FROM apparel_store.silver.silver_customers
WHERE customer_id = 3
ORDER BY __START_AT;
```

The name is the same in every version, while the address, email or phone number changes from one version to the next; every version but the last has `__END_AT` set. In `silver_products` a price change opens a version and a stock-only change updates the current version in place.

### Silver facts (`02C_silver.py`)

```sql
SELECT
  (SELECT COUNT(*) FROM apparel_store.bronze.bronze_sales) AS bronze_rows,
  (SELECT COUNT(*) FROM apparel_store.silver.silver_sales_transactions) AS sales_rows,
  (SELECT COUNT(*) FROM apparel_store.silver.silver_returns_transactions) AS return_rows,
  (SELECT MIN(returned_quantity) FROM apparel_store.silver.silver_returns_transactions) AS min_returned_qty;

SELECT COUNT_IF(discount_applied < 0) AS negative_discounts,
       ROUND(COUNT_IF(discount_applied > 0) / COUNT(*), 2) AS discounted_share,
       COUNT_IF(ABS(total_amount - (quantity * unit_price - discount_applied)) > 0.01) AS arithmetic_mismatches
FROM apparel_store.silver.silver_sales_transactions;
```
- `sales_rows` is about 93% of `bronze_rows` (95% are sales, minus the 2% with a negative discount), `return_rows` about 5%, `min_returned_qty` at least 1.
- `negative_discounts` 0, `discounted_share` about 0.30, `arithmetic_mismatches` 0 (`total_amount` is the net line value).

### Silver current views (`02D_silver.py`)

The views are pipeline-only; check the rule they apply on the SCD2 table:

```sql
SELECT COUNT_IF(__END_AT IS NULL) AS current_rows, COUNT(DISTINCT customer_id) AS customers
FROM apparel_store.silver.silver_customers;
```

The two numbers are equal: one current row per customer.

### Gold (`03_gold.py`)

```sql
SELECT
  (SELECT COUNT(*) FROM apparel_store.silver.silver_sales_transactions) AS silver_sales,
  (SELECT COUNT(*) FROM apparel_store.gold.denormalized_sales_facts) AS denormalized,
  (SELECT ROUND(SUM(total_amount), 2) FROM apparel_store.gold.denormalized_sales_facts) AS fact_revenue,
  (SELECT ROUND(SUM(total_revenue), 2) FROM apparel_store.gold.gold_daily_sales_by_store) AS daily_revenue;

SELECT 'customers' AS gold_table, COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_ids FROM apparel_store.gold.gold_customer_lifetime_value
UNION ALL SELECT 'products', COUNT(*) - COUNT(DISTINCT product_id) FROM apparel_store.gold.gold_product_performance
UNION ALL SELECT 'store-days', COUNT(*) - COUNT(DISTINCT sale_date, store_id) FROM apparel_store.gold.gold_daily_sales_by_store;

SELECT sale_date, COUNT(*) AS stores, ROUND(AVG(total_transactions)) AS avg_transactions
FROM apparel_store.gold.gold_daily_sales_by_store
GROUP BY sale_date
ORDER BY sale_date;
```
- `denormalized` equals `silver_sales`; `daily_revenue` matches `fact_revenue` within a few cents (each daily row is rounded to the cent).
- Every `duplicate_ids` is 0: one row per customer, per product and per store-day.
- About 31 dates: 30 backfilled days plus today (a run across midnight UTC adds one). Each backfilled day has 5 stores with about 37 transactions each (40 sales per store-day, minus returns and invalid discounts). Today can show a 6th store or more: the live generator opens a new store about every 10 minutes. Today also holds the live batches at demo speed, so its store rows are many times larger than a backfilled day: compare backfilled days with each other.
- `SELECT COUNT(*) FROM apparel_store.gold.gold_product_performance WHERE product_name IS NULL` returns about 1-2 after 20 minutes of generator time (sometimes 0) and grows by about one for every 10 more minutes: products whose only record was dropped for a bad price (Task 6).
- In `gold_customer_lifetime_value`, almost every one of the 50 starting customers (`customer_id` 1-50) has the first backfilled day as `first_purchase_date`. Each of them shops about 4 times a backfilled day at this demo scale, so one or two may start a day later. Every customer created live has today's date; in `gold_product_performance` the lowest `product_ids` have the highest `total_quantity_sold` (the generator's best-seller curve).