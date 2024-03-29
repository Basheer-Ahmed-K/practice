# Learn Joins with PySpark

This PySpark script demonstrates different types of joins performed on DataFrames using PySpark SQL API.

This script performs various types of joins on three DataFrames:

1. `customer_df`: DataFrame containing customer data.
2. `sales_df`: DataFrame containing sales data.
3. `product_df`: DataFrame containing product data.

## Joins Demonstrated

The following types of joins are demonstrated in the script:

1. **Inner Join:** Joins `customer_df` and `sales_df` based on the common column "customer_id".
2. **Left Join (Left Outer Join):** Joins `customer_df` and `sales_df` with all records from the left DataFrame (`customer_df`).
3. **Right Join (Right Outer Join):** Joins `sales_df` and `product_df` with all records from the right DataFrame (`product_df`).
4. **Full Outer Join (Outer Join):** Joins `customer_df` and `sales_df` with all records from both DataFrames.
5. **Left Semi Join:** Returns only the rows from the left DataFrame (`customer_df`) for which there is a match in the right DataFrame (`sales_df`).
6. **Left Anti Join:** Returns only the rows from the left DataFrame (`customer_df`) for which there is no match in the right DataFrame (`sales_df`).
7. **Cross Join:** Returns the Cartesian product of the two DataFrames `customer_df` and `sales_df`.
