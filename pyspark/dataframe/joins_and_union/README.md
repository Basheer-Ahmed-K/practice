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


# PySpark Union Operation

1. **Creating DataFrames**: Two DataFrames, `df1` and `df2`, are created using sample data.
2. **Displaying DataFrames**: The content of both DataFrames is displayed using the `show()` method.
3. **Union with Unique Values**: The `union()` method is applied to combine the rows of `df1` and `df2`. The `distinct()` method is used to remove duplicate rows, and the result is displayed.
4. **Counting Union with Unique Values**: The count of rows in the union with unique values is printed.
5. **Union with Duplicate Values**: The `unionAll()` method is applied to combine the rows of `df1` and `df2` without removing duplicates, and the result is displayed.
6. **Counting Union with Duplicate Values**: The count of rows in the union with duplicate values is printed.
