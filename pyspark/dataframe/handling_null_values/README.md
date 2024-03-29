# Handling Null Values in PySpark

## Filtering Null Values
- Checking for Specific Column: Identifies rows with null values in a specific column.
- Using Select and Filtering: Utilizes select and filter functions to isolate rows with null values.
- Null Replacement: Replaces null values with a specified default value.

## Filling Null Values
- Filling Null Values: Replaces null values with a default value.
- Null Replacement for Int and String: Replaces null values with zero for integers and "nothing here" for strings.

## Removing Rows with Null Values
- Drop Null Values: Removes rows containing null values from the DataFrame.
- Dropping Selected Columns: Removes rows based on null values in selected columns.

## Handling Duplicates
- Distinct Rows: Retrieves distinct rows from the DataFrame.
- Removing Duplicate Rows: Removes duplicate rows from the DataFrame, optionally considering specific columns.

## Using coalesce Function
- Coalesce Function: Compares specified columns and returns the non-null value.