## Array Functions in PySpark

### 1. `array_contains`
- Returns `null` if the array is null, `true` if the array contains the given value, and `false` otherwise.

### 2. `array_position`
- Returns the position of the first occurrence of a specified element in the array. Returns 0 if the element is not present.

### 3. `array_remove`
- Removes all occurrences of an element from an array.

### 4. `array_sort`
- Sorts the input array in ascending order. If `null`, it will be placed at the end of the array.

### 5. `array_union`
- Returns an array of the elements in the union of two arrays, without duplicates.


## Explode and Posexplode Functions in PySpark

### 1. `explode`
- Explodes an array of elements into multiple rows. Each element of the array becomes a separate row in the DataFrame.

### 2. `explode_outer`
- Explodes an array of elements into multiple rows. If the array is null or empty, it returns a single null row.

### 3. `posexplode`
- Explodes an array of elements into multiple rows, along with their respective positions. Each element of the array becomes a separate row, and its position in the array is also included.

### 4. `posexplode_outer`
- Explodes an array of elements into multiple rows, along with their respective positions. If the array is null or empty, it returns a single null row with position null.


