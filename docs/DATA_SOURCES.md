# Data Sources Guide

This guide explains how to add new data sources for validation suites.

## Overview

Data sources are Python functions that return a pandas DataFrame. They are registered in a central registry so YAML validation suites can reference them by name.

## How It Works

```
YAML Suite                    Query Registry               Snowflake
-----------                   --------------               ---------
data_source: "my_query"  -->  QUERY_REGISTRY["my_query"]  -->  SQL Query
                              returns function                 returns DataFrame
```

## Current Data Sources

| Registry Name | Function | Description |
|---------------|----------|-------------|
| `get_aurora_motor_dataframe` | `get_aurora_motor_dataframe()` | Aurora Motors product data filtered by hierarchy, pricing group, org, plant |
| `get_level_1_dataframe` | `get_level_1_dataframe()` | Sample of 1000 rows from vw_ProductDataAll for baseline validation |
| `get_mg4_dataframe` | `get_mg4_dataframe()` | Material Group 4 data with derived MG4_EXPECTED column |

## Adding a New Data Source

### Step 1: Create the Query Function

Open `core/queries.py` and add your function with the `@register_query` decorator:

```python
@register_query("get_my_data")
def get_my_dataframe() -> pd.DataFrame:
    """
    Brief description of what this data source returns.
    """
    sql = """
        SELECT *
        FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
        WHERE YOUR_FILTER_CONDITIONS
    """
    return run_query(sql)
```

### Step 2: Reference in YAML

Create or update your YAML validation suite to use the new data source:

```yaml
metadata:
  suite_name: My_Validation_Suite
  index_column: MATERIAL_NUMBER
  data_source: get_my_data  # Must match @register_query name exactly
  description: Validation for my specific use case

validations:
  - type: expect_column_values_to_not_be_null
    columns:
      - MATERIAL_NUMBER
      - DESCRIPTION
```

### Step 3: Verify Registration

You can verify your data source was registered:

```python
from core.queries import QUERY_REGISTRY

# List all registered data sources
print(QUERY_REGISTRY.keys())
# Output: dict_keys(['get_aurora_motor_dataframe', 'get_level_1_dataframe', 'get_mg4_dataframe', 'get_my_data'])

# Check if your data source exists
print("get_my_data" in QUERY_REGISTRY)
# Output: True
```

## Complete Example

Here's a complete example of adding a new data source for validating motor parts:

### 1. Add to core/queries.py

```python
@register_query("get_motor_parts_dataframe")
def get_motor_parts_dataframe() -> pd.DataFrame:
    """
    Return motor parts data for parts validation.
    Filters to specific material groups and active status.
    """
    sql = """
        SELECT
            MATERIAL_NUMBER,
            DESCRIPTION,
            MATERIAL_GROUP_1,
            MATERIAL_GROUP_2,
            SALES_ORGANIZATION,
            PLANT,
            STORAGE_LOCATION,
            SALES_STATUS,
            STANDARD_PRICE,
            GROSS_WEIGHT,
            NET_WEIGHT
        FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
        WHERE MATERIAL_GROUP_1 = 'MOTOR'
          AND MATERIAL_GROUP_2 IN ('PARTS', 'COMPONENTS')
          AND SALES_STATUS NOT IN ('13', 'DR')
    """
    return run_query(sql)
```

### 2. Create validation_yaml/Motor_Parts_Validation.yaml

```yaml
metadata:
  suite_name: Motor_Parts_Validation
  index_column: MATERIAL_NUMBER
  data_source: get_motor_parts_dataframe
  description: Validation rules for motor parts and components

validations:
  # Required fields
  - type: expect_column_values_to_not_be_null
    columns:
      - MATERIAL_NUMBER
      - DESCRIPTION
      - STANDARD_PRICE

  # Price must be positive
  - type: expect_column_values_to_be_between
    columns:
      - STANDARD_PRICE
    min_value: 0.01
    max_value: 999999

  # Weight check
  - type: expect_column_pair_values_a_to_be_greater_than_b
    column_a: GROSS_WEIGHT
    column_b: NET_WEIGHT
    or_equal: true
```

### 3. Run the validation

The validation can now be run through:
- The YAML Editor page in Streamlit
- Programmatically via `gx_runner.run_validation_from_yaml()`

## Best Practices

### Query Design

1. **Select only needed columns** - Don't use `SELECT *` if you only need a few columns
2. **Add appropriate filters** - Reduce data volume with WHERE clauses
3. **Include the INDEX_COLUMN** - Your query must return the column specified in YAML's `index_column`
4. **Consider row limits** - For development, add `LIMIT 1000` to speed up testing

### Naming Conventions

- Function name: `get_<descriptive_name>_dataframe`
- Registry name: Same as function name (e.g., `"get_motor_parts_dataframe"`)
- Use snake_case for consistency

### Documentation

Always add a docstring explaining:
- What data the function returns
- Any important filters applied
- Expected row count (approximate)

## Troubleshooting

### "Unknown data source: 'my_query'"

**Cause:** The name in YAML doesn't match the `@register_query()` decorator.

**Fix:** Ensure exact match:
```python
@register_query("get_my_data")  # This name...
```
```yaml
data_source: get_my_data  # ...must match this exactly
```

### "Column 'X' not found in DataFrame"

**Cause:** Your YAML references a column that doesn't exist in the query result.

**Fix:** Check your SQL query returns the column, or fix the column name in YAML.

### Query returns empty DataFrame

**Cause:** Your WHERE clause filters out all rows.

**Fix:**
1. Test your SQL directly in Snowflake
2. Loosen filters for testing
3. Check for case sensitivity in string comparisons

### Import errors when adding new function

**Cause:** The module wasn't reloaded after adding the function.

**Fix:** Restart the Streamlit application to reload `core/queries.py`.

## See Also

- `docs/templates/validation_template.py` - Template for Python-based validation suites
- `docs/TROUBLESHOOTING.md` - General troubleshooting guide
- `validation_yaml/` - Example YAML validation suites
