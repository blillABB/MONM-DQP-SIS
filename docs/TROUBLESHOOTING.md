# Troubleshooting Guide

Common issues and solutions when developing validation suites.

## Quick Diagnostic Commands

```bash
# Validate YAML structure without connecting to Snowflake
python scripts/validate_yaml.py validation_yaml/Your_Suite.yaml

# List all registered data sources
python -c "from core.queries import QUERY_REGISTRY; print(list(QUERY_REGISTRY.keys()))"

# Check if a specific data source exists
python -c "from core.queries import QUERY_REGISTRY; print('get_my_data' in QUERY_REGISTRY)"
```

---

## YAML Validation Errors

### "Unknown data source: 'my_query'"

**Cause:** The `data_source` in your YAML doesn't match any registered query function.

**Fix:**
1. Check spelling - names must match exactly (case-sensitive)
2. Verify the function has the `@register_query()` decorator in `core/queries.py`
3. Restart Streamlit if you just added the function

```yaml
# Wrong
data_source: get_aurora_motors  # Typo - missing "_dataframe"

# Correct
data_source: get_aurora_motor_dataframe
```

### "Unknown validation type: 'expect_column_values_to_not_be_nul'"

**Cause:** Typo in the expectation type name.

**Fix:** Check spelling against the supported types:
- `expect_column_values_to_not_be_null` (note: "null" not "nul")
- `expect_column_values_to_be_in_set`
- `expect_column_values_to_not_be_in_set`
- `expect_column_values_to_match_regex`
- `expect_column_values_to_not_match_regex`
- `expect_column_pair_values_a_to_be_greater_than_b`
- `expect_column_pair_values_to_be_equal`
- `expect_column_value_lengths_to_equal`
- `expect_column_value_lengths_to_be_between`
- `expect_column_values_to_be_between`
- `expect_column_values_to_be_unique`
- `expect_compound_columns_to_be_unique`

### "metadata.suite_name is required"

**Cause:** Missing or empty `suite_name` in metadata section.

**Fix:**
```yaml
metadata:
  suite_name: My_Validation_Suite  # Add this
  data_source: get_level_1_dataframe
```

### "'validations' list is empty"

**Cause:** No validation rules defined.

**Fix:** Add at least one validation rule:
```yaml
validations:
  - type: expect_column_values_to_not_be_null
    columns:
      - MATERIAL_NUMBER
```

---

## Column Errors

### "Column 'X' not found in DataFrame"

**Cause:** The column name in YAML doesn't match the actual column in the data.

**Possible issues:**
1. Typo in column name
2. Case mismatch (Snowflake columns are often UPPERCASE)
3. Column doesn't exist in the query result

**Fix:**
1. Check the exact column names in your data source query
2. Run the query in Snowflake to see actual column names
3. Column names are case-sensitive - use exact casing

```yaml
# Wrong
columns:
  - material_number  # Lowercase won't match

# Correct (if Snowflake returns uppercase)
columns:
  - MATERIAL_NUMBER
```

### "INDEX_COLUMN 'Material Number' not found in DataFrame"

**Cause:** The `index_column` in metadata doesn't exist in the data.

**Fix:**
```yaml
metadata:
  index_column: MATERIAL_NUMBER  # Must match actual column name
```

---

## Data Source Errors

### "Data source 'X' returned None"

**Cause:** The query function returned `None` instead of a DataFrame.

**Fix:** Check your query function in `core/queries.py`:
```python
@register_query("get_my_data")
def get_my_dataframe() -> pd.DataFrame:
    sql = "SELECT * FROM ..."
    return run_query(sql)  # Make sure you return the result!
```

### "Data source 'X' returned empty DataFrame"

**Cause:** Your SQL query returned zero rows.

**Fix:**
1. Test your SQL directly in Snowflake
2. Check your WHERE clause filters
3. Verify data exists for your filter conditions

```sql
-- Test in Snowflake first
SELECT COUNT(*) FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
WHERE YOUR_CONDITIONS;
```

### Snowflake connection errors

**Symptoms:**
- "Failed to connect to Snowflake"
- "Authentication failed"
- Browser doesn't open for SSO

**Fix:**
1. **Local development:** Ensure you're on VPN if required
2. **Check credentials:** Set `SNOWFLAKE_USER` environment variable
3. **SSO issues:** Try clearing browser cache or use incognito mode
4. **Docker:** Verify private key file exists and secrets are mounted

---

## Validation Runtime Errors

### Validation runs but 0 expectations evaluated

**Cause:** The `define_expectations()` method didn't add any expectations.

**Debug:**
1. Check your YAML has rules in the `validations` section
2. Run the YAML validator: `python scripts/validate_yaml.py your_suite.yaml`
3. Add debug output:
```python
# In your validation class
def define_expectations(self):
    print(f"DEBUG: Building {len(self._yaml_validations)} expectations")
    super().define_expectations()
```

### "DataFrame is empty. No data to validate."

**Cause:** The data source query returned no rows.

**Fix:**
1. Test your SQL query directly in Snowflake
2. Loosen filters during development
3. Add a `LIMIT` clause for testing:
```sql
SELECT * FROM table WHERE conditions LIMIT 100
```

### Results show NaN or None for unexpected_percent

**Cause:** Division by zero when `element_count` is 0.

**Fix:** This is usually caused by an empty DataFrame. Ensure your query returns data.

---

## Streamlit/UI Errors

### "Import Error" when starting app

**Cause:** Module not found or circular import.

**Fix:**
1. Run from project root: `python app_launcher.py`
2. Check PYTHONPATH includes project root
3. Verify all imports exist

### Session state errors

**Cause:** Accessing session state before it's initialized.

**Fix:** Always check if key exists:
```python
if "aurora_results" not in st.session_state:
    st.session_state["aurora_results"] = None
```

### Cache not updating

**Cause:** Daily cache hasn't refreshed yet.

**Fix:**
1. Use "Clear Cache" button in sidebar
2. Manually delete cache files: `validation_results/cache/`
3. Cache refreshes at midnight, new data available at 6 AM

---

## YAML Syntax Issues

### Incorrect indentation

YAML is whitespace-sensitive. Use consistent indentation (2 spaces recommended).

```yaml
# Wrong - inconsistent indentation
validations:
- type: expect_column_values_to_not_be_null
  columns:
- MATERIAL_NUMBER

# Correct
validations:
  - type: expect_column_values_to_not_be_null
    columns:
      - MATERIAL_NUMBER
```

### Special characters in regex

Regex patterns need proper escaping in YAML:

```yaml
# Wrong - backslash needs escaping
regex: "^\d{13}$"

# Correct - double backslash
regex: "^\\d{13}$"

# Alternative - use single quotes (no escaping needed)
regex: '^\d{13}$'
```

### Lists vs single values

Some fields expect lists, others expect single values:

```yaml
# 'columns' expects a list
columns:
  - MATERIAL_NUMBER
  - DESCRIPTION

# 'column' expects a single value (for not_be_in_set)
column: PROFIT_CENTER

# 'rules' expects a mapping
rules:
  Material Type: [CAT, FERT]
  Industry Sector: [M]
```

---

## Performance Issues

### Validation takes too long

**Fix:**
1. Add row limits to query during development
2. Reduce number of expectations for testing
3. Use caching (results cache after first run)

### Out of memory errors

**Cause:** DataFrame too large for available memory.

**Fix:**
1. Add `LIMIT` to your SQL query
2. Select only needed columns (avoid `SELECT *`)
3. Add more specific WHERE filters

---

## Getting Help

If you're still stuck:

1. **Check the logs:** Look at terminal output for detailed error messages
2. **Validate YAML first:** `python scripts/validate_yaml.py your_suite.yaml`
3. **Test query separately:** Run your SQL in Snowflake directly
4. **Check recent changes:** Did something work before? What changed?

### Useful Debug Commands

```bash
# Full validation with verbose output
python -c "
from validations.base_validation import BaseValidationSuite
validator = BaseValidationSuite.from_yaml('validation_yaml/Your_Suite.yaml')
print(f'Suite: {validator.SUITE_NAME}')
print(f'Rows: {len(validator.df)}')
print(f'Columns: {list(validator.df.columns)}')
"
```

## See Also

- `docs/DATA_SOURCES.md` - How to add new data sources
- `docs/templates/validation_template.py` - Template with all expectation types
- `CLAUDE.md` - Project overview and conventions
