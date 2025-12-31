# Simplified Streamlit UI Approach

## Current Complex Flow
```python
# Old way - complex parsing
results = run_validation_from_yaml_snowflake(yaml_path)

# Complex nested structure
for result in results['results']:
    expectation_type = result['expectation_type']
    column = result['column']
    unexpected_count = result['unexpected_count']
    # ... lots of extraction logic

# Need to parse validation_results array
df = results['full_results_df']
# Parse JSON arrays to get failures
```

## New Simple Flow
```python
# New way - direct DataFrame usage
from validations.snowflake_runner import run_validation_simple
from core.validation_metrics import get_failed_materials, get_summary_table

# Run validation
result = run_validation_simple("validation_yaml/MY_SUITE.yaml")

df = result["df"]              # Raw Snowflake DataFrame
metrics = result["metrics"]    # Simple metrics dict
suite_name = result["suite_name"]

# Display summary metrics
st.header(f"{suite_name} - Validation Report")
st.metric("Overall Pass Rate", f"{metrics['overall_pass_rate']}%")
st.metric("Total Materials", metrics['total_materials'])

# Show summary table
summary_df = get_summary_table(metrics)
st.dataframe(summary_df)

# Filter by specific expectation
st.subheader("Drill Down")
selected_exp = st.selectbox("Select Expectation", df.filter(like='exp_').columns)

# Get failures directly
failures = df[df[selected_exp] == 'FAIL']
st.write(f"Found {len(failures)} failures")
st.dataframe(failures)

# Filter by derived status
derived_cols = [col for col in df.columns if col.startswith('derived_')]
selected_derived = st.selectbox("Select Derived Status", derived_cols)

derived_failures = df[df[selected_derived] == 'FAIL']
st.write(f"Materials with {selected_derived}: {len(derived_failures)}")
st.dataframe(derived_failures[['material_number', 'org_level', selected_derived]])
```

## Benefits

1. **No Complex Parsing** - Work directly with the DataFrame
2. **Simple Filtering** - Use pandas directly: `df[df['exp_a3f'] == 'FAIL']`
3. **Easy Aggregation** - Standard pandas operations
4. **Database Ready** - Can write DataFrame directly to Snowflake table

## Example: Drill-Down Component

```python
import streamlit as st
import pandas as pd

def show_drill_down(df: pd.DataFrame, metrics: dict):
    """Simple drill-down without complex parsing."""

    st.subheader("Expectation Drill-Down")

    # Get all expectation columns
    exp_cols = [col for col in df.columns if col.startswith('exp_')]

    if not exp_cols:
        st.info("No expectations found")
        return

    # Let user select expectation
    selected_exp = st.selectbox("Select Expectation", exp_cols)

    # Get metrics for this expectation
    exp_metrics = metrics['expectation_metrics'].get(selected_exp, {})

    # Display metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Checks", exp_metrics.get('total', 0))
    col2.metric("Failures", exp_metrics.get('failures', 0))
    col3.metric("Pass Rate", f"{exp_metrics.get('pass_rate', 0)}%")

    # Show failures
    failures = df[df[selected_exp] == 'FAIL']

    if not failures.empty:
        st.write(f"**{len(failures)} materials failed this check:**")

        # Show relevant columns (context + the failed column)
        context_cols = ['material_number', 'org_level', 'product_hierarchy']
        display_cols = [col for col in context_cols if col in df.columns]
        display_cols.append(selected_exp)

        st.dataframe(failures[display_cols])

        # Download button
        csv = failures.to_csv(index=False)
        st.download_button(
            "Download Failures",
            csv,
            f"failures_{selected_exp}.csv",
            "text/csv"
        )
    else:
        st.success("No failures for this expectation!")
```

## Example: Derived Status Report

```python
def show_derived_status_report(df: pd.DataFrame, metrics: dict):
    """Show derived status failures."""

    st.subheader("Derived Status Report")

    # Get derived columns
    derived_cols = [col for col in df.columns if col.startswith('derived_')]

    if not derived_cols:
        st.info("No derived statuses defined")
        return

    # Show each derived status
    for derived_col in derived_cols:
        # Clean up label
        label = derived_col.replace('derived_', '').replace('_', ' ').title()

        # Get metrics
        derived_metrics = metrics['derived_metrics'].get(derived_col, {})
        failure_count = derived_metrics.get('failures', 0)
        pass_rate = derived_metrics.get('pass_rate', 0)

        # Expandable section
        with st.expander(f"{label} - {failure_count} failures ({pass_rate}% pass rate)"):
            if failure_count > 0:
                failures = df[df[derived_col] == 'FAIL']

                # Show failed materials
                st.write(f"Materials with {label}:")
                display_cols = ['material_number', 'org_level', derived_col]
                display_cols = [col for col in display_cols if col in df.columns]

                st.dataframe(failures[display_cols].head(100))

                if len(failures) > 100:
                    st.info(f"Showing first 100 of {len(failures)} failures")
            else:
                st.success("All materials pass!")
```

## Example: Persist to Database

```python
def persist_validation_results(
    df: pd.DataFrame,
    metrics: dict,
    suite_name: str,
    run_id: str = None
):
    """Persist validation results and metrics to Snowflake."""
    import uuid
    from datetime import datetime

    run_id = run_id or str(uuid.uuid4())
    run_timestamp = datetime.now()

    # Add metadata columns
    df['run_id'] = run_id
    df['run_timestamp'] = run_timestamp
    df['suite_name'] = suite_name

    # Write results to table
    conn = get_snowflake_connection()

    # Write raw results
    df.to_sql(
        'validation_results',
        conn,
        if_exists='append',
        index=False
    )

    # Write metrics to separate table
    metrics_df = pd.DataFrame([{
        'run_id': run_id,
        'run_timestamp': run_timestamp,
        'suite_name': suite_name,
        'total_rows': metrics['total_rows'],
        'total_materials': metrics['total_materials'],
        'overall_pass_rate': metrics['overall_pass_rate'],
    }])

    metrics_df.to_sql(
        'validation_metrics',
        conn,
        if_exists='append',
        index=False
    )

    print(f"✅ Persisted run {run_id}")
    return run_id
```

## Migration Strategy

### Phase 1: Update UI to use `run_validation_simple()`
- Replace complex result parsing with direct DataFrame usage
- Use `get_summary_table()` for overview
- Use pandas filtering for drill-downs

### Phase 2: Add Database Persistence
- Create `validation_results` table
- Create `validation_metrics` table
- Add persist function to save after each run

### Phase 3: Historical Queries
- Query historical runs from database
- Compare pass rates over time
- Track trends

### Key Advantages

1. **Simpler Code** - No complex parsing, just pandas
2. **Faster** - No restructuring overhead
3. **Flexible** - Easy to add new views and filters
4. **Database Ready** - DataFrame can go straight to DB
5. **BI Tool Friendly** - Standard columnar format

The DataFrame IS the API!
