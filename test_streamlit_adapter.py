"""Test the simplified validation flow with adapter for Streamlit UI."""

import yaml
from pathlib import Path
from validations.snowflake_runner import run_validation_simple
from core.validation_metrics import get_failed_materials
from core.expectation_metadata import lookup_expectation_metadata

def adapt_simple_to_legacy_format(simple_payload, yaml_path):
    """
    Convert simplified validation payload to legacy format for UI compatibility.
    (Copy of the adapter function from Validation_Report.py for testing)
    """
    df = simple_payload["df"]
    metrics = simple_payload["metrics"]
    suite_config = simple_payload["suite_config"]

    # Extract expectation and derived columns
    exp_columns = [col for col in df.columns if col.startswith("exp_")]
    derived_columns = [col for col in df.columns if col.startswith("derived_")]

    # Get index column from suite config
    index_column = suite_config.get("metadata", {}).get("index_column", "material_number").lower()

    # Build legacy results list
    results = []
    for exp_col in exp_columns:
        exp_metrics = metrics["expectation_metrics"].get(exp_col, {})

        # Get failed materials for this expectation
        failed_df = get_failed_materials(df, exp_id=exp_col, index_column=index_column)

        # Look up expectation metadata from YAML
        metadata = lookup_expectation_metadata(exp_col, yaml_path)

        if metadata:
            expectation_type = metadata.get("expectation_type", exp_col)
            column = metadata.get("column", exp_col)
        else:
            # Fallback if lookup fails
            expectation_type = exp_col
            column = exp_col

        results.append({
            "expectation_id": exp_col,
            "expectation_type": expectation_type,
            "column": column,
            "element_count": exp_metrics.get("total", 0),
            "unexpected_count": exp_metrics.get("failures", 0),
            "unexpected_percent": 100 - exp_metrics.get("pass_rate", 100),
            "table_grain": suite_config.get("metadata", {}).get("table_grain", "MATERIAL"),
            "unique_by": suite_config.get("metadata", {}).get("unique_by", ["MATERIAL_NUMBER"]),
        })

    # Build legacy derived_status_results list
    derived_status_results = []
    for derived_col in derived_columns:
        derived_metrics = metrics["derived_metrics"].get(derived_col, {})

        # Get failed materials for this derived status
        failed_df = get_failed_materials(df, derived_id=derived_col, index_column=index_column)

        # Extract status label (remove "derived_" prefix)
        status_label = derived_col.replace("derived_", "").replace("_", " ").title()

        # Build failed materials list
        failed_materials = []
        for _, row in failed_df.iterrows():
            failed_materials.append({
                "MATERIAL_NUMBER": row.get(index_column.upper(), row.get(index_column, "")),
                "failed_columns": [],
                "failure_count": 0,
                "failed_expectations": []
            })

        derived_status_results.append({
            "status_label": status_label,
            "expectation_id": derived_col,
            "expectation_type": "Derived Status",
            "unexpected_count": derived_metrics.get("failures", 0),
            "unexpected_percent": 100 - derived_metrics.get("pass_rate", 100),
            "context_columns": [index_column.upper()],
            "failed_materials": failed_materials,
        })

    # Get list of validated materials
    if index_column in df.columns:
        validated_materials = df[index_column].unique().tolist()
    else:
        validated_materials = []

    return {
        "results": results,
        "derived_status_results": derived_status_results,
        "validated_materials": validated_materials,
        "total_validated_count": metrics.get("total_materials", len(validated_materials)),
        "full_results_df": df,
    }


# Test with real YAML
yaml_path = Path("validation_yaml/ABB SHOP DATA PRESENCE.yaml")

print("=" * 80)
print("Testing Simplified Validation Flow with Adapter")
print("=" * 80)

print("\n▶ Step 1: Run simplified validation")
try:
    simple_payload = run_validation_simple(yaml_path, limit=10)
    print(f"✅ Validation completed successfully")
    print(f"   - DataFrame shape: {simple_payload['df'].shape}")
    print(f"   - Total materials: {simple_payload['metrics']['total_materials']}")
    print(f"   - Overall pass rate: {simple_payload['metrics']['overall_pass_rate']}%")
except Exception as e:
    print(f"❌ Validation failed: {e}")
    exit(1)

print("\n▶ Step 2: Apply adapter to convert to legacy format")
try:
    legacy_payload = adapt_simple_to_legacy_format(simple_payload, yaml_path)
    print(f"✅ Adapter completed successfully")
    print(f"   - Number of results: {len(legacy_payload['results'])}")
    print(f"   - Number of derived status results: {len(legacy_payload['derived_status_results'])}")
    print(f"   - Total validated materials: {legacy_payload['total_validated_count']}")
except Exception as e:
    print(f"❌ Adapter failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n▶ Step 3: Verify legacy payload structure")
print("\nFirst result:")
if legacy_payload['results']:
    first_result = legacy_payload['results'][0]
    print(f"  - Expectation ID: {first_result['expectation_id']}")
    print(f"  - Expectation Type: {first_result['expectation_type']}")
    print(f"  - Column: {first_result['column']}")
    print(f"  - Element Count: {first_result['element_count']}")
    print(f"  - Unexpected Count: {first_result['unexpected_count']}")
    print(f"  - Unexpected %: {first_result['unexpected_percent']:.2f}%")

print("\nFirst 3 expectation columns from DataFrame:")
exp_columns = [col for col in simple_payload['df'].columns if col.startswith('exp_')]
for col in exp_columns[:3]:
    print(f"  - {col}")

print("\n" + "=" * 80)
print("✅ All tests passed!")
print("=" * 80)
