"""
Test script for the unified Snowflake-native validation runner.

Tests the complete flow:
1. Load YAML configuration
2. Generate SQL dynamically
3. Execute in Snowflake
4. Parse results into GX-compatible format
5. Verify output structure and grain-based context
"""

import sys
from pathlib import Path

# Add project root to Python path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

import json
import importlib.util

# Import snowflake_runner directly to avoid GX dependency in __init__.py
spec = importlib.util.spec_from_file_location(
    "snowflake_runner",
    project_root / "validations" / "snowflake_runner.py"
)
snowflake_runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snowflake_runner)
run_validation_from_yaml_snowflake = snowflake_runner.run_validation_from_yaml_snowflake

from core.grain_mapping import get_grain_for_column


def test_unified_runner(
    yaml_path: str = "validation_yaml/abb_shop_abp_data_presence.yaml",
    limit: int = 1000
):
    """
    Test the unified validation runner end-to-end.

    Args:
        yaml_path: Path to YAML validation configuration
        limit: Row limit for testing (smaller = faster)
    """
    print("=" * 80)
    print("Testing Unified Snowflake-Native Validation Runner")
    print("=" * 80)
    print(f"\nYAML Config: {yaml_path}")
    print(f"Row Limit: {limit:,}")
    print()

    # Run validation
    print("▶ Running validation...")
    try:
        results = run_validation_from_yaml_snowflake(yaml_path, limit=limit)
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print()
    print("=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)

    # Check top-level structure
    print("\n1. Checking top-level structure...")
    required_keys = ["results", "validated_materials"]
    for key in required_keys:
        if key not in results:
            print(f"  ❌ Missing required key: {key}")
            return False
    print("  ✅ Top-level structure valid")

    # Check results array
    print("\n2. Checking results array...")
    if not isinstance(results["results"], list):
        print("  ❌ 'results' is not a list")
        return False

    num_results = len(results["results"])
    print(f"  ✅ Found {num_results} validation results")

    if num_results == 0:
        print("  ⚠️  No validation results (this may be expected if all passed)")
        return True

    # Check individual result structure
    print("\n3. Checking individual result structure...")
    required_result_fields = [
        "expectation_type",
        "column",
        "success",
        "element_count",
        "unexpected_count",
        "unexpected_percent",
        "failed_materials",
        "table_grain",
        "unique_by"
    ]

    sample_result = results["results"][0]
    print(f"  Sample result (first validation):")
    print(f"    Type: {sample_result.get('expectation_type')}")
    print(f"    Column: {sample_result.get('column')}")
    print(f"    Success: {sample_result.get('success')}")

    for field in required_result_fields:
        if field not in sample_result:
            print(f"  ❌ Missing required field in result: {field}")
            return False

    print("  ✅ All required fields present")

    # Check grain-based context
    print("\n4. Verifying grain-based context...")
    grain_check_passed = True

    for result in results["results"]:
        column = result.get("column")
        if not column or "|" in column:
            # Skip column pairs
            continue

        expected_grain, expected_unique_by = get_grain_for_column(column)
        actual_grain = result.get("table_grain")
        actual_unique_by = result.get("unique_by")

        if expected_grain != actual_grain:
            print(f"  ❌ Grain mismatch for {column}: expected {expected_grain}, got {actual_grain}")
            grain_check_passed = False

        if set(expected_unique_by) != set(actual_unique_by):
            print(f"  ❌ Unique_by mismatch for {column}:")
            print(f"      Expected: {expected_unique_by}")
            print(f"      Got: {actual_unique_by}")
            grain_check_passed = False

    if grain_check_passed:
        print("  ✅ Grain-based context is correct for all columns")

    # Check failed_materials structure
    print("\n5. Checking failed_materials structure...")
    has_failures = False
    for result in results["results"]:
        if result.get("unexpected_count", 0) > 0:
            has_failures = True
            failed_materials = result.get("failed_materials", [])

            if not isinstance(failed_materials, list):
                print(f"  ❌ failed_materials is not a list for {result['column']}")
                return False

            if len(failed_materials) > 0:
                sample_failure = failed_materials[0]
                column = result.get("column")

                # Get expected context columns for this column's grain
                _, expected_context = get_grain_for_column(column)

                # Check that failure has all grain-specific context columns
                missing_context = [col for col in expected_context if col not in sample_failure]
                if missing_context:
                    print(f"  ❌ Failure for {column} missing context columns: {missing_context}")
                    print(f"      Sample failure: {sample_failure}")
                    return False

                print(f"  ✅ Sample failure for {column}:")
                print(f"      {json.dumps(sample_failure, indent=6)}")
                break

    if not has_failures:
        print("  ℹ️  No failures found (all validations passed)")

    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total_validations = len(results["results"])
    passed_validations = sum(1 for r in results["results"] if r.get("success"))
    failed_validations = total_validations - passed_validations
    total_rows = results["results"][0].get("element_count", 0) if results["results"] else 0

    print(f"\nTotal Validations: {total_validations}")
    print(f"  ✅ Passed: {passed_validations}")
    print(f"  ❌ Failed: {failed_validations}")
    print(f"\nRows Validated: {total_rows:,}")

    # Show validation types
    validation_types = {}
    for result in results["results"]:
        vtype = result.get("expectation_type", "unknown")
        validation_types[vtype] = validation_types.get(vtype, 0) + 1

    print("\nValidation Types:")
    for vtype, count in sorted(validation_types.items()):
        print(f"  {vtype}: {count}")

    # Check for grain-based context optimization
    print("\n" + "=" * 80)
    print("GRAIN-BASED CONTEXT OPTIMIZATION")
    print("=" * 80)

    grain_distribution = {}
    for result in results["results"]:
        grain = result.get("table_grain", "unknown")
        grain_distribution[grain] = grain_distribution.get(grain, 0) + 1

    print("\nValidations by grain:")
    for grain, count in sorted(grain_distribution.items()):
        _, unique_by = get_grain_for_column(
            next(r["column"] for r in results["results"] if r.get("table_grain") == grain)
        )
        print(f"  {grain}: {count} validations")
        print(f"    Context columns: {', '.join(unique_by)}")

    print("\n✅ All tests passed!")
    print("=" * 80)

    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test the unified validation runner")
    parser.add_argument(
        '--yaml',
        type=str,
        default='validation_yaml/abb_shop_abp_data_presence.yaml',
        help='Path to validation YAML file'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Row limit for testing'
    )

    args = parser.parse_args()

    success = test_unified_runner(args.yaml, args.limit)
    sys.exit(0 if success else 1)
