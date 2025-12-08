"""
Test SQL generation for the unified Snowflake-native validation framework.

This test validates SQL generation without requiring Snowflake connection.
Run the full end-to-end test (test_unified_runner.py) in Docker environment.
"""

import sys
from pathlib import Path

# Add project root to Python path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

import yaml
import importlib.util

# Import sql_generator directly to avoid GX dependency
spec = importlib.util.spec_from_file_location(
    "sql_generator",
    project_root / "validations" / "sql_generator.py"
)
sql_generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sql_generator)
ValidationSQLGenerator = sql_generator.ValidationSQLGenerator

from core.grain_mapping import get_grain_for_column, get_context_columns_for_columns


def test_sql_generation(yaml_path: str = "validation_yaml/abb_shop_abp_data_presence.yaml"):
    """
    Test SQL generation from YAML configuration.

    Args:
        yaml_path: Path to YAML validation configuration
    """
    print("=" * 80)
    print("Testing SQL Generation for Unified Framework")
    print("=" * 80)
    print(f"\nYAML Config: {yaml_path}")
    print()

    # Load YAML configuration
    print("▶ Loading YAML configuration...")
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    suite_name = suite_config.get("metadata", {}).get("suite_name", "Unknown")
    validations = suite_config.get("validations", [])
    print(f"  Suite: {suite_name}")
    print(f"  Validations: {len(validations)}")

    # Create generator
    print("\n▶ Creating SQL generator...")
    generator = ValidationSQLGenerator(suite_config)

    # Generate SQL
    print("\n▶ Generating SQL...")
    try:
        sql = generator.generate_sql(limit=1000)
    except Exception as e:
        print(f"❌ SQL generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("  ✅ SQL generated successfully")

    # Analyze SQL
    print("\n" + "=" * 80)
    print("SQL ANALYSIS")
    print("=" * 80)

    lines = sql.strip().split('\n')
    print(f"\nSQL Statistics:")
    print(f"  Lines: {len(lines)}")
    print(f"  Characters: {len(sql):,}")

    # Count components
    validation_comments = sql.count('-- Validation for')
    print(f"  Validation blocks: {validation_comments}")

    # Check for key SQL patterns
    print(f"\nSQL Pattern Checks:")
    patterns = {
        "WITH base_data AS": "✅" if "WITH base_data AS" in sql else "❌",
        "COUNT(*) as": "✅" if "COUNT(*) as" in sql else "❌",
        "ARRAY_COMPACT": "✅" if "ARRAY_COMPACT" in sql else "❌",
        "OBJECT_CONSTRUCT": "✅" if "OBJECT_CONSTRUCT" in sql else "❌",
        "LIMIT 1000": "✅" if "LIMIT 1000" in sql else "❌",
    }

    for pattern, status in patterns.items():
        print(f"  {status} {pattern}")

    # Analyze validated columns
    print("\n" + "=" * 80)
    print("VALIDATED COLUMNS ANALYSIS")
    print("=" * 80)

    validated_columns = generator._collect_validated_columns()
    print(f"\nTotal columns validated: {len(validated_columns)}")

    # Group by grain
    grain_groups = {}
    for col in validated_columns:
        grain, _ = get_grain_for_column(col)
        if grain not in grain_groups:
            grain_groups[grain] = []
        grain_groups[grain].append(col)

    print(f"\nColumns by grain:")
    for grain, cols in sorted(grain_groups.items()):
        _, unique_by = get_grain_for_column(cols[0])
        print(f"  {grain}: {len(cols)} columns")
        print(f"    Context: {', '.join(unique_by)}")
        print(f"    Sample columns: {', '.join(cols[:3])}")
        if len(cols) > 3:
            print(f"    ... and {len(cols) - 3} more")

    # Check context columns
    print("\n" + "=" * 80)
    print("CONTEXT COLUMNS OPTIMIZATION")
    print("=" * 80)

    context_cols = get_context_columns_for_columns(validated_columns)
    print(f"\nContext columns needed for this suite: {len(context_cols)}")
    print(f"  {', '.join(context_cols)}")

    # Calculate optimization benefit
    from core.constants import VALIDATION_CONTEXT_COLUMNS
    old_context_count = len(VALIDATION_CONTEXT_COLUMNS)
    new_context_count = len(context_cols)

    if old_context_count > 0:
        reduction_pct = ((old_context_count - new_context_count) / old_context_count) * 100
        print(f"\nOld approach (all context columns): {old_context_count}")
        print(f"New approach (grain-based): {new_context_count}")
        if reduction_pct > 0:
            print(f"Reduction: {reduction_pct:.1f}%")

    # Show validation types
    print("\n" + "=" * 80)
    print("VALIDATION TYPES")
    print("=" * 80)

    validation_types = {}
    for validation in validations:
        vtype = validation.get("type", "unknown")
        validation_types[vtype] = validation_types.get(vtype, 0) + 1

    print(f"\nValidation types in suite:")
    for vtype, count in sorted(validation_types.items()):
        print(f"  {vtype}: {count}")

    # Show sample SQL (first 50 lines)
    print("\n" + "=" * 80)
    print("SAMPLE SQL (first 50 lines)")
    print("=" * 80)
    print()
    for i, line in enumerate(lines[:50], 1):
        print(f"{i:3d} | {line}")

    if len(lines) > 50:
        print(f"\n... and {len(lines) - 50} more lines")

    print("\n" + "=" * 80)
    print("✅ SQL Generation Test Passed!")
    print("=" * 80)
    print("\nNext step: Run full end-to-end test in Docker environment:")
    print("  docker-compose exec app python scripts/test_unified_runner.py --limit 1000")
    print()

    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test SQL generation")
    parser.add_argument(
        '--yaml',
        type=str,
        default='validation_yaml/abb_shop_abp_data_presence.yaml',
        help='Path to validation YAML file'
    )

    args = parser.parse_args()

    success = test_sql_generation(args.yaml)
    sys.exit(0 if success else 1)
