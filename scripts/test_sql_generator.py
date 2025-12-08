"""
Test script for the SQL Generator

Demonstrates how to use the ValidationSQLGenerator to generate
SQL from YAML configuration.
"""

import sys
from pathlib import Path

# Add project root to Python path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

import yaml
from validations.sql_generator import ValidationSQLGenerator


def test_sql_generator(yaml_path: str = "validation_yaml/abb_shop_abp_data_presence.yaml"):
    """
    Load a YAML validation suite and generate SQL.

    Args:
        yaml_path: Path to YAML configuration file
    """
    print(f"Loading configuration from: {yaml_path}")
    print("=" * 80)

    # Load YAML configuration
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    print(f"\nSuite: {suite_config['metadata']['suite_name']}")
    print(f"Description: {suite_config['metadata']['description']}")
    print(f"Validations: {len(suite_config['validations'])} rules")
    print()

    # Create generator
    generator = ValidationSQLGenerator(suite_config)

    # Generate SQL
    print("Generating SQL...")
    print("=" * 80)
    sql = generator.generate_sql(limit=10000)

    print("\nGenerated SQL:")
    print("-" * 80)
    print(sql)
    print("-" * 80)

    # Show stats
    lines = sql.strip().split('\n')
    print(f"\nSQL Stats:")
    print(f"  Lines: {len(lines)}")
    print(f"  Characters: {len(sql)}")

    # Count validations
    validation_count = sql.count('-- Validation for')
    print(f"  Validations: {validation_count}")

    # Show what would be selected
    validated_columns = generator._collect_validated_columns()
    print(f"\n  Columns validated: {len(validated_columns)}")
    print(f"  {', '.join(validated_columns[:5])}...")

    # Show context columns
    from core.grain_mapping import get_context_columns_for_columns
    context = get_context_columns_for_columns(validated_columns)
    print(f"\n  Context columns needed: {len(context)}")
    print(f"  {', '.join(context)}")

    print("\n✅ SQL generation successful!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test the SQL Generator")
    parser.add_argument(
        '--yaml',
        type=str,
        default='validation_yaml/abb_shop_abp_data_presence.yaml',
        help='Path to validation YAML file'
    )

    args = parser.parse_args()
    test_sql_generator(args.yaml)
