"""Test columnar SQL generation."""

import yaml
from pathlib import Path
from validations.sql_generator import ValidationSQLGenerator

# Load YAML
yaml_path = Path("validation_yaml/ABB SHOP DATA PRESENCE.yaml")

with open(yaml_path, 'r') as f:
    suite_config = yaml.safe_load(f)

print("=" * 80)
print("Testing Columnar SQL Generation")
print("=" * 80)

# Generate SQL with columnar format
generator = ValidationSQLGenerator(suite_config, columnar_format=True)
sql = generator.generate_sql(limit=10)

print("\nGenerated SQL (columnar format):\n")
print(sql)

print("\n" + "=" * 80)
print("Testing Legacy SQL Generation (for comparison)")
print("=" * 80)

# Generate SQL with legacy format
generator_legacy = ValidationSQLGenerator(suite_config, columnar_format=False)
sql_legacy = generator_legacy.generate_sql(limit=10)

print("\nGenerated SQL (legacy format):\n")
print(sql_legacy[:1000] + "...\n(truncated)")

# Count columns
columnar_columns = sql.count(" AS exp_")
legacy_has_validation_results = "validation_results" in sql_legacy

print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print(f"\nColumnar format:")
print(f"  - Expectation columns: {columnar_columns}")
print(f"  - SQL length: {len(sql)} chars")

print(f"\nLegacy format:")
print(f"  - Has validation_results array: {legacy_has_validation_results}")
print(f"  - SQL length: {len(sql_legacy)} chars")

print("\n✅ Both formats generated successfully!")
