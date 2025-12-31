"""Test script for new expectation ID generation and lookup."""

import yaml
from pathlib import Path
from validations.sql_generator import _annotate_expectation_ids, build_scoped_expectation_id
from core.expectation_metadata import lookup_expectation_metadata, build_expectation_catalog

# Test with a real YAML file
yaml_path = Path("validation_yaml/ABB SHOP DATA PRESENCE.yaml")

print("=" * 80)
print("Testing Expectation ID Generation and Lookup")
print("=" * 80)

# Load YAML
with open(yaml_path, 'r') as f:
    suite_config = yaml.safe_load(f)

suite_name = suite_config['metadata']['suite_name']
validations = suite_config.get('validations', [])

print(f"\nSuite: {suite_name}")
print(f"Validations: {len(validations)}")

# Annotate with new IDs
annotated = _annotate_expectation_ids(validations, suite_name)

print("\n" + "=" * 80)
print("Generated Base IDs (showing first 3 validations)")
print("=" * 80)

for i, validation in enumerate(annotated[:3]):
    val_type = validation.get('type')
    exp_id = validation.get('expectation_id')
    columns = validation.get('columns', [validation.get('column')])
    print(f"\n{i+1}. Type: {val_type}")
    print(f"   Base ID: {exp_id}")
    print(f"   Columns: {len(columns) if isinstance(columns, list) else 1}")

# Test scoped IDs
print("\n" + "=" * 80)
print("Generated Scoped IDs (first validation, first 5 columns)")
print("=" * 80)

first_validation = annotated[0]
columns = first_validation.get('columns', [])[:5]

for col in columns:
    scoped_id = build_scoped_expectation_id(first_validation, col)
    print(f"  {col:30} → {scoped_id}")

# Test lookup function
print("\n" + "=" * 80)
print("Testing Lookup Function")
print("=" * 80)

test_scoped_id = build_scoped_expectation_id(first_validation, columns[0])
print(f"\nLooking up ID: {test_scoped_id}")

metadata = lookup_expectation_metadata(test_scoped_id, yaml_path)
if metadata:
    print("✅ Found metadata:")
    for key, value in metadata.items():
        print(f"  {key}: {value}")
else:
    print("❌ Metadata not found!")

# Test base ID lookup
base_id = first_validation.get('expectation_id')
print(f"\nLooking up base ID: {base_id}")

metadata = lookup_expectation_metadata(base_id, yaml_path)
if metadata:
    print("✅ Found metadata:")
    for key, value in metadata.items():
        if key == 'column' and isinstance(value, str) and len(value) > 50:
            print(f"  {key}: {value[:50]}... ({len(value.split('|'))} columns)")
        else:
            print(f"  {key}: {value}")
else:
    print("❌ Metadata not found!")

# Build full catalog
print("\n" + "=" * 80)
print("Building Complete Catalog")
print("=" * 80)

catalog = build_expectation_catalog(yaml_path)
print(f"\nTotal catalog entries: {len(catalog)}")
print(f"\nFirst 5 catalog entries:")

for entry in catalog[:5]:
    print(f"\n  ID: {entry['expectation_id']}")
    print(f"  Type: {entry['expectation_type']}")
    print(f"  Column: {entry['column']}")

# Test ID stability
print("\n" + "=" * 80)
print("Testing ID Stability (re-generate should match)")
print("=" * 80)

annotated_again = _annotate_expectation_ids(validations, suite_name)
for i, (orig, new) in enumerate(zip(annotated[:3], annotated_again[:3])):
    orig_id = orig.get('expectation_id')
    new_id = new.get('expectation_id')
    match = "✅" if orig_id == new_id else "❌"
    print(f"{match} Validation {i+1}: {orig_id} == {new_id}")

print("\n" + "=" * 80)
print("Tests Complete!")
print("=" * 80)
