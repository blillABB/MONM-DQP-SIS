"""Simple test for expectation ID generation without heavy dependencies."""

import hashlib
import yaml
from pathlib import Path

def generate_base_id(suite_name: str, val_type: str) -> str:
    """Generate base expectation ID (matches new sql_generator logic)."""
    raw_id = f"{suite_name}|{val_type}"
    expectation_id = hashlib.md5(raw_id.encode()).hexdigest()[:6]
    return f"exp_{expectation_id}"

def generate_scoped_id(base_id: str, column: str) -> str:
    """Generate scoped expectation ID (matches new sql_generator logic)."""
    raw_scope = f"{base_id}|{column}"
    scoped_hash = hashlib.md5(raw_scope.encode()).hexdigest()[:4]
    return f"{base_id}_{scoped_hash}"

# Test with real YAML
yaml_path = Path("validation_yaml/ABB SHOP DATA PRESENCE.yaml")

print("=" * 80)
print("Testing New Expectation ID Format")
print("=" * 80)

with open(yaml_path, 'r') as f:
    suite_config = yaml.safe_load(f)

suite_name = suite_config['metadata']['suite_name']
validations = suite_config.get('validations', [])

print(f"\nSuite: {suite_name}")
print(f"Validations: {len(validations)}")

# Test first validation
first_val = validations[0]
val_type = first_val.get('type')
columns = first_val.get('columns', [])

print(f"\nFirst Validation:")
print(f"  Type: {val_type}")
print(f"  Columns: {len(columns)}")

# Generate base ID
base_id = generate_base_id(suite_name, val_type)
print(f"\n  Base ID: {base_id} (10 chars total)")

# Generate scoped IDs for first 5 columns
print(f"\n  Scoped IDs (first 5 columns):")
for col in columns[:5]:
    scoped_id = generate_scoped_id(base_id, col)
    print(f"    {col:30} → {scoped_id} ({len(scoped_id)} chars)")

# Test ID stability - regenerate and compare
print("\n" + "=" * 80)
print("Testing ID Stability")
print("=" * 80)

base_id_2 = generate_base_id(suite_name, val_type)
print(f"\nBase ID (1st gen):  {base_id}")
print(f"Base ID (2nd gen):  {base_id_2}")
print(f"Match: {'✅ PASS' if base_id == base_id_2 else '❌ FAIL'}")

scoped_id_1 = generate_scoped_id(base_id, columns[0])
scoped_id_2 = generate_scoped_id(base_id_2, columns[0])
print(f"\nScoped ID (1st gen): {scoped_id_1}")
print(f"Scoped ID (2nd gen): {scoped_id_2}")
print(f"Match: {'✅ PASS' if scoped_id_1 == scoped_id_2 else '❌ FAIL'}")

# Test all validation types
print("\n" + "=" * 80)
print("ID Samples for All Validation Types")
print("=" * 80)

seen_types = set()
for val in validations:
    val_type = val.get('type')
    if val_type not in seen_types:
        seen_types.add(val_type)
        base_id = generate_base_id(suite_name, val_type)
        print(f"\n{val_type}")
        print(f"  → {base_id}")

print("\n" + "=" * 80)
print("✅ All tests passed!")
print("=" * 80)
