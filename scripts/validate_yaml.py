#!/usr/bin/env python3
"""
YAML Validation Suite Validator
===============================

Validates YAML validation suite files WITHOUT connecting to Snowflake.
Use this to check your YAML structure before running expensive queries.

Usage:
    python scripts/validate_yaml.py path/to/suite.yaml
    python scripts/validate_yaml.py validation_yaml/Aurora_Motors_Validation.yaml
    python scripts/validate_yaml.py validation_yaml/*.yaml  # Validate all

Exit codes:
    0 - All validations passed
    1 - Validation errors found
    2 - File not found or read error
"""

import sys
import yaml
from pathlib import Path

# Supported expectation types (must match BaseValidationSuite.SUPPORTED_EXPECTATION_TYPES)
SUPPORTED_EXPECTATION_TYPES = [
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_pair_values_to_be_equal",
    "expect_column_value_lengths_to_equal",
    "expect_column_value_lengths_to_be_between",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_unique",
    "expect_compound_columns_to_be_unique",
]

# Required fields for each expectation type
EXPECTATION_REQUIREMENTS = {
    "expect_column_values_to_not_be_null": {"required": ["columns"], "optional": []},
    "expect_column_values_to_be_in_set": {"required": ["rules"], "optional": []},
    "expect_column_values_to_not_be_in_set": {"required": ["column", "value_set"], "optional": []},
    "expect_column_values_to_match_regex": {"required": ["columns", "regex"], "optional": []},
    "expect_column_values_to_not_match_regex": {"required": ["columns", "regex"], "optional": []},
    "expect_column_pair_values_a_to_be_greater_than_b": {"required": ["column_a", "column_b"], "optional": ["or_equal"]},
    "expect_column_pair_values_to_be_equal": {"required": ["column_a", "column_b"], "optional": []},
    "expect_column_value_lengths_to_equal": {"required": ["columns", "value"], "optional": []},
    "expect_column_value_lengths_to_be_between": {"required": ["columns", "min_value", "max_value"], "optional": []},
    "expect_column_values_to_be_between": {"required": ["columns", "min_value", "max_value"], "optional": []},
    "expect_column_values_to_be_unique": {"required": ["columns"], "optional": []},
    "expect_compound_columns_to_be_unique": {"required": ["column_list"], "optional": []},
}


def validate_yaml_file(yaml_path: Path) -> tuple[bool, list[str]]:
    """
    Validate a YAML validation suite file.

    Returns:
        tuple: (is_valid: bool, errors: list[str])
    """
    errors = []

    # Check file exists
    if not yaml_path.exists():
        return False, [f"File not found: {yaml_path}"]

    # Try to load YAML
    try:
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return False, [f"YAML syntax error: {e}"]
    except Exception as e:
        return False, [f"Error reading file: {e}"]

    # Check config is a dict
    if not isinstance(config, dict):
        return False, [f"YAML must be a mapping (dict), got {type(config).__name__}"]

    # Validate metadata section
    metadata = config.get("metadata")
    if metadata is None:
        errors.append("Missing required 'metadata' section")
    elif not isinstance(metadata, dict):
        errors.append(f"'metadata' must be a mapping, got {type(metadata).__name__}")
    else:
        # Check required metadata fields
        if not metadata.get("suite_name"):
            errors.append("metadata.suite_name is required")
        if not metadata.get("data_source"):
            errors.append("metadata.data_source is required")

        # Warn about optional but recommended fields
        if not metadata.get("index_column"):
            errors.append("Warning: metadata.index_column not specified, will default to 'MATERIAL_NUMBER'")

    # Validate validations section
    validations = config.get("validations")
    if validations is None:
        errors.append("Missing required 'validations' section")
    elif not isinstance(validations, list):
        errors.append(f"'validations' must be a list, got {type(validations).__name__}")
    elif len(validations) == 0:
        errors.append("'validations' list is empty - no rules to execute")
    else:
        # Validate each validation rule
        for i, validation in enumerate(validations):
            rule_errors = validate_rule(validation, i)
            errors.extend(rule_errors)

    is_valid = len([e for e in errors if not e.startswith("Warning:")]) == 0
    return is_valid, errors


def validate_rule(validation: dict, index: int) -> list[str]:
    """Validate a single validation rule."""
    errors = []
    prefix = f"validations[{index}]"

    if not isinstance(validation, dict):
        return [f"{prefix}: must be a mapping, got {type(validation).__name__}"]

    # Check type field
    val_type = validation.get("type")
    if not val_type:
        return [f"{prefix}: missing required 'type' field"]

    if val_type not in SUPPORTED_EXPECTATION_TYPES:
        return [f"{prefix}: unknown type '{val_type}'. Valid types:\n    " +
                "\n    ".join(SUPPORTED_EXPECTATION_TYPES)]

    # Check required fields for this expectation type
    requirements = EXPECTATION_REQUIREMENTS.get(val_type, {})
    required_fields = requirements.get("required", [])

    for field in required_fields:
        if field not in validation or validation[field] is None:
            errors.append(f"{prefix}: '{val_type}' requires '{field}' field")
        elif field == "columns" and not isinstance(validation[field], list):
            errors.append(f"{prefix}: 'columns' must be a list")
        elif field == "column_list" and not isinstance(validation[field], list):
            errors.append(f"{prefix}: 'column_list' must be a list")
        elif field == "rules" and not isinstance(validation[field], dict):
            errors.append(f"{prefix}: 'rules' must be a mapping")
        elif field == "value_set" and not isinstance(validation[field], list):
            errors.append(f"{prefix}: 'value_set' must be a list")

    # Type-specific validations
    if val_type == "expect_column_values_to_be_in_set":
        rules = validation.get("rules", {})
        if isinstance(rules, dict):
            for col, values in rules.items():
                if not isinstance(values, list):
                    errors.append(f"{prefix}: rules['{col}'] must be a list of allowed values")

    if val_type in ["expect_column_value_lengths_to_equal"]:
        value = validation.get("value")
        if value is not None and not isinstance(value, int):
            errors.append(f"{prefix}: 'value' must be an integer")

    if val_type in ["expect_column_value_lengths_to_be_between", "expect_column_values_to_be_between"]:
        min_val = validation.get("min_value")
        max_val = validation.get("max_value")
        if min_val is not None and max_val is not None:
            if not isinstance(min_val, (int, float)) or not isinstance(max_val, (int, float)):
                errors.append(f"{prefix}: 'min_value' and 'max_value' must be numbers")
            elif min_val > max_val:
                errors.append(f"{prefix}: 'min_value' ({min_val}) cannot be greater than 'max_value' ({max_val})")

    return errors


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    # Collect all files to validate
    files_to_validate = []
    for arg in sys.argv[1:]:
        path = Path(arg)
        if path.exists():
            files_to_validate.append(path)
        else:
            # Try glob pattern
            import glob
            matches = glob.glob(arg)
            if matches:
                files_to_validate.extend(Path(m) for m in matches)
            else:
                print(f"File not found: {arg}")
                sys.exit(2)

    if not files_to_validate:
        print("No files to validate")
        sys.exit(2)

    # Validate each file
    all_valid = True
    total_rules = 0

    for yaml_path in files_to_validate:
        print(f"\n{'='*60}")
        print(f"Validating: {yaml_path}")
        print('='*60)

        is_valid, errors = validate_yaml_file(yaml_path)

        if is_valid:
            # Count rules for summary
            try:
                with open(yaml_path, 'r') as f:
                    config = yaml.safe_load(f)
                rule_count = len(config.get("validations", []))
                total_rules += rule_count
                print(f"PASSED - {rule_count} validation rules found")
            except Exception:
                print("PASSED")
        else:
            all_valid = False
            print("FAILED")

        # Print errors/warnings
        for error in errors:
            if error.startswith("Warning:"):
                print(f"  [WARN] {error}")
            else:
                print(f"  [ERROR] {error}")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    print(f"Files validated: {len(files_to_validate)}")
    print(f"Total rules: {total_rules}")
    print(f"Status: {'ALL PASSED' if all_valid else 'ERRORS FOUND'}")

    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
