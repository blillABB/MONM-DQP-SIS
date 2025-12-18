"""
Expectation ID Metadata Lookup

This module provides functions to map between expectation IDs and their metadata
(expectation type, column, suite) by parsing YAML configuration files.

The YAML files act as the canonical source of truth for expectation metadata.
IDs are deterministic hashes, so we can regenerate them from YAML to perform lookups.
"""

import hashlib
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional


def lookup_expectation_metadata(
    exp_id: str,
    yaml_path: str | Path
) -> Optional[Dict[str, Any]]:
    """
    Look up expectation metadata from YAML by regenerating IDs.

    Args:
        exp_id: Expectation ID (e.g., 'exp_a3f4b2' or 'exp_a3f4b2_c7d8')
        yaml_path: Path to YAML validation configuration file

    Returns:
        Dictionary with keys:
        - expectation_id: The full expectation ID
        - expectation_type: Type of expectation
        - column: Column name (or compound key for multi-column)
        - suite_name: Name of the validation suite
        - base_id: Base expectation ID (without scope suffix)
        - is_scoped: Whether this is a scoped ID

        Returns None if no match found.

    Example:
        >>> metadata = lookup_expectation_metadata('exp_a3f4b2_c7d8', 'suite.yaml')
        >>> print(metadata)
        {
            'expectation_id': 'exp_a3f4b2_c7d8',
            'expectation_type': 'expect_column_values_to_not_be_null',
            'column': 'ORG_LEVEL',
            'suite_name': 'ABB SHOP DATA PRESENCE',
            'base_id': 'exp_a3f4b2',
            'is_scoped': True
        }
    """
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    suite_name = suite_config.get('metadata', {}).get('suite_name', '')
    validations = suite_config.get('validations', [])

    # Check if this is a scoped ID (has underscore suffix)
    is_scoped = '_' in exp_id.replace('exp_', '', 1)  # Ignore 'exp_' prefix

    # Build catalog of all expectation IDs
    for validation in validations:
        val_type = validation.get('type', '')

        # Generate base ID
        raw_base_id = f"{suite_name}|{val_type}"
        base_hash = hashlib.md5(raw_base_id.encode()).hexdigest()[:6]
        base_id = f"exp_{base_hash}"

        # Extract target columns
        targets = _extract_validation_targets(validation)

        if not is_scoped:
            # Looking for base ID match
            if base_id == exp_id:
                return {
                    'expectation_id': exp_id,
                    'expectation_type': val_type,
                    'column': '|'.join(targets) if targets else None,
                    'suite_name': suite_name,
                    'base_id': base_id,
                    'is_scoped': False
                }
        else:
            # Looking for scoped ID match - check each target
            validation_with_id = dict(validation)
            validation_with_id['expectation_id'] = base_id

            for target in targets:
                scoped_id = _build_scoped_id(validation_with_id, target)
                if scoped_id == exp_id:
                    return {
                        'expectation_id': exp_id,
                        'expectation_type': val_type,
                        'column': target,
                        'suite_name': suite_name,
                        'base_id': base_id,
                        'is_scoped': True
                    }

    return None


def build_expectation_catalog(yaml_path: str | Path) -> List[Dict[str, Any]]:
    """
    Build a complete catalog of all expectation IDs in a YAML suite.

    Args:
        yaml_path: Path to YAML validation configuration file

    Returns:
        List of dictionaries, each containing:
        - expectation_id: Full scoped expectation ID
        - expectation_type: Type of expectation
        - column: Column name
        - suite_name: Name of the validation suite
        - base_id: Base expectation ID (without scope)

    Example:
        >>> catalog = build_expectation_catalog('suite.yaml')
        >>> print(catalog[0])
        {
            'expectation_id': 'exp_a3f4b2_c7d8',
            'expectation_type': 'expect_column_values_to_not_be_null',
            'column': 'ORG_LEVEL',
            'suite_name': 'ABB SHOP DATA PRESENCE',
            'base_id': 'exp_a3f4b2'
        }
    """
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    suite_name = suite_config.get('metadata', {}).get('suite_name', '')
    validations = suite_config.get('validations', [])

    catalog = []

    for validation in validations:
        val_type = validation.get('type', '')

        # Generate base ID
        raw_base_id = f"{suite_name}|{val_type}"
        base_hash = hashlib.md5(raw_base_id.encode()).hexdigest()[:6]
        base_id = f"exp_{base_hash}"

        # Extract target columns
        targets = _extract_validation_targets(validation)

        validation_with_id = dict(validation)
        validation_with_id['expectation_id'] = base_id

        # Generate scoped IDs for each target
        for target in targets:
            scoped_id = _build_scoped_id(validation_with_id, target)

            catalog.append({
                'expectation_id': scoped_id,
                'expectation_type': val_type,
                'column': target,
                'suite_name': suite_name,
                'base_id': base_id
            })

    return catalog


def _extract_validation_targets(validation: Dict[str, Any]) -> List[str]:
    """Extract target columns from a validation definition."""
    val_type = validation.get('type', '')

    # Single column expectations
    if 'column' in validation:
        return [validation['column']]

    # Multiple columns expectations
    if 'columns' in validation:
        return validation['columns']

    # Column pair expectations
    if val_type.startswith('expect_column_pair'):
        col_a = validation.get('column_a')
        col_b = validation.get('column_b')
        if col_a and col_b:
            return [f"{col_a}|{col_b}"]

    # Rules-based expectations (value_in_set)
    if val_type == 'expect_column_values_to_be_in_set':
        rules = validation.get('rules', {})
        return list(rules.keys())

    # Conditional expectations
    if 'condition_column' in validation and 'required_column' in validation:
        return [f"{validation['condition_column']}|{validation['required_column']}"]

    if 'condition_column' in validation and 'target_column' in validation:
        return [f"{validation['condition_column']}|{validation['target_column']}"]

    # Column list (compound unique)
    if 'column_list' in validation:
        return ['|'.join(validation['column_list'])]

    return []


def _build_scoped_id(validation: Dict[str, Any], discriminator: str) -> str:
    """Build scoped expectation ID (matches sql_generator.py logic)."""
    base_id = validation.get('expectation_id', '')
    raw_scope = f"{base_id}|{discriminator}"
    scoped_hash = hashlib.md5(raw_scope.encode()).hexdigest()[:4]
    return f"{base_id}_{scoped_hash}"


__all__ = [
    'lookup_expectation_metadata',
    'build_expectation_catalog',
]
