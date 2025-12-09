"""
GX Runner - Compatibility wrapper for validation execution.

This module provides backward compatibility for code that imports from core.gx_runner.
The actual implementation is in validations/snowflake_runner.py.
"""

from validations.snowflake_runner import run_validation_from_yaml_snowflake


def run_validation_from_yaml(yaml_path, limit=None):
    """
    Run validation from YAML configuration.

    This is a compatibility wrapper that calls the actual Snowflake-native implementation.

    Args:
        yaml_path: Path to YAML validation configuration file
        limit: Optional row limit for testing

    Returns:
        Dictionary with 'results' and 'validated_materials' keys
    """
    return run_validation_from_yaml_snowflake(yaml_path, limit=limit)


__all__ = ["run_validation_from_yaml"]
