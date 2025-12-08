"""
Validations package - Snowflake-native validation framework.

This package contains the unified validation system that generates SQL
dynamically from YAML configuration and executes validations entirely in Snowflake.
"""

from validations.snowflake_runner import run_validation_from_yaml_snowflake
from validations.sql_generator import ValidationSQLGenerator

__all__ = [
    "run_validation_from_yaml_snowflake",
    "ValidationSQLGenerator",
]
