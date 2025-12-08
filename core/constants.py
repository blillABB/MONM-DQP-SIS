"""
Application constants for the validation framework.

This module contains static values that don't change based on environment
or configuration. For environment-specific settings, see config.py.
"""

# Context columns used for traceability in validation results
# Using actual Snowflake DB column names (not display names)
VALIDATION_CONTEXT_COLUMNS = [
    "SALES_ORGANIZATION",
    "PLANT",
    "DISTRIBUTION_CHANNEL",
    "WAREHOUSE_NUMBER",
    "STORAGE_TYPE",
    "STORAGE_LOCATION",
]
