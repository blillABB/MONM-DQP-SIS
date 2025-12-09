"""Central configuration for Snowflake and other shared settings."""

import os
import streamlit as st


def safe_secret(key, default=""):
    """Fetch a setting from Streamlit secrets, falling back to the environment."""
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)


# -----------------------------------------------------------------------------
# Snowflake connection template (single external-browser flow)
# Fill in the placeholders below with your account-specific values.
# -----------------------------------------------------------------------------
SNOWFLAKE_CONFIG = {
    "account": "ABB-ABB_MO",
    "user": "BLAKE.LILLARD@US.ABB.COM",
    "authenticator": "externalbrowser",
    "role": "R_IS_MO_MONM",
    "warehouse": "WH_BU_READ",
    "database": "PROD_MO_MONM",
    "schema": "REPORTING",
    "client_store_temporary_credential": False,
}


# Data Lark API configuration - actual values should be in .streamlit/secrets.toml
DATALARK_URL = safe_secret("DATALARK_URL", "")
DATALARK_TOKEN = safe_secret("DATALARK_TOKEN", "")


def ensure_snowflake_config():
    """
    Validate that Snowflake configuration is properly set up.

    This function checks that SNOWFLAKE_CONFIG exists and has required fields.
    Raises an error if configuration is invalid.
    """
    if not SNOWFLAKE_CONFIG:
        raise RuntimeError("SNOWFLAKE_CONFIG is not defined")

    required_fields = ["account", "user", "warehouse", "database", "schema"]
    missing = [field for field in required_fields if field not in SNOWFLAKE_CONFIG]

    if missing:
        raise RuntimeError(f"SNOWFLAKE_CONFIG is missing required fields: {missing}")

    # All checks passed
    return True


def snowflake_config_summary():
    """Return a sanitized summary of the active Snowflake configuration."""
    return {
        "account": SNOWFLAKE_CONFIG.get("account"),
        "user": SNOWFLAKE_CONFIG.get("user"),
        "role": SNOWFLAKE_CONFIG.get("role"),
        "warehouse": SNOWFLAKE_CONFIG.get("warehouse"),
        "database": SNOWFLAKE_CONFIG.get("database"),
        "schema": SNOWFLAKE_CONFIG.get("schema"),
        "authenticator": SNOWFLAKE_CONFIG.get("authenticator"),
    }
