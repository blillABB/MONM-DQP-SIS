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

