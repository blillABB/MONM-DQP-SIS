import os
import streamlit as st


def safe_secret(key, default=""):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)


# Data Lark API configuration - actual values should be in .streamlit/secrets.toml
DATALARK_URL = safe_secret("DATALARK_URL", "")
DATALARK_TOKEN = safe_secret("DATALARK_TOKEN", "")

_account = os.getenv("SNOWFLAKE_ACCOUNT", "ABB-ABB_MO")
_database = os.getenv("SNOWFLAKE_DATABASE", "PROD_MO_MONM")
_schema = os.getenv("SNOWFLAKE_SCHEMA", "REPORTING")

# Single, external-browser Snowflake connection (no key-pair/Docker flow)
_local_user = safe_secret("SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER"))
_local_role = os.getenv("SNOWFLAKE_ROLE", "R_IS_MO_MONM")

# Disable cached SSO tokens so we always prompt for the active user. This avoids the
# "user differs from the IDP" error when a prior cached login was created under a
# different account. If you prefer reusing cached tokens, set SNOWFLAKE_USE_TOKEN_CACHE=true.
_use_token_cache = os.getenv("SNOWFLAKE_USE_TOKEN_CACHE", "false").lower() == "true"

# Allow warehouse override via environment variable
_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "WH_BU_READ")

SNOWFLAKE_CONFIG = {
    "account": _account,
    "user": _local_user,
    "authenticator": "externalbrowser",
    "role": _local_role,
    "warehouse": _warehouse,
    "database": _database,
    "schema": _schema,
    "client_store_temporary_credential": _use_token_cache,
}

