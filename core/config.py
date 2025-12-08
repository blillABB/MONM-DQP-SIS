import os
import streamlit as st

def safe_secret(key, default=""):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)

def read_docker_secret(name):
    """Read a secret from Docker's /run/secrets directory."""
    path = f"/run/secrets/{name}"
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None

# Data Lark API configuration - actual values should be in .streamlit/secrets.toml
DATALARK_URL = safe_secret("DATALARK_URL", "")
DATALARK_TOKEN = safe_secret("DATALARK_TOKEN", "")

# Check if running in Docker with key-pair auth
_private_key_file = os.getenv("PRIVATE_KEY_FILE")
_private_key_pwd = read_docker_secret("snowflake_key_pwd")

# Allow warehouse override via environment variable
_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "WH_BU_READ")

if _private_key_file and os.path.exists(_private_key_file):
    # Docker environment with key-pair authentication
    SNOWFLAKE_CONFIG = {
        "account": "ABB-ABB_MO",
        "user": "SNOW@us.abb.com",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": _private_key_file,
        "private_key_file_pwd": _private_key_pwd,
        "role": "GR_MO_MONM",
        "warehouse": _warehouse,
        "database": "PROD_MO_MONM",
        "schema": "REPORTING",
    }
else:
    # Local development with browser SSO
    # User should be set via SNOWFLAKE_USER environment variable or Streamlit secrets
    _local_user = safe_secret("SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER", "dat.nguyen@us.abb.com"))
    _local_role = os.getenv("SNOWFLAKE_ROLE", "R_IS_MO_MONM")

    SNOWFLAKE_CONFIG = {
        "account": "ABB-ABB_MO",
        "user": _local_user,
        "authenticator": "externalbrowser",
        "role": _local_role,
        "warehouse": _warehouse,
        "database": "PROD_MO_MONM",
        "schema": "REPORTING",
    }