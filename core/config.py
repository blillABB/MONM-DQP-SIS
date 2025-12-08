import os
import streamlit as st


def safe_secret(key, default=""):
    """Fetch a setting from Streamlit secrets, falling back to the environment."""
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)


# Data Lark API configuration - actual values should be in .streamlit/secrets.toml
DATALARK_URL = safe_secret("DATALARK_URL", "")
DATALARK_TOKEN = safe_secret("DATALARK_TOKEN", "")

