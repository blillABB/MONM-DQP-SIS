# core/unified_logs.py
"""
Unified logs reader for tracking rectified materials.

Reads Data Lark unified logs to determine which materials have been
successfully sent for rectification.
"""

import os
import pandas as pd
from pathlib import Path
from typing import Set


def load_unified_logs(base_dir: str = "Logs/Unified_Logs") -> pd.DataFrame:
    """
    Read and combine all Unified Log CSV files.

    Parameters
    ----------
    base_dir : str
        Directory containing unified log CSV files

    Returns
    -------
    pd.DataFrame
        Combined log entries with columns:
        Timestamp, Plugin, Material Number, Field, Extra, Status, Note
    """
    logs_path = Path(base_dir)
    if not logs_path.exists():
        return pd.DataFrame()

    files = sorted(logs_path.glob("Unified_Logs_*.csv"), key=os.path.getmtime, reverse=True)
    if not files:
        return pd.DataFrame()

    combined = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            df["Source File"] = f.name
            combined.append(df)
        except Exception as e:
            print(f"⚠️ Could not read {f.name}: {e}")

    if not combined:
        return pd.DataFrame()

    df_all = pd.concat(combined, ignore_index=True)
    df_all["Timestamp"] = pd.to_datetime(df_all["Timestamp"], errors="coerce")
    df_all = df_all.dropna(subset=["Timestamp"])
    return df_all


def get_rectified_materials(column: str = None) -> Set[str]:
    """
    Get set of material numbers that have been successfully rectified.

    Parameters
    ----------
    column : str, optional
        Filter by specific field/column name (e.g., "Planned Delivery Time").
        If None, returns all successfully rectified materials.

    Returns
    -------
    set
        Set of material numbers that have been successfully rectified
    """
    df = load_unified_logs()

    if df.empty:
        return set()

    # Filter for successful entries
    success_df = df[df["Status"] == "Success"]

    if success_df.empty:
        return set()

    # Filter by column/field if specified
    if column:
        success_df = success_df[success_df["Field"] == column]

    # Return unique material numbers
    materials = success_df["Material Number"].dropna().unique()
    return set(str(m) for m in materials)


def get_rectified_materials_with_details(column: str = None) -> pd.DataFrame:
    """
    Get DataFrame of rectified materials with full details.

    Parameters
    ----------
    column : str, optional
        Filter by specific field/column name

    Returns
    -------
    pd.DataFrame
        Rectified materials with Timestamp, Material Number, Field, Status, Note
    """
    df = load_unified_logs()

    if df.empty:
        return pd.DataFrame()

    # Filter for successful entries
    success_df = df[df["Status"] == "Success"]

    if column:
        success_df = success_df[success_df["Field"] == column]

    return success_df[["Timestamp", "Material Number", "Field", "Status", "Note"]].copy()


def check_material_rectified(material_number: str, column: str = None) -> bool:
    """
    Check if a specific material has been rectified.

    Parameters
    ----------
    material_number : str
        The material number to check
    column : str, optional
        Specific field/column to check

    Returns
    -------
    bool
        True if material has been successfully rectified
    """
    rectified = get_rectified_materials(column)
    return str(material_number) in rectified
