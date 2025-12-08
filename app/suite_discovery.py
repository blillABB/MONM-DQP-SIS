"""
Suite discovery utility for dynamically loading validation suites from YAML files.

This module scans the validation_yaml/ directory and parses suite metadata,
enabling dynamic report generation without hardcoded suite references.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Optional


def discover_suites(yaml_dir: Path = None) -> List[Dict]:
    """
    Discover all validation suites from YAML files.

    Parameters
    ----------
    yaml_dir : Path, optional
        Directory containing YAML files. Defaults to validation_yaml/ in project root.

    Returns
    -------
    List[Dict]
        List of suite configurations with keys:
        - suite_name: Display name (e.g., "Aurora_Motors_Validation")
        - suite_key: Cache key (lowercase, e.g., "aurora_motors_validation")
        - yaml_path: Path to YAML file
        - index_column: Column name for indexing failures
        - description: Suite description
        - data_source: Query function name
    """
    if yaml_dir is None:
        # Default to validation_yaml/ in project root
        project_root = Path(__file__).resolve().parents[1]
        yaml_dir = project_root / "validation_yaml"

    if not yaml_dir.exists():
        return []

    suites = []
    for yaml_file in sorted(yaml_dir.glob("*.yaml")):
        try:
            suite_config = parse_suite_yaml(yaml_file)
            if suite_config:
                suites.append(suite_config)
        except Exception as e:
            print(f"⚠️ Failed to parse {yaml_file.name}: {e}")
            continue

    return suites


def parse_suite_yaml(yaml_path: Path) -> Optional[Dict]:
    """
    Parse a single YAML validation suite file.

    Parameters
    ----------
    yaml_path : Path
        Path to YAML file

    Returns
    -------
    Dict or None
        Suite configuration dictionary or None if parsing fails
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data or "metadata" not in data:
        return None

    metadata = data["metadata"]
    suite_name = metadata.get("suite_name")

    if not suite_name:
        return None

    # Generate cache key (lowercase suite name)
    suite_key = suite_name.lower().replace(" ", "_")

    return {
        "suite_name": suite_name,
        "suite_key": suite_key,
        "yaml_path": yaml_path,
        "index_column": metadata.get("index_column", "MATERIAL_NUMBER"),
        "description": metadata.get("description", ""),
        "data_source": metadata.get("data_source", ""),
    }


def get_suite_by_name(suite_name: str, suites: List[Dict] = None) -> Optional[Dict]:
    """
    Get suite configuration by name.

    Parameters
    ----------
    suite_name : str
        Suite name to search for
    suites : List[Dict], optional
        List of suite configs. If None, will discover suites.

    Returns
    -------
    Dict or None
        Suite configuration or None if not found
    """
    if suites is None:
        suites = discover_suites()

    for suite in suites:
        if suite["suite_name"] == suite_name:
            return suite

    return None
