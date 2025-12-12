"""
Derived Status Resolver

This module provides a clean, centralized way to resolve derived statuses from
base expectation IDs to their scoped variants. It eliminates fragile string
matching and ensures the UI and runtime use the same resolution logic.

Key Features:
- Single source of truth for expectation catalog building
- Pre-resolved mappings from base IDs to scoped IDs
- Consistent behavior between YAML editor and validation runner
- No runtime string matching or iteration needed
"""

from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
from validations.sql_generator import build_scoped_expectation_id


class DerivedStatusResolver:
    """
    Resolves derived status expectation IDs to their scoped variants.

    This class builds a complete catalog of all expectations in a validation suite,
    creates mappings between base and scoped IDs, and pre-resolves derived status
    memberships so that runtime evaluation can use simple lookups instead of
    expensive string matching.

    Example:
        resolver = DerivedStatusResolver(validations, derived_statuses)
        scoped_ids = resolver.get_scoped_ids_for_derived("my_derived_status")
    """

    def __init__(self, validations: List[Dict[str, Any]], derived_statuses: Optional[List[Dict[str, Any]]] = None):
        """
        Initialize the resolver with validations and optional derived statuses.

        Args:
            validations: List of validation configurations from YAML
            derived_statuses: Optional list of derived status configurations
        """
        self.validations = validations
        self.derived_statuses = derived_statuses or []

        # Build the core data structures
        self.catalog = self._build_catalog()
        self.base_to_scoped_map = self._build_base_to_scoped_map()
        self.resolved_derived_statuses = self._resolve_all_derived_statuses()

    def _build_catalog(self) -> List[Dict[str, Any]]:
        """
        Build a complete catalog of all expectations with their scoped IDs.

        This is the single source of truth for how validations are expanded
        into individual scoped expectations. It handles all validation types
        and generates scoped IDs consistently.

        Returns:
            List of catalog entries, each containing:
                - scoped_id: The full scoped expectation ID
                - base_id: The base expectation ID (before scoping)
                - type: The validation type
                - targets: List of column/field names this expectation targets
                - discriminator: The string used to generate the scoped ID
        """
        catalog = []

        for validation in self.validations:
            val_type = validation.get("type", "")
            base_id = validation.get("expectation_id", "")

            if not base_id:
                continue  # Skip validations without IDs

            # Handle each validation type and expand to scoped expectations
            if val_type == "expect_column_values_to_not_be_null":
                for col in validation.get("columns", []):
                    catalog.append({
                        "scoped_id": build_scoped_expectation_id(validation, col),
                        "base_id": base_id,
                        "type": val_type,
                        "targets": [col],
                        "discriminator": col,
                    })

            elif val_type == "expect_column_values_to_be_in_set":
                for column in validation.get("rules", {}).keys():
                    catalog.append({
                        "scoped_id": build_scoped_expectation_id(validation, column),
                        "base_id": base_id,
                        "type": val_type,
                        "targets": [column],
                        "discriminator": column,
                    })

            elif val_type == "expect_column_values_to_not_be_in_set":
                column = validation.get("column")
                if column:
                    catalog.append({
                        "scoped_id": build_scoped_expectation_id(validation, column),
                        "base_id": base_id,
                        "type": val_type,
                        "targets": [column],
                        "discriminator": column,
                    })

            elif val_type in {
                "expect_column_pair_values_to_be_equal",
                "expect_column_pair_values_a_to_be_greater_than_b",
                "custom:conditional_required",
                "custom:conditional_value_in_set",
            }:
                col_a = validation.get("column_a") or validation.get("condition_column")
                col_b = (
                    validation.get("column_b")
                    or validation.get("required_column")
                    or validation.get("target_column")
                )
                if col_a and col_b:
                    discriminator = "|".join([col_a, col_b])
                    catalog.append({
                        "scoped_id": build_scoped_expectation_id(validation, discriminator),
                        "base_id": base_id,
                        "type": val_type,
                        "targets": [col_a, col_b],
                        "discriminator": discriminator,
                    })

            elif isinstance(validation.get("columns"), list) and validation["columns"]:
                # Generic handler for single-column list expectations (regex, lengths, ranges, uniqueness, etc.)
                for column in validation["columns"]:
                    catalog.append({
                        "scoped_id": build_scoped_expectation_id(validation, column),
                        "base_id": base_id,
                        "type": val_type,
                        "targets": [column],
                        "discriminator": column,
                    })

            elif isinstance(validation.get("column_list"), list) and validation["column_list"]:
                # Compound unique checks target a set of columns together
                discriminator = "|".join(validation["column_list"])
                catalog.append({
                    "scoped_id": build_scoped_expectation_id(validation, discriminator),
                    "base_id": base_id,
                    "type": val_type,
                    "targets": validation["column_list"],
                    "discriminator": discriminator,
                })

            else:
                # Unknown validation types get a catalog entry with base_id only
                catalog.append({
                    "scoped_id": base_id,
                    "base_id": base_id,
                    "type": val_type,
                    "targets": [],
                    "discriminator": "",
                })

        return catalog

    def _build_base_to_scoped_map(self) -> Dict[str, List[str]]:
        """
        Create an explicit mapping from base IDs to their scoped variants.

        This eliminates the need for string prefix matching at runtime.

        Returns:
            Dictionary mapping base_id -> [scoped_id1, scoped_id2, ...]
        """
        mapping = defaultdict(list)

        for entry in self.catalog:
            base_id = entry["base_id"]
            scoped_id = entry["scoped_id"]
            mapping[base_id].append(scoped_id)

        return dict(mapping)

    def _resolve_all_derived_statuses(self) -> List[Dict[str, Any]]:
        """
        Resolve all derived statuses to their scoped expectation IDs.

        Supports two modes:
        1. Legacy: expectation_ids list (pre-resolved specific IDs)
        2. Filter-based: expectation_type + columns (runtime filtering)

        The filter-based approach is cleaner for cases where you have one
        validation checking many columns but want to group subsets differently.

        Returns:
            List of derived statuses with added "resolved_scoped_ids" field
        """
        resolved = []

        for derived_status in self.derived_statuses:
            # Check if using filter-based approach (expectation_type + columns)
            expectation_type = derived_status.get("expectation_type")
            filter_columns = derived_status.get("columns", [])

            if filter_columns:
                # NEW: Filter-based resolution
                # Find all catalog entries matching type and columns
                resolved_scoped_ids = []
                for entry in self.catalog:
                    # Match by expectation type (if specified)
                    if expectation_type and entry["type"] != expectation_type:
                        continue

                    # Match if entry targets any of the specified columns
                    entry_targets = entry.get("targets", [])
                    if any(target in filter_columns for target in entry_targets):
                        resolved_scoped_ids.append(entry["scoped_id"])

                resolved_entry = {
                    **derived_status,
                    "resolved_scoped_ids": resolved_scoped_ids,
                    "missing_ids": [],  # N/A for filter-based approach
                    "resolution_mode": "filter",
                }
                resolved.append(resolved_entry)

            else:
                # LEGACY: expectation_ids list (backward compatibility)
                expectation_ids = derived_status.get("expectation_ids", [])
                resolved_scoped_ids = []
                missing_ids = []

                for exp_id in expectation_ids:
                    scoped_ids = self.base_to_scoped_map.get(exp_id, [])
                    if scoped_ids:
                        resolved_scoped_ids.extend(scoped_ids)
                    else:
                        # Check if it's already a scoped ID (direct match in catalog)
                        if any(entry["scoped_id"] == exp_id for entry in self.catalog):
                            resolved_scoped_ids.append(exp_id)
                        else:
                            missing_ids.append(exp_id)

                resolved_entry = {
                    **derived_status,
                    "resolved_scoped_ids": resolved_scoped_ids,
                    "missing_ids": missing_ids,
                    "resolution_mode": "ids",
                }
                resolved.append(resolved_entry)

        return resolved

    def get_scoped_ids_for_derived(self, derived_status_id: str) -> List[str]:
        """
        Get the pre-resolved scoped IDs for a derived status.

        Args:
            derived_status_id: The expectation_id of the derived status

        Returns:
            List of scoped expectation IDs, or empty list if not found
        """
        for derived in self.resolved_derived_statuses:
            if derived.get("expectation_id") == derived_status_id:
                return derived.get("resolved_scoped_ids", [])
        return []

    def get_resolved_derived_status(self, derived_status_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the complete resolved derived status entry.

        Args:
            derived_status_id: The expectation_id of the derived status

        Returns:
            The resolved derived status dict, or None if not found
        """
        for derived in self.resolved_derived_statuses:
            if derived.get("expectation_id") == derived_status_id:
                return derived
        return None

    def get_catalog_for_ui(self) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, str]]:
        """
        Get catalog data formatted for the YAML editor UI.

        Returns a tuple of (catalog, label_lookup, target_lookup) that can be
        used directly in the Streamlit multiselect interface.

        Returns:
            Tuple of:
                - catalog: List of entries with id, label, type, targets
                - label_lookup: Dict mapping expectation_id -> human-readable label
                - target_lookup: Dict mapping target names to themselves (for deduplication)
        """
        ui_catalog = []
        label_lookup = {}
        target_lookup = {}

        for entry in self.catalog:
            scoped_id = entry["scoped_id"]
            base_id = entry["base_id"]
            val_type = entry["type"]
            targets = entry["targets"]

            # Build human-readable label
            target_text = ", ".join(targets) if targets else "(no column/field)"
            label = f"{scoped_id} — {val_type} on {target_text}" if val_type else scoped_id

            ui_catalog.append({
                "id": scoped_id,
                "label": label,
                "type": val_type,
                "targets": targets,
            })

            label_lookup[scoped_id] = label
            label_lookup.setdefault(base_id, f"{base_id} — {val_type}")

            # Track unique targets
            for target in targets:
                target_lookup[target] = target
            if not targets:
                target_lookup["(no column/field)"] = "(no column/field)"

        return ui_catalog, label_lookup, target_lookup

    def get_scoped_ids_for_base_id(self, base_id: str) -> List[str]:
        """
        Get all scoped IDs for a given base expectation ID.

        This is useful when you have a base ID and need to find all its
        scoped variants (e.g., for display or filtering purposes).

        Args:
            base_id: The base expectation ID

        Returns:
            List of scoped expectation IDs
        """
        return self.base_to_scoped_map.get(base_id, [])

    def resolve_expectation_ids(self, expectation_ids: List[str]) -> Tuple[List[str], List[str]]:
        """
        Resolve a list of expectation IDs to their scoped variants.

        This is a lower-level method that can be used to resolve arbitrary
        lists of expectation IDs, not just those in derived statuses.

        Args:
            expectation_ids: List of base or scoped expectation IDs

        Returns:
            Tuple of (resolved_ids, missing_ids)
        """
        resolved = []
        missing = []

        for exp_id in expectation_ids:
            # Try to map as base ID first
            scoped_ids = self.base_to_scoped_map.get(exp_id, [])
            if scoped_ids:
                resolved.extend(scoped_ids)
            # Check if it's already a scoped ID
            elif any(entry["scoped_id"] == exp_id for entry in self.catalog):
                resolved.append(exp_id)
            else:
                missing.append(exp_id)

        return resolved, missing

    def get_all_resolved_derived_statuses(self) -> List[Dict[str, Any]]:
        """
        Get all resolved derived statuses.

        Returns:
            List of all derived statuses with their resolved scoped IDs
        """
        return self.resolved_derived_statuses
