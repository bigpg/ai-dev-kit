"""
Unity Catalog - Catalog Operations

Functions for listing and getting catalog information.
"""
from typing import List
from databricks.sdk.service.catalog import CatalogInfo

from ..auth import get_workspace_client


def list_catalogs() -> List[CatalogInfo]:
    """
    List all catalogs in Unity Catalog.

    Returns:
        List of CatalogInfo objects with catalog metadata

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    return list(w.catalogs.list())


def get_catalog(catalog_name: str) -> CatalogInfo:
    """
    Get detailed information about a specific catalog.

    Args:
        catalog_name: Name of the catalog

    Returns:
        CatalogInfo object with catalog metadata including:
        - name, full_name, owner, comment
        - created_at, updated_at
        - storage_location

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    return w.catalogs.get(name=catalog_name)
