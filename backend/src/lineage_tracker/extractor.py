"""BigQuery metadata extraction module.

Connects to BigQuery and extracts table/view/column metadata
from INFORMATION_SCHEMA for lineage analysis.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from google.cloud import bigquery

from lineage_tracker.models import ColumnInfo, DatasetInfo, LineageNode, TableInfo

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class _DatasetCache:
    """Cached metadata for a single dataset."""

    table_types: dict[str, str] = field(default_factory=dict)
    view_sqls: dict[str, str] = field(default_factory=dict)
    columns: dict[str, list[ColumnInfo]] = field(default_factory=dict)


class BigQueryExtractor:
    """Extracts metadata from BigQuery INFORMATION_SCHEMA."""

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self._cache: dict[str, _DatasetCache] = {}

    def list_datasets(self) -> list[DatasetInfo]:
        """List all datasets in the project with table/view counts."""
        datasets: list[DatasetInfo] = []

        try:
            dataset_refs = list(self.client.list_datasets())
        except Exception:
            logger.exception("Failed to list datasets for project %s", self.project_id)
            return datasets

        for ds_ref in dataset_refs:
            ds_id = ds_ref.dataset_id
            try:
                tables = list(self.client.list_tables(ds_id))
                table_count = sum(1 for t in tables if t.table_type == "TABLE")
                view_count = sum(1 for t in tables if t.table_type == "VIEW")
                datasets.append(DatasetInfo(id=ds_id, table_count=table_count, view_count=view_count))
            except Exception:
                logger.warning("Skipping dataset %s: insufficient permissions", ds_id)
                datasets.append(DatasetInfo(id=ds_id))

        return datasets

    def list_tables(self, dataset_id: str) -> list[TableInfo]:
        """List all tables and views in a dataset."""
        query = f"""
            SELECT table_name, table_type
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        """
        try:
            rows = self.client.query(query).result()
        except Exception:
            logger.warning("Failed to list tables for dataset %s", dataset_id)
            return []

        result: list[TableInfo] = []
        for row in rows:
            table_type = _map_table_type(row.table_type)
            result.append(TableInfo(name=row.table_name, type=table_type, dataset=dataset_id))
        return result

    def get_view_definitions(self, dataset_id: str) -> dict[str, str]:
        """Get SQL definitions for all views in a dataset.

        Returns a dict mapping view_name -> view_definition SQL.
        """
        query = f"""
            SELECT table_name, view_definition
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.VIEWS`
        """
        try:
            rows = self.client.query(query).result()
        except Exception:
            logger.warning("Failed to get view definitions for dataset %s", dataset_id)
            return {}

        return {row.table_name: row.view_definition for row in rows}

    def get_columns(self, dataset_id: str, table_name: str | None = None) -> dict[str, list[ColumnInfo]]:
        """Get column metadata for tables in a dataset.

        Args:
            dataset_id: The dataset to query.
            table_name: If provided, only get columns for this specific table.
                        Otherwise, get columns for all tables in the dataset.

        Returns:
            Dict mapping table_name -> list of ColumnInfo.
        """
        if table_name:
            # Use cache for single-table lookups
            self._ensure_cached(dataset_id)
            cached = self._cache[dataset_id].columns.get(table_name)
            if cached is not None:
                return {table_name: cached}
            return {}

        # Bulk query (used by _ensure_cached and extract_dataset)
        query = f"""
            SELECT table_name, column_name, data_type
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            ORDER BY table_name, ordinal_position
        """

        try:
            rows = self.client.query(query).result()
        except Exception:
            logger.warning("Failed to get columns for %s.*", dataset_id)
            return {}

        columns_by_table: dict[str, list[ColumnInfo]] = {}
        for row in rows:
            columns_by_table.setdefault(row.table_name, []).append(
                ColumnInfo(name=row.column_name, data_type=row.data_type)
            )
        return columns_by_table

    def extract_dataset(self, dataset_id: str) -> list[LineageNode]:
        """Extract all tables, views, and their columns from a dataset.

        Returns a list of LineageNode objects with full metadata.
        """
        tables = self.list_tables(dataset_id)
        view_defs = self.get_view_definitions(dataset_id)
        all_columns = self.get_columns(dataset_id)

        nodes: list[LineageNode] = []
        for table in tables:
            node_id = f"{dataset_id}.{table.name}"
            sql = view_defs.get(table.name)
            source = "bigquery_view" if table.type == "view" else "ingestion"

            node = LineageNode(
                id=node_id,
                type=table.type,
                dataset=dataset_id,
                name=table.name,
                columns=all_columns.get(table.name, []),
                source=source,
                sql=sql,
            )
            nodes.append(node)

        return nodes

    def _ensure_cached(self, dataset_id: str) -> None:
        """Ensure dataset metadata is cached (tables, views, columns in 3 bulk queries)."""
        if dataset_id in self._cache:
            return

        logger.info("Caching metadata for dataset %s (3 bulk queries)", dataset_id)
        cache = _DatasetCache()

        # 1. Bulk fetch all table types
        tables = self.list_tables(dataset_id)
        for t in tables:
            cache.table_types[t.name] = t.type

        # 2. Bulk fetch all view definitions
        cache.view_sqls = self.get_view_definitions(dataset_id)

        # 3. Bulk fetch all columns
        cache.columns = self.get_columns(dataset_id)

        self._cache[dataset_id] = cache

    def get_view_sql(self, dataset_id: str, view_name: str) -> str | None:
        """Get the SQL definition of a single view."""
        self._ensure_cached(dataset_id)
        return self._cache[dataset_id].view_sqls.get(view_name)

    def get_table_type(self, dataset_id: str, table_name: str) -> str | None:
        """Get the type of a single table (table, view, materialized)."""
        self._ensure_cached(dataset_id)
        return self._cache[dataset_id].table_types.get(table_name)


def _map_table_type(bq_type: str) -> str:
    """Map BigQuery table_type to our internal type."""
    mapping = {
        "BASE TABLE": "table",
        "VIEW": "view",
        "MATERIALIZED VIEW": "materialized",
        "EXTERNAL": "table",
        "SNAPSHOT": "table",
    }
    return mapping.get(bq_type, "table")
