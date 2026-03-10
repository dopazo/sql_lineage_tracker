"""BigQuery metadata extraction module.

Connects to BigQuery and extracts table/view/column metadata
from INFORMATION_SCHEMA for lineage analysis.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.cloud import bigquery

from lineage_tracker.models import ColumnInfo, DatasetInfo, LineageNode, TableInfo

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class BigQueryExtractor:
    """Extracts metadata from BigQuery INFORMATION_SCHEMA."""

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

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
        query = f"""
            SELECT table_name, column_name, data_type
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        """
        if table_name:
            query += f"    WHERE table_name = '{table_name}'"

        query += "\n    ORDER BY table_name, ordinal_position"

        try:
            rows = self.client.query(query).result()
        except Exception:
            logger.warning("Failed to get columns for %s.%s", dataset_id, table_name or "*")
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

    def get_view_sql(self, dataset_id: str, view_name: str) -> str | None:
        """Get the SQL definition of a single view."""
        query = f"""
            SELECT view_definition
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.VIEWS`
            WHERE table_name = '{view_name}'
        """
        try:
            rows = list(self.client.query(query).result())
        except Exception:
            logger.warning("Failed to get SQL for view %s.%s", dataset_id, view_name)
            return None

        if rows:
            return rows[0].view_definition
        return None

    def get_table_type(self, dataset_id: str, table_name: str) -> str | None:
        """Get the type of a single table (table, view, materialized)."""
        query = f"""
            SELECT table_type
            FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table_name}'
        """
        try:
            rows = list(self.client.query(query).result())
        except Exception:
            logger.warning("Failed to get type for %s.%s", dataset_id, table_name)
            return None

        if rows:
            return _map_table_type(rows[0].table_type)
        return None


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
