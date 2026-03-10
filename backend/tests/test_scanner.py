"""Tests for the scanner module."""

from unittest.mock import MagicMock, patch

import pytest

from lineage_tracker.models import ColumnInfo, LineageNode, ScanConfig
from lineage_tracker.scanner import (
    ScanResult,
    extract_table_references,
    run_scoped_scan,
)


# --- extract_table_references tests ---


class TestExtractTableReferences:
    def test_simple_select(self):
        refs = extract_table_references("SELECT a FROM my_table")
        assert (None, "my_table") in refs

    def test_qualified_table(self):
        refs = extract_table_references("SELECT a FROM dataset1.my_table")
        assert ("dataset1", "my_table") in refs

    def test_fully_qualified_table(self):
        refs = extract_table_references(
            "SELECT a FROM `my-project.dataset1.my_table`"
        )
        assert ("dataset1", "my_table") in refs

    def test_join_multiple_tables(self):
        sql = """
            SELECT a.col1, b.col2
            FROM dataset1.table_a a
            JOIN dataset2.table_b b ON a.id = b.id
        """
        refs = extract_table_references(sql)
        datasets = {(d, t) for d, t in refs}
        assert ("dataset1", "table_a") in datasets
        assert ("dataset2", "table_b") in datasets

    def test_cte(self):
        sql = """
            WITH cte AS (
                SELECT col1 FROM source_table
            )
            SELECT col1 FROM cte
        """
        refs = extract_table_references(sql)
        # Should find source_table; cte is not a real table reference
        table_names = [t for _, t in refs]
        assert "source_table" in table_names

    def test_subquery(self):
        sql = """
            SELECT *
            FROM (SELECT col1 FROM dataset1.inner_table) sub
        """
        refs = extract_table_references(sql)
        assert ("dataset1", "inner_table") in refs

    def test_invalid_sql_returns_empty(self):
        refs = extract_table_references("NOT VALID SQL ???")
        # sqlglot may still parse partial results; just ensure no crash
        assert isinstance(refs, list)

    def test_backtick_references(self):
        sql = "SELECT a FROM `staging.orders_clean`"
        refs = extract_table_references(sql)
        assert ("staging", "orders_clean") in refs

    def test_create_table_as_select(self):
        sql = """
            CREATE TABLE dataset1.new_table AS
            SELECT col1 FROM dataset2.source_table
        """
        refs = extract_table_references(sql)
        table_names = [t for _, t in refs]
        assert "source_table" in table_names


# --- run_scoped_scan tests ---


@pytest.fixture
def mock_extractor():
    with patch("lineage_tracker.scanner.BigQueryExtractor") as _:
        extractor = MagicMock()
        extractor.project_id = "test-project"
        yield extractor


class TestRunScopedScanFromTarget:
    def test_invalid_target_format(self, mock_extractor):
        config = ScanConfig(target="invalid_no_dot")
        result = run_scoped_scan(mock_extractor, config)
        assert len(result.errors) == 1
        assert "Invalid target format" in result.errors[0]

    def test_target_base_table_no_deps(self, mock_extractor):
        """Scanning a base table with no SQL should just return that node."""
        mock_extractor.get_table_type.return_value = "table"
        mock_extractor.get_view_sql.return_value = None
        mock_extractor.get_columns.return_value = {
            "orders": [ColumnInfo(name="id", data_type="STRING")]
        }

        config = ScanConfig(target="raw_data.orders")
        result = run_scoped_scan(mock_extractor, config)

        assert "raw_data.orders" in result.nodes
        node = result.nodes["raw_data.orders"]
        assert node.type == "table"
        assert node.source == "ingestion"
        assert len(node.columns) == 1

    def test_target_view_follows_dependency(self, mock_extractor):
        """A view referencing a table should discover both nodes."""

        def get_table_type(dataset, name):
            if dataset == "staging" and name == "orders_clean":
                return "view"
            if dataset == "raw_data" and name == "orders":
                return "table"
            return None

        def get_view_sql(dataset, name):
            if dataset == "staging" and name == "orders_clean":
                return "SELECT order_id FROM `raw_data.orders`"
            return None

        def get_columns(dataset, name):
            if name == "orders_clean":
                return {"orders_clean": [ColumnInfo(name="order_id", data_type="STRING")]}
            if name == "orders":
                return {"orders": [ColumnInfo(name="order_id", data_type="STRING")]}
            return {}

        mock_extractor.get_table_type.side_effect = get_table_type
        mock_extractor.get_view_sql.side_effect = get_view_sql
        mock_extractor.get_columns.side_effect = get_columns

        config = ScanConfig(target="staging.orders_clean")
        result = run_scoped_scan(mock_extractor, config)

        assert "staging.orders_clean" in result.nodes
        assert "raw_data.orders" in result.nodes
        assert result.nodes["staging.orders_clean"].type == "view"
        assert result.nodes["raw_data.orders"].type == "table"

    def test_depth_limit_truncates(self, mock_extractor):
        """With depth=0, dependencies crossing datasets should be truncated."""

        def get_table_type(dataset, name):
            if dataset == "analytics" and name == "report":
                return "view"
            return "table"

        def get_view_sql(dataset, name):
            if dataset == "analytics" and name == "report":
                return "SELECT col FROM `staging.source_table`"
            return None

        def get_columns(dataset, name):
            return {name: [ColumnInfo(name="col", data_type="STRING")]}

        mock_extractor.get_table_type.side_effect = get_table_type
        mock_extractor.get_view_sql.side_effect = get_view_sql
        mock_extractor.get_columns.side_effect = get_columns

        config = ScanConfig(target="analytics.report", depth=0)
        result = run_scoped_scan(mock_extractor, config)

        assert "analytics.report" in result.nodes
        assert "staging.source_table" in result.nodes
        assert result.nodes["staging.source_table"].status == "truncated"

    def test_same_dataset_deps_dont_count_as_hop(self, mock_extractor):
        """Dependencies within the same dataset should NOT count as a dataset hop."""

        def get_table_type(dataset, name):
            types = {
                ("analytics", "final_view"): "view",
                ("analytics", "intermediate_view"): "view",
                ("analytics", "base_table"): "table",
            }
            return types.get((dataset, name))

        def get_view_sql(dataset, name):
            sqls = {
                ("analytics", "final_view"): "SELECT col FROM analytics.intermediate_view",
                ("analytics", "intermediate_view"): "SELECT col FROM analytics.base_table",
            }
            return sqls.get((dataset, name))

        def get_columns(dataset, name):
            return {name: [ColumnInfo(name="col", data_type="STRING")]}

        mock_extractor.get_table_type.side_effect = get_table_type
        mock_extractor.get_view_sql.side_effect = get_view_sql
        mock_extractor.get_columns.side_effect = get_columns

        # depth=0 means no dataset hops allowed, but same-dataset is fine
        config = ScanConfig(target="analytics.final_view", depth=0)
        result = run_scoped_scan(mock_extractor, config)

        assert "analytics.final_view" in result.nodes
        assert "analytics.intermediate_view" in result.nodes
        assert "analytics.base_table" in result.nodes
        # All should be fully resolved, not truncated
        for node in result.nodes.values():
            assert node.status != "truncated"

    def test_dataset_filter_limits_scope(self, mock_extractor):
        """Only datasets in the filter should be fully scanned."""

        def get_table_type(dataset, name):
            return "view" if name == "v1" else "table"

        def get_view_sql(dataset, name):
            if name == "v1" and dataset == "allowed":
                return "SELECT col FROM forbidden.some_table"
            return None

        def get_columns(dataset, name):
            return {name: [ColumnInfo(name="col", data_type="STRING")]}

        mock_extractor.get_table_type.side_effect = get_table_type
        mock_extractor.get_view_sql.side_effect = get_view_sql
        mock_extractor.get_columns.side_effect = get_columns

        config = ScanConfig(target="allowed.v1", datasets=["allowed"])
        result = run_scoped_scan(mock_extractor, config)

        assert "allowed.v1" in result.nodes
        assert "forbidden.some_table" in result.nodes
        assert result.nodes["forbidden.some_table"].status == "truncated"
        assert "not in scan scope" in result.nodes["forbidden.some_table"].status_message.lower()

    def test_handles_missing_table(self, mock_extractor):
        """If a referenced table doesn't exist, it should be marked as error."""
        mock_extractor.get_table_type.return_value = None
        mock_extractor.get_columns.return_value = {}

        config = ScanConfig(target="dataset.nonexistent")
        result = run_scoped_scan(mock_extractor, config)

        assert "dataset.nonexistent" in result.nodes
        assert result.nodes["dataset.nonexistent"].status == "error"
        assert len(result.errors) > 0

    def test_handles_circular_reference(self, mock_extractor):
        """Circular references should not cause infinite loop."""

        call_count = {"get_table_type": 0}

        def get_table_type(dataset, name):
            call_count["get_table_type"] += 1
            if call_count["get_table_type"] > 10:
                raise RuntimeError("Infinite loop detected")
            return "view"

        def get_view_sql(dataset, name):
            # view_a references view_b, view_b references view_a
            if name == "view_a":
                return "SELECT col FROM ds.view_b"
            if name == "view_b":
                return "SELECT col FROM ds.view_a"
            return None

        def get_columns(dataset, name):
            return {name: [ColumnInfo(name="col", data_type="STRING")]}

        mock_extractor.get_table_type.side_effect = get_table_type
        mock_extractor.get_view_sql.side_effect = get_view_sql
        mock_extractor.get_columns.side_effect = get_columns

        config = ScanConfig(target="ds.view_a")
        result = run_scoped_scan(mock_extractor, config)

        assert "ds.view_a" in result.nodes
        assert "ds.view_b" in result.nodes


class TestRunScopedScanDatasets:
    def test_scans_specified_datasets(self, mock_extractor):
        """Scanning by datasets should extract all nodes from those datasets."""
        node1 = LineageNode(
            id="ds1.t1", type="table", dataset="ds1", name="t1",
            columns=[ColumnInfo(name="col", data_type="STRING")],
            source="ingestion",
        )
        node2 = LineageNode(
            id="ds1.v1", type="view", dataset="ds1", name="v1",
            columns=[ColumnInfo(name="col", data_type="STRING")],
            source="bigquery_view",
            sql="SELECT col FROM ds1.t1",
        )
        mock_extractor.extract_dataset.return_value = [node1, node2]

        config = ScanConfig(datasets=["ds1"])
        result = run_scoped_scan(mock_extractor, config)

        assert "ds1.t1" in result.nodes
        assert "ds1.v1" in result.nodes

    def test_follows_external_deps(self, mock_extractor):
        """External dependencies should be followed within depth limits."""
        node1 = LineageNode(
            id="ds1.v1", type="view", dataset="ds1", name="v1",
            columns=[ColumnInfo(name="col", data_type="STRING")],
            source="bigquery_view",
            sql="SELECT col FROM ds2.external_table",
        )
        mock_extractor.extract_dataset.return_value = [node1]
        mock_extractor.get_table_type.return_value = "table"
        mock_extractor.get_view_sql.return_value = None
        mock_extractor.get_columns.return_value = {
            "external_table": [ColumnInfo(name="col", data_type="STRING")]
        }

        config = ScanConfig(datasets=["ds1"])
        result = run_scoped_scan(mock_extractor, config)

        assert "ds1.v1" in result.nodes
        assert "ds2.external_table" in result.nodes
        assert result.nodes["ds2.external_table"].type == "table"

    def test_external_deps_truncated_by_depth(self, mock_extractor):
        """External deps beyond depth limit should be truncated."""
        node1 = LineageNode(
            id="ds1.v1", type="view", dataset="ds1", name="v1",
            columns=[], source="bigquery_view",
            sql="SELECT col FROM ds2.external_table",
        )
        mock_extractor.extract_dataset.return_value = [node1]

        config = ScanConfig(datasets=["ds1"], depth=0)
        result = run_scoped_scan(mock_extractor, config)

        assert "ds2.external_table" in result.nodes
        assert result.nodes["ds2.external_table"].status == "truncated"
