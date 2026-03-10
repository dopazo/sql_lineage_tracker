"""Tests for the BigQuery extractor module."""

from unittest.mock import MagicMock, patch

import pytest

from lineage_tracker.extractor import BigQueryExtractor, _map_table_type


class TestMapTableType:
    def test_base_table(self):
        assert _map_table_type("BASE TABLE") == "table"

    def test_view(self):
        assert _map_table_type("VIEW") == "view"

    def test_materialized_view(self):
        assert _map_table_type("MATERIALIZED VIEW") == "materialized"

    def test_external(self):
        assert _map_table_type("EXTERNAL") == "table"

    def test_unknown_defaults_to_table(self):
        assert _map_table_type("SOMETHING_ELSE") == "table"


@pytest.fixture
def mock_bq_client():
    with patch("lineage_tracker.extractor.bigquery.Client") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        yield mock_client


@pytest.fixture
def extractor(mock_bq_client):
    return BigQueryExtractor("test-project")


class TestListDatasets:
    def test_returns_datasets_with_counts(self, extractor, mock_bq_client):
        # Mock dataset refs
        ds1 = MagicMock()
        ds1.dataset_id = "raw_data"
        ds2 = MagicMock()
        ds2.dataset_id = "staging"
        mock_bq_client.list_datasets.return_value = [ds1, ds2]

        # Mock tables for each dataset
        table1 = MagicMock()
        table1.table_type = "TABLE"
        view1 = MagicMock()
        view1.table_type = "VIEW"
        view2 = MagicMock()
        view2.table_type = "VIEW"

        mock_bq_client.list_tables.side_effect = [
            [table1, view1],  # raw_data: 1 table, 1 view
            [view2],  # staging: 0 tables, 1 view
        ]

        datasets = extractor.list_datasets()

        assert len(datasets) == 2
        assert datasets[0].id == "raw_data"
        assert datasets[0].table_count == 1
        assert datasets[0].view_count == 1
        assert datasets[1].id == "staging"
        assert datasets[1].table_count == 0
        assert datasets[1].view_count == 1

    def test_handles_permission_error(self, extractor, mock_bq_client):
        ds1 = MagicMock()
        ds1.dataset_id = "restricted"
        mock_bq_client.list_datasets.return_value = [ds1]
        mock_bq_client.list_tables.side_effect = Exception("403 Forbidden")

        datasets = extractor.list_datasets()

        assert len(datasets) == 1
        assert datasets[0].id == "restricted"
        assert datasets[0].table_count == 0
        assert datasets[0].view_count == 0

    def test_handles_list_datasets_failure(self, extractor, mock_bq_client):
        mock_bq_client.list_datasets.side_effect = Exception("Network error")

        datasets = extractor.list_datasets()
        assert datasets == []


class TestListTables:
    def test_returns_tables(self, extractor, mock_bq_client):
        row1 = MagicMock()
        row1.table_name = "orders"
        row1.table_type = "BASE TABLE"
        row2 = MagicMock()
        row2.table_name = "orders_clean"
        row2.table_type = "VIEW"

        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [row1, row2]
        mock_bq_client.query.return_value = mock_query_result

        tables = extractor.list_tables("raw_data")

        assert len(tables) == 2
        assert tables[0].name == "orders"
        assert tables[0].type == "table"
        assert tables[0].dataset == "raw_data"
        assert tables[1].name == "orders_clean"
        assert tables[1].type == "view"

    def test_handles_error(self, extractor, mock_bq_client):
        mock_bq_client.query.side_effect = Exception("403 Forbidden")
        tables = extractor.list_tables("restricted")
        assert tables == []


class TestGetViewDefinitions:
    def test_returns_view_sql(self, extractor, mock_bq_client):
        row = MagicMock()
        row.table_name = "orders_clean"
        row.view_definition = "SELECT * FROM orders"

        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [row]
        mock_bq_client.query.return_value = mock_query_result

        views = extractor.get_view_definitions("staging")

        assert views == {"orders_clean": "SELECT * FROM orders"}

    def test_handles_error(self, extractor, mock_bq_client):
        mock_bq_client.query.side_effect = Exception("error")
        assert extractor.get_view_definitions("x") == {}


class TestGetColumns:
    def test_returns_columns_by_table(self, extractor, mock_bq_client):
        rows = []
        for tname, cname, dtype in [
            ("orders", "order_id", "STRING"),
            ("orders", "amount", "FLOAT64"),
            ("users", "user_id", "INT64"),
        ]:
            row = MagicMock()
            row.table_name = tname
            row.column_name = cname
            row.data_type = dtype
            rows.append(row)

        mock_query_result = MagicMock()
        mock_query_result.result.return_value = rows
        mock_bq_client.query.return_value = mock_query_result

        columns = extractor.get_columns("raw_data")

        assert "orders" in columns
        assert len(columns["orders"]) == 2
        assert columns["orders"][0].name == "order_id"
        assert columns["orders"][0].data_type == "STRING"
        assert "users" in columns
        assert len(columns["users"]) == 1

    def test_filters_by_table_name(self, extractor, mock_bq_client):
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = []
        mock_bq_client.query.return_value = mock_query_result

        extractor.get_columns("raw_data", table_name="orders")

        call_args = mock_bq_client.query.call_args[0][0]
        assert "WHERE table_name = 'orders'" in call_args

    def test_handles_error(self, extractor, mock_bq_client):
        mock_bq_client.query.side_effect = Exception("error")
        assert extractor.get_columns("x") == {}


class TestExtractDataset:
    def test_combines_tables_views_columns(self, extractor, mock_bq_client):
        # Mock list_tables query
        table_row = MagicMock()
        table_row.table_name = "orders"
        table_row.table_type = "BASE TABLE"
        view_row = MagicMock()
        view_row.table_name = "orders_clean"
        view_row.table_type = "VIEW"

        # Mock view definitions query
        view_def_row = MagicMock()
        view_def_row.table_name = "orders_clean"
        view_def_row.view_definition = "SELECT order_id FROM orders"

        # Mock columns query
        col1 = MagicMock()
        col1.table_name = "orders"
        col1.column_name = "order_id"
        col1.data_type = "STRING"
        col2 = MagicMock()
        col2.table_name = "orders_clean"
        col2.column_name = "order_id"
        col2.data_type = "STRING"

        def query_side_effect(sql):
            result = MagicMock()
            if "INFORMATION_SCHEMA.TABLES" in sql:
                result.result.return_value = [table_row, view_row]
            elif "INFORMATION_SCHEMA.VIEWS" in sql:
                result.result.return_value = [view_def_row]
            elif "INFORMATION_SCHEMA.COLUMNS" in sql:
                result.result.return_value = [col1, col2]
            return result

        mock_bq_client.query.side_effect = query_side_effect

        nodes = extractor.extract_dataset("raw_data")

        assert len(nodes) == 2

        orders_node = next(n for n in nodes if n.name == "orders")
        assert orders_node.type == "table"
        assert orders_node.source == "ingestion"
        assert orders_node.sql is None
        assert len(orders_node.columns) == 1

        view_node = next(n for n in nodes if n.name == "orders_clean")
        assert view_node.type == "view"
        assert view_node.source == "bigquery_view"
        assert view_node.sql == "SELECT order_id FROM orders"
        assert len(view_node.columns) == 1


class TestGetViewSql:
    def test_returns_sql(self, extractor, mock_bq_client):
        row = MagicMock()
        row.view_definition = "SELECT 1"
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [row]
        mock_bq_client.query.return_value = mock_query_result

        sql = extractor.get_view_sql("ds", "my_view")
        assert sql == "SELECT 1"

    def test_returns_none_for_missing(self, extractor, mock_bq_client):
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = []
        mock_bq_client.query.return_value = mock_query_result

        assert extractor.get_view_sql("ds", "no_view") is None

    def test_handles_error(self, extractor, mock_bq_client):
        mock_bq_client.query.side_effect = Exception("error")
        assert extractor.get_view_sql("ds", "v") is None


class TestGetTableType:
    def test_returns_type(self, extractor, mock_bq_client):
        row = MagicMock()
        row.table_type = "VIEW"
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [row]
        mock_bq_client.query.return_value = mock_query_result

        assert extractor.get_table_type("ds", "my_view") == "view"

    def test_returns_none_for_missing(self, extractor, mock_bq_client):
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = []
        mock_bq_client.query.return_value = mock_query_result

        assert extractor.get_table_type("ds", "nope") is None
