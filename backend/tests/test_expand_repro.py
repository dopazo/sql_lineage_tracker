"""Tests for the node expansion (merge + rebuild) flow.

Verifies that expanding a truncated node correctly merges the scan results
with the existing graph and rebuilds the graph without errors.
"""

from lineage_tracker.graph import build_graph
from lineage_tracker.models import (
    ColumnInfo,
    GraphMetadata,
    LineageEdge,
    LineageGraph,
    LineageNode,
    ScanConfig,
    ScanStats,
)
from lineage_tracker.scanner import ScanResult


def _col(name: str, dtype: str = "STRING") -> ColumnInfo:
    return ColumnInfo(name=name, data_type=dtype)


# ---------------------------------------------------------------------------
# 1. Original graph (after scanning analytics dataset)
# ---------------------------------------------------------------------------
def _make_original_graph() -> LineageGraph:
    """Simulate the graph the user would have after scanning analytics."""
    nodes = {
        # Full analytics views
        "analytics.monthly_revenue": LineageNode(
            id="analytics.monthly_revenue",
            type="view",
            dataset="analytics",
            name="monthly_revenue",
            columns=[
                _col("month"), _col("pais"), _col("total_revenue", "FLOAT64"),
                _col("order_count", "INT64"),
            ],
            source="bigquery_view",
            sql='''SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais''',
        ),
        "analytics.product_sales": LineageNode(
            id="analytics.product_sales",
            type="view",
            dataset="analytics",
            name="product_sales",
            columns=[
                _col("product_id"), _col("product_name"), _col("category"),
                _col("unit_price", "FLOAT64"), _col("total_units_sold", "INT64"),
                _col("total_transactions", "INT64"), _col("channels_used", "INT64"),
                _col("last_sale_at", "TIMESTAMP"),
            ],
            source="bigquery_view",
            sql='''SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.unit_price,
  SUM(t.quantity) AS total_units_sold,
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  COUNT(DISTINCT t.channel) AS channels_used,
  MAX(t.sold_at) AS last_sale_at
FROM `rational-world-470315-b4.staging.products_clean` p
LEFT JOIN `rational-world-470315-b4.staging.transactions_clean` t
  ON p.product_id = t.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price''',
        ),
        # Truncated staging nodes (dataset not in scan scope)
        "staging.orders_with_customer": LineageNode(
            id="staging.orders_with_customer",
            type="table",
            dataset="staging",
            name="orders_with_customer",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
        "staging.products_clean": LineageNode(
            id="staging.products_clean",
            type="table",
            dataset="staging",
            name="products_clean",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
        "staging.transactions_clean": LineageNode(
            id="staging.transactions_clean",
            type="table",
            dataset="staging",
            name="transactions_clean",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
    }

    return LineageGraph(
        metadata=GraphMetadata(
            project_id="rational-world-470315-b4",
            generated_at="2026-03-11T10:00:00Z",
            scan_config=ScanConfig(
                target=None,
                datasets=["analytics"],
                depth=None,
            ),
            scan_stats=ScanStats(total_nodes=5, total_edges=2, truncated_nodes=3),
        ),
        nodes=nodes,
        edges=[],  # Edges would have been built by build_graph, but simplify for test
    )


# ---------------------------------------------------------------------------
# 2. Expand scan result (scanning from staging.orders_with_customer)
# ---------------------------------------------------------------------------
def _make_expand_scan_result() -> ScanResult:
    """Simulate what run_scoped_scan returns when expanding orders_with_customer."""
    nodes = {
        "staging.orders_with_customer": LineageNode(
            id="staging.orders_with_customer",
            type="view",
            dataset="staging",
            name="orders_with_customer",
            columns=[
                _col("id_pedido"), _col("revenue", "FLOAT64"),
                _col("created_at", "TIMESTAMP"), _col("nombre_cliente"),
                _col("pais"),
            ],
            source="bigquery_view",
            sql='''SELECT
  o.id_pedido,
  o.revenue,
  o.created_at,
  c.nombre AS nombre_cliente,
  c.pais
FROM `rational-world-470315-b4.staging.orders_clean` o
JOIN `rational-world-470315-b4.staging.customers_clean` c
  ON o.id_cliente = c.id_cliente''',
        ),
        "staging.orders_clean": LineageNode(
            id="staging.orders_clean",
            type="view",
            dataset="staging",
            name="orders_clean",
            columns=[
                _col("id_pedido"), _col("id_cliente"),
                _col("revenue", "FLOAT64"), _col("created_at", "TIMESTAMP"),
            ],
            source="bigquery_view",
            sql='''SELECT
  order_id AS id_pedido,
  customer_id AS id_cliente,
  amount AS revenue,
  created_at
FROM `rational-world-470315-b4.raw_data.orders`
WHERE status = "completed"''',
        ),
        "staging.customers_clean": LineageNode(
            id="staging.customers_clean",
            type="view",
            dataset="staging",
            name="customers_clean",
            columns=[
                _col("id_cliente"), _col("nombre"),
                _col("pais"), _col("fecha_registro", "TIMESTAMP"),
            ],
            source="bigquery_view",
            sql='''SELECT
  customer_id AS id_cliente,
  name AS nombre,
  UPPER(country) AS pais,
  registered_at AS fecha_registro
FROM `rational-world-470315-b4.raw_data.customers`''',
        ),
        "raw_data.orders": LineageNode(
            id="raw_data.orders",
            type="table",
            dataset="raw_data",
            name="orders",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
        "raw_data.customers": LineageNode(
            id="raw_data.customers",
            type="table",
            dataset="raw_data",
            name="customers",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
    }
    return ScanResult(nodes=nodes, errors=[])


# ---------------------------------------------------------------------------
# 3. Test: merge + build_graph (same as _run_expand does)
# ---------------------------------------------------------------------------
def test_expand_merge_and_build():
    """Reproduce the expand flow: merge existing graph nodes with
    scan results, then rebuild the graph.
    """
    original_graph = _make_original_graph()
    scan_result = _make_expand_scan_result()

    # Merge: same as _run_expand lines 618-625
    merged_nodes = dict(original_graph.nodes)
    merged_nodes.update(scan_result.nodes)

    merged_result = ScanResult(
        nodes=merged_nodes,
        errors=scan_result.errors,
    )

    # Preserve manual edges (none in this case)
    existing_manual_edges = [
        e for e in original_graph.edges if e.edge_type == "manual"
    ]

    # This is the call that fails in _run_expand
    new_graph = build_graph(
        merged_result,
        original_graph.metadata.scan_config,
        "rational-world-470315-b4",
        existing_manual_edges,
    )

    # 5 original + 5 from scan - 1 overlap (orders_with_customer) = 9
    assert len(new_graph.nodes) == 9
    assert "staging.orders_with_customer" in new_graph.nodes
    assert new_graph.nodes["staging.orders_with_customer"].type == "view"
    assert new_graph.nodes["staging.orders_with_customer"].sql is not None

    # Should have edges from the parsed views
    assert len(new_graph.edges) > 0

    # orders_with_customer should have edges to orders_clean and customers_clean
    owc_source_edges = [
        e for e in new_graph.edges
        if e.target_node == "staging.orders_with_customer"
    ]
    source_nodes = {e.source_node for e in owc_source_edges}
    assert "staging.orders_clean" in source_nodes
    assert "staging.customers_clean" in source_nodes


def test_expand_with_progress_callback():
    """Test expand merge with progress callback (simulates event_bus.publish)."""
    from lineage_tracker.graph import format_scan_report
    from lineage_tracker.persistence import save_graph, load_graph
    from pathlib import Path
    import tempfile

    original_graph = _make_original_graph()
    scan_result = _make_expand_scan_result()

    merged_nodes = dict(original_graph.nodes)
    merged_nodes.update(scan_result.nodes)

    merged_result = ScanResult(
        nodes=merged_nodes,
        errors=scan_result.errors,
    )

    existing_manual_edges = [
        e for e in original_graph.edges if e.edge_type == "manual"
    ]

    # Use a real progress callback (collects events)
    events: list[tuple[str, str | None]] = []
    def progress(event_type: str, message: str | None = None) -> None:
        events.append((event_type, message))

    new_graph = build_graph(
        merged_result,
        original_graph.metadata.scan_config,
        "rational-world-470315-b4",
        existing_manual_edges,
        progress=progress,
    )

    # Progress callback should have been called
    assert len(events) > 0
    event_types = [e[0] for e in events]
    assert "build_sort" in event_types
    assert "build_complete" in event_types

    # format_scan_report should work
    report = format_scan_report(new_graph, scan_result.errors)
    assert "SCAN REPORT" in report

    # Save and load should work
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        save_graph(new_graph, tmp, "rational-world-470315-b4")
        loaded = load_graph(tmp, "rational-world-470315-b4")
        assert loaded is not None
        assert len(loaded.nodes) == len(new_graph.nodes)


def test_expand_after_initial_build_graph():
    """More realistic test: build the initial graph first (like _run_scan does),
    then do the expand merge (like _run_expand does)."""

    # Step 1: Simulate initial scan (analytics dataset only)
    initial_scan_nodes = {
        "analytics.monthly_revenue": LineageNode(
            id="analytics.monthly_revenue",
            type="view",
            dataset="analytics",
            name="monthly_revenue",
            columns=[
                _col("month"), _col("pais"), _col("total_revenue", "FLOAT64"),
                _col("order_count", "INT64"),
            ],
            source="bigquery_view",
            sql='''SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais''',
        ),
        "analytics.product_sales": LineageNode(
            id="analytics.product_sales",
            type="view",
            dataset="analytics",
            name="product_sales",
            columns=[
                _col("product_id"), _col("product_name"), _col("category"),
                _col("unit_price", "FLOAT64"), _col("total_units_sold", "INT64"),
                _col("total_transactions", "INT64"), _col("channels_used", "INT64"),
                _col("last_sale_at", "TIMESTAMP"),
            ],
            source="bigquery_view",
            sql='''SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.unit_price,
  SUM(t.quantity) AS total_units_sold,
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  COUNT(DISTINCT t.channel) AS channels_used,
  MAX(t.sold_at) AS last_sale_at
FROM `rational-world-470315-b4.staging.products_clean` p
LEFT JOIN `rational-world-470315-b4.staging.transactions_clean` t
  ON p.product_id = t.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price''',
        ),
        # Truncated staging nodes
        "staging.orders_with_customer": LineageNode(
            id="staging.orders_with_customer",
            type="table",
            dataset="staging",
            name="orders_with_customer",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
        "staging.products_clean": LineageNode(
            id="staging.products_clean",
            type="table",
            dataset="staging",
            name="products_clean",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
        "staging.transactions_clean": LineageNode(
            id="staging.transactions_clean",
            type="table",
            dataset="staging",
            name="transactions_clean",
            source="unknown",
            status="truncated",
            status_message="Dataset not in scan scope",
        ),
    }

    initial_config = ScanConfig(target=None, datasets=["analytics"], depth=None)
    initial_result = ScanResult(nodes=initial_scan_nodes, errors=[])

    # Build the initial graph (just like _run_scan does)
    original_graph = build_graph(
        initial_result,
        initial_config,
        "rational-world-470315-b4",
    )

    assert len(original_graph.nodes) == 5
    assert original_graph.nodes["staging.orders_with_customer"].status == "truncated"

    # Step 2: Expand staging.orders_with_customer (just like _run_expand does)
    scan_result = _make_expand_scan_result()

    merged_nodes = dict(original_graph.nodes)
    merged_nodes.update(scan_result.nodes)

    merged_result = ScanResult(
        nodes=merged_nodes,
        errors=scan_result.errors,
    )

    existing_manual_edges = [
        e for e in original_graph.edges if e.edge_type == "manual"
    ]

    # This should NOT raise an exception
    new_graph = build_graph(
        merged_result,
        original_graph.metadata.scan_config,
        "rational-world-470315-b4",
        existing_manual_edges,
    )

    assert len(new_graph.nodes) == 9
    # The expanded node should now be a view with SQL
    expanded_node = new_graph.nodes["staging.orders_with_customer"]
    assert expanded_node.type == "view"
    assert expanded_node.status != "truncated"
    assert expanded_node.sql is not None
    assert len(expanded_node.columns) > 0
