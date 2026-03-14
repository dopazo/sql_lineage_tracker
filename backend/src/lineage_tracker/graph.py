"""Graph construction module.

Takes nodes from the scanner and builds a complete LineageGraph by:
1. Topologically sorting views (dependencies before dependents).
2. Parsing each view's SQL in order using sqlglot.
3. Detecting holes (orphan nodes, terminal nodes).
4. Marking columns with unresolved lineage.
5. Computing scan statistics.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

from lineage_tracker.models import (
    GraphMetadata,
    LineageEdge,
    LineageGraph,
    LineageNode,
    ProgressCallback,
    ScanConfig,
    ScanStats,
    _NOOP_PROGRESS,
)
from lineage_tracker.parser import contains_dynamic_sql, parse_view_lineage
from lineage_tracker.scanner import ScanResult, extract_table_references

logger = logging.getLogger(__name__)


def topological_sort(nodes: dict[str, LineageNode]) -> list[str]:
    """Sort view nodes in topological order (dependencies first).

    Base tables (no SQL) come first, then views ordered so that each
    view's dependencies appear before it in the list.

    Uses Kahn's algorithm. If cycles exist, remaining nodes are appended
    at the end (the cycle is broken arbitrarily).

    Args:
        nodes: Dict of node_id -> LineageNode.

    Returns:
        List of node_ids in topological order.
    """
    # Build adjacency: for each view, find which other nodes it depends on
    # dependency_of[A] = [B, C] means A is a dependency of B and C
    dependents: dict[str, list[str]] = defaultdict(list)  # dep -> [nodes that depend on it]
    in_degree: dict[str, int] = {node_id: 0 for node_id in nodes}

    for node_id, node in nodes.items():
        if not node.sql:
            continue

        refs = extract_table_references(node.sql)
        for ref_dataset, ref_name in refs:
            if ref_dataset:
                dep_id = f"{ref_dataset}.{ref_name}"
            else:
                # Unqualified: assume same dataset
                dep_id = f"{node.dataset}.{ref_name}"

            if dep_id in nodes and dep_id != node_id:
                dependents[dep_id].append(node_id)
                in_degree[node_id] += 1

    # Kahn's algorithm
    queue: deque[str] = deque(nid for nid, deg in in_degree.items() if deg == 0)
    sorted_ids: list[str] = []

    while queue:
        nid = queue.popleft()
        sorted_ids.append(nid)
        for dependent in dependents.get(nid, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # If there are remaining nodes (cycles), append them
    remaining = [nid for nid in nodes if nid not in set(sorted_ids)]
    if remaining:
        logger.warning(
            "Cycle detected in dependency graph, %d nodes involved: %s",
            len(remaining),
            remaining,
        )
        sorted_ids.extend(remaining)

    return sorted_ids


def _parse_view_task(
    node_id: str,
    sql: str,
    schemas: dict[str, dict[str, str]],
) -> tuple[str, list[LineageEdge] | None]:
    """Worker function for parallel view parsing (must be module-level for pickling)."""
    try:
        edges = parse_view_lineage(node_id, sql, schemas)
        return (node_id, edges)
    except Exception:
        logging.getLogger(__name__).exception("Failed to parse lineage for %s", node_id)
        return (node_id, None)


def build_graph(
    scan_result: ScanResult,
    config: ScanConfig,
    project_id: str,
    existing_manual_edges: list[LineageEdge] | None = None,
    progress: ProgressCallback = _NOOP_PROGRESS,
) -> LineageGraph:
    """Build a complete LineageGraph from scan results.

    1. Topologically sorts the scanned nodes.
    2. Parses each view in order, building up schemas progressively.
    3. Collects all edges (automatic + preserved manual).
    4. Detects holes and computes statistics.

    Args:
        scan_result: Result from run_scoped_scan().
        config: The scan configuration used.
        project_id: GCP project ID.
        existing_manual_edges: Manual edges to preserve from a previous graph.

    Returns:
        Complete LineageGraph ready for serialization.
    """
    nodes = scan_result.nodes

    # Build known schemas from node columns
    schemas: dict[str, dict[str, str]] = {}
    for node_id, node in nodes.items():
        if node.columns:
            schemas[node_id] = {col.name.lower(): col.data_type for col in node.columns}

    # Sort views topologically
    progress("build_sort", "Sorting views in topological order...")
    sorted_ids = topological_sort(nodes)
    logger.info("Topological order: %d nodes sorted", len(sorted_ids))

    # Count views that will be parsed (have SQL)
    views_to_parse = [nid for nid in sorted_ids if nodes[nid].sql]
    total_views = len(views_to_parse)
    parsed_count = 0

    # Pre-filter views: handle dynamic SQL and missing schemas in main thread
    all_edges: list[LineageEdge] = []
    parse_errors = 0
    parseable_views: list[str] = []

    for node_id in sorted_ids:
        node = nodes[node_id]
        if not node.sql:
            continue

        if contains_dynamic_sql(node.sql):
            logger.warning(
                "View %s contains dynamic SQL (EXECUTE IMMEDIATE) — "
                "lineage cannot be traced automatically",
                node_id,
            )
            node.status = "warning"
            node.status_message = (
                "Contains dynamic SQL (EXECUTE IMMEDIATE) — "
                "lineage cannot be traced automatically"
            )
            for col in node.columns:
                col.lineage_status = "unknown"
            parse_errors += 1
            continue

        view_schema = schemas.get(node_id, {})
        if not view_schema:
            logger.warning("No schema for view %s, skipping lineage parse", node_id)
            if node.status == "ok":
                node.status = "warning"
                node.status_message = "No schema available for lineage parsing"
            parse_errors += 1
            continue

        parseable_views.append(node_id)

    total_parseable = len(parseable_views)
    max_workers = min(os.cpu_count() or 4, 8)

    if total_parseable <= 1 or max_workers <= 1:
        # Sequential fallback
        for idx, node_id in enumerate(parseable_views, 1):
            node = nodes[node_id]
            progress("build_parse", f"Parsing lineage for {node_id} ({idx}/{total_views})")
            try:
                edges = parse_view_lineage(node_id, node.sql, schemas)
                all_edges.extend(edges)
                _mark_column_lineage_status(node, edges)
                if not edges:
                    parse_errors += 1
                    if node.status == "ok":
                        node.status = "warning"
                        node.status_message = "Lineage could not be resolved"
            except Exception:
                logger.exception("Failed to parse lineage for %s", node_id)
                if node.status == "ok":
                    node.status = "warning"
                    node.status_message = "Lineage parsing failed"
                parse_errors += 1
                for col in node.columns:
                    col.lineage_status = "unknown"
    else:
        # Parallel parsing with ProcessPoolExecutor
        logger.info("Parsing %d views in parallel (max_workers=%d)", total_parseable, max_workers)
        parsed_count = 0

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_parse_view_task, nid, nodes[nid].sql, schemas): nid
                for nid in parseable_views
            }

            for future in as_completed(futures):
                node_id = futures[future]
                node = nodes[node_id]
                parsed_count += 1
                progress("build_parse", f"Parsed lineage for {node_id} ({parsed_count}/{total_views})")

                try:
                    _, edges = future.result()
                except Exception:
                    logger.exception("Worker crashed for %s", node_id)
                    edges = None

                if edges is not None:
                    all_edges.extend(edges)
                    _mark_column_lineage_status(node, edges)
                    if not edges:
                        parse_errors += 1
                        if node.status == "ok":
                            node.status = "warning"
                            node.status_message = "Lineage could not be resolved"
                else:
                    if node.status == "ok":
                        node.status = "warning"
                        node.status_message = "Lineage parsing failed"
                    parse_errors += 1
                    for col in node.columns:
                        col.lineage_status = "unknown"

    # Preserve manual edges from previous graph
    manual_edges: list[LineageEdge] = []
    if existing_manual_edges:
        for edge in existing_manual_edges:
            if edge.edge_type == "manual":
                # Check if referenced nodes still exist
                if edge.source_node in nodes and edge.target_node in nodes:
                    manual_edges.append(edge)
                else:
                    missing = []
                    if edge.source_node not in nodes:
                        missing.append(edge.source_node)
                    if edge.target_node not in nodes:
                        missing.append(edge.target_node)
                    logger.warning(
                        "Manual edge %s references missing nodes: %s",
                        edge.id,
                        missing,
                    )

    combined_edges = all_edges + manual_edges

    # Detect holes and compute stats
    stats = _compute_stats(nodes, combined_edges, parse_errors)

    metadata = GraphMetadata(
        project_id=project_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        scan_config=config,
        scan_stats=stats,
    )

    graph = LineageGraph(
        metadata=metadata,
        nodes=nodes,
        edges=combined_edges,
    )

    _log_report(stats, scan_result.errors)
    progress(
        "build_complete",
        f"Graph built: {stats.total_nodes} nodes, {stats.total_edges} edges",
    )

    return graph


def _mark_column_lineage_status(
    node: LineageNode,
    edges: list[LineageEdge],
) -> None:
    """Mark columns as resolved or unknown based on parsed edges.

    A column is "resolved" if it appears as a target_column in at least
    one edge's column_mappings with a non-unknown transformation.
    """
    resolved_cols: set[str] = set()

    for edge in edges:
        if edge.target_node != node.id:
            continue
        for mapping in edge.column_mappings:
            if mapping.transformation != "unknown":
                resolved_cols.add(mapping.target_column)

    for col in node.columns:
        if col.name.lower() in resolved_cols:
            col.lineage_status = "resolved"
        else:
            col.lineage_status = "unknown"


def _compute_stats(
    nodes: dict[str, LineageNode],
    edges: list[LineageEdge],
    parse_errors: int,
) -> ScanStats:
    """Compute graph statistics and detect holes."""
    # Count nodes by type
    nodes_by_type: dict[str, int] = defaultdict(int)
    truncated_nodes = 0
    for node in nodes.values():
        nodes_by_type[node.type] += 1
        if node.status == "truncated":
            truncated_nodes += 1

    # Build edge sets for hole detection
    sources_in_edges: set[str] = set()  # nodes that appear as source
    targets_in_edges: set[str] = set()  # nodes that appear as target

    for edge in edges:
        sources_in_edges.add(edge.source_node)
        targets_in_edges.add(edge.target_node)

    # Orphan nodes: no incoming edges AND no outgoing edges
    orphan_nodes = 0
    # Terminal nodes: have incoming edges but no outgoing (leaf consumers)
    terminal_nodes = 0

    for node_id in nodes:
        has_incoming = node_id in targets_in_edges
        has_outgoing = node_id in sources_in_edges

        if not has_incoming and not has_outgoing:
            orphan_nodes += 1
        elif has_incoming and not has_outgoing:
            terminal_nodes += 1

    return ScanStats(
        total_nodes=len(nodes),
        total_edges=len(edges),
        nodes_by_type=dict(nodes_by_type),
        orphan_nodes=orphan_nodes,
        terminal_nodes=terminal_nodes,
        truncated_nodes=truncated_nodes,
        parse_errors=parse_errors,
    )


def _log_report(stats: ScanStats, errors: list[str]) -> None:
    """Log a summary report of the scan results."""
    logger.info(
        "Graph built: %d nodes, %d edges",
        stats.total_nodes,
        stats.total_edges,
    )
    logger.info("  Nodes by type: %s", stats.nodes_by_type)

    if stats.orphan_nodes:
        logger.warning("  Orphan nodes (no connections): %d", stats.orphan_nodes)
    if stats.terminal_nodes:
        logger.info("  Terminal nodes (consumers): %d", stats.terminal_nodes)
    if stats.truncated_nodes:
        logger.info("  Truncated nodes (depth limit): %d", stats.truncated_nodes)
    if stats.parse_errors:
        logger.warning("  Parse errors: %d", stats.parse_errors)

    if errors:
        logger.warning("  Scan errors:")
        for err in errors:
            logger.warning("    - %s", err)


def format_scan_report(
    graph: LineageGraph, errors: list[str] | None = None
) -> str:
    """Build a human-readable console report of scan results.

    Returns a formatted string ready to be printed to stdout.
    """
    stats = graph.metadata.scan_stats
    config = graph.metadata.scan_config
    lines: list[str] = []

    lines.append("")
    lines.append("=" * 60)
    lines.append("  SCAN REPORT")
    lines.append("=" * 60)

    # --- Scan config ---
    cfg_parts: list[str] = []
    if config.target:
        cfg_parts.append(f"target={config.target}")
    if config.datasets:
        cfg_parts.append(f"datasets={', '.join(config.datasets)}")
    if config.depth is not None:
        cfg_parts.append(f"depth={config.depth}")
    if cfg_parts:
        lines.append(f"  Config: {', '.join(cfg_parts)}")

    lines.append("")

    # --- Summary ---
    lines.append(f"  Nodes: {stats.total_nodes}")
    if stats.nodes_by_type:
        type_parts = [f"{v} {k}s" for k, v in sorted(stats.nodes_by_type.items())]
        lines.append(f"    ({', '.join(type_parts)})")
    lines.append(f"  Edges: {stats.total_edges}")
    lines.append("")

    # --- Graph structure ---
    has_structure_info = (
        stats.orphan_nodes or stats.terminal_nodes or stats.truncated_nodes
    )
    if has_structure_info:
        lines.append("  Structure:")
        if stats.orphan_nodes:
            lines.append(f"    Orphan nodes (no connections): {stats.orphan_nodes}")
        if stats.terminal_nodes:
            lines.append(f"    Terminal nodes (consumers):    {stats.terminal_nodes}")
        if stats.truncated_nodes:
            lines.append(f"    Truncated (depth limit):       {stats.truncated_nodes}")
        lines.append("")

    # --- Parse quality ---
    if stats.parse_errors:
        lines.append(f"  Parse errors: {stats.parse_errors}")
        lines.append("")

    # --- Node status breakdown ---
    status_counts: dict[str, int] = defaultdict(int)
    for node in graph.nodes.values():
        if node.status != "ok":
            status_counts[node.status] += 1
    if status_counts:
        lines.append("  Nodes with issues:")
        for status, count in sorted(status_counts.items()):
            lines.append(f"    {status}: {count}")
        lines.append("")

    # --- Errors ---
    err_list = errors or []
    if err_list:
        lines.append(f"  Errors ({len(err_list)}):")
        for err in err_list:
            lines.append(f"    - {err}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("")
    return "\n".join(lines)
