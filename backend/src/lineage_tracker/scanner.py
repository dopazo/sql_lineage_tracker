"""Scoped scanning module.

Follows dependencies from a target table/view backward through the
transformation chain, respecting dataset filters and depth limits.

Depth is measured in **dataset hops**: each time a dependency crosses
to a different dataset, it counts as +1. Dependencies within the same
dataset do NOT count as a hop.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field

import sqlglot

from lineage_tracker.extractor import BigQueryExtractor
from lineage_tracker.models import ColumnInfo, LineageNode, ProgressCallback, ScanConfig, _NOOP_PROGRESS

logger = logging.getLogger(__name__)


@dataclass
class _NodeRef:
    """Internal reference to a table/view discovered during scanning."""

    dataset: str
    name: str
    dataset_hops: int  # number of dataset boundary crossings from the target

    @property
    def node_id(self) -> str:
        return f"{self.dataset}.{self.name}"


@dataclass
class ScanResult:
    """Result of a scoped scan."""

    nodes: dict[str, LineageNode] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def extract_table_references(sql: str) -> list[tuple[str | None, str]]:
    """Parse SQL and extract referenced table/view names.

    Returns a list of (dataset, table_name) tuples. Dataset may be None
    if the reference is unqualified.
    """
    refs: list[tuple[str | None, str]] = []
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")
    except sqlglot.errors.ParseError:
        logger.warning("Failed to parse SQL for table references")
        return refs
    except Exception:
        logger.warning("Unexpected error parsing SQL for table references")
        return refs

    for statement in parsed:
        if statement is None:
            continue

        # Collect CTE names to avoid treating them as real table references
        cte_names: set[str] = set()
        with_clause = statement.find(sqlglot.exp.With)
        if with_clause:
            for cte in with_clause.expressions:
                if cte.alias:
                    cte_names.add(cte.alias)

        for table in statement.find_all(sqlglot.exp.Table):
            table_name = table.name
            if not table_name:
                continue

            # Skip CTE references — they are not real tables
            if table_name in cte_names:
                continue

            # Extract dataset (db in sqlglot terms)
            db = table.db
            # Strip backticks and project prefix if present
            # BigQuery refs can be `project.dataset.table` or `dataset.table` or just `table`
            catalog = table.catalog

            if catalog and db:
                # Full reference: project.dataset.table -> use dataset part
                refs.append((db, table_name))
            elif db:
                # dataset.table
                refs.append((db, table_name))
            else:
                # Unqualified table name
                refs.append((None, table_name))

    return refs


def _resolve_dataset(
    ref_dataset: str | None,
    current_dataset: str,
    project_id: str,
) -> str:
    """Resolve a possibly-None dataset reference to an actual dataset name.

    If the reference has no dataset qualifier, assume it's in the same
    dataset as the referencing view.
    """
    if ref_dataset is None:
        return current_dataset

    # Strip project prefix if it matches (e.g. "my-project.staging" -> "staging")
    if "." in ref_dataset:
        parts = ref_dataset.split(".")
        if parts[0] == project_id:
            return parts[-1]

    return ref_dataset


def run_scoped_scan(
    extractor: BigQueryExtractor,
    config: ScanConfig,
    progress: ProgressCallback = _NOOP_PROGRESS,
) -> ScanResult:
    """Run a scoped scan following dependencies from a target.

    If config.target is set, starts from that target and follows
    dependencies recursively. If only config.datasets is set, extracts
    all nodes from those datasets and follows external dependencies.

    Args:
        extractor: BigQuery extractor instance.
        config: Scan configuration with target, datasets, and depth.

    Returns:
        ScanResult with discovered nodes and any errors.
    """
    result = ScanResult()

    progress("scan_start", f"Starting scan (target={config.target}, datasets={config.datasets}, depth={config.depth})")

    if config.target:
        _scan_from_target(extractor, config, result, progress)
    elif config.datasets:
        _scan_datasets(extractor, config, result, progress)
    else:
        # Full project scan: get all datasets, then scan each
        progress("scan_dataset", "Listing all datasets in project...")
        datasets = extractor.list_datasets()
        full_config = ScanConfig(
            target=None,
            datasets=[ds.id for ds in datasets],
            depth=config.depth,
        )
        _scan_datasets(extractor, full_config, result, progress)

    progress("scan_complete", f"Scan finished: {len(result.nodes)} nodes discovered, {len(result.errors)} errors")
    return result


def _scan_from_target(
    extractor: BigQueryExtractor,
    config: ScanConfig,
    result: ScanResult,
    report: ProgressCallback = _NOOP_PROGRESS,
) -> None:
    """Scan backward from a target table/view."""
    assert config.target is not None

    parts = config.target.split(".")
    if len(parts) != 2:
        result.errors.append(
            f"Invalid target format '{config.target}': expected 'dataset.table'"
        )
        return

    target_dataset, target_name = parts
    report("scan_node", f"Starting backward trace from {config.target}")

    # Queue: nodes to visit. Key is node_id, value is _NodeRef.
    queue: deque[_NodeRef] = deque([_NodeRef(dataset=target_dataset, name=target_name, dataset_hops=0)])
    visited: set[str] = set()

    while queue:
        ref = queue.popleft()
        node_id = ref.node_id

        if node_id in visited:
            continue
        visited.add(node_id)

        # Check dataset filter
        if config.datasets and ref.dataset not in config.datasets:
            # Dataset not in allowed list — add as truncated node
            _add_truncated_node(extractor, ref, result, "Dataset not in scan scope")
            continue

        # Fetch node metadata
        report("scan_node", f"Fetching metadata for {node_id}")
        node = _fetch_node(extractor, ref, result)
        if node is None:
            continue

        # Check depth limit for following further dependencies
        if node.sql:
            # Parse SQL to find dependencies
            deps = extract_table_references(node.sql)
            for dep_dataset, dep_name in deps:
                resolved_dataset = _resolve_dataset(
                    dep_dataset, ref.dataset, extractor.project_id
                )
                dep_id = f"{resolved_dataset}.{dep_name}"

                if dep_id in visited:
                    continue

                # Calculate dataset hops
                dep_hops = ref.dataset_hops
                if resolved_dataset != ref.dataset:
                    dep_hops += 1

                # Check depth limit
                if config.depth is not None and dep_hops > config.depth:
                    _add_truncated_node(
                        extractor,
                        _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=dep_hops),
                        result,
                        f"Depth limit reached ({dep_hops} dataset hops > {config.depth})",
                    )
                    continue

                queue.append(
                    _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=dep_hops)
                )


def _scan_datasets(
    extractor: BigQueryExtractor,
    config: ScanConfig,
    result: ScanResult,
    report: ProgressCallback = _NOOP_PROGRESS,
) -> None:
    """Scan all specified datasets and follow external dependencies."""
    total_datasets = len(config.datasets)
    for idx, dataset_id in enumerate(config.datasets, 1):
        logger.info("Scanning dataset: %s", dataset_id)
        report("scan_dataset", f"Scanning dataset {dataset_id} ({idx}/{total_datasets})")
        try:
            nodes = extractor.extract_dataset(dataset_id)
        except Exception:
            msg = f"Failed to extract dataset {dataset_id}"
            logger.exception(msg)
            result.errors.append(msg)
            continue

        for node in nodes:
            result.nodes[node.id] = node

    # Now follow dependencies outside the specified datasets
    external_queue: deque[_NodeRef] = deque()

    for node in list(result.nodes.values()):
        if not node.sql:
            continue
        deps = extract_table_references(node.sql)
        for dep_dataset, dep_name in deps:
            resolved_dataset = _resolve_dataset(
                dep_dataset, node.dataset, extractor.project_id
            )
            dep_id = f"{resolved_dataset}.{dep_name}"

            if dep_id in result.nodes:
                continue

            # External dependency — calculate hops
            hops = 0 if resolved_dataset in config.datasets else 1

            if config.depth is not None and hops > config.depth:
                _add_truncated_node(
                    extractor,
                    _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=hops),
                    result,
                    f"Depth limit reached ({hops} dataset hops > {config.depth})",
                )
                continue

            external_queue.append(
                _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=hops)
            )

    # BFS for external dependencies
    visited: set[str] = set(result.nodes.keys())
    while external_queue:
        ref = external_queue.popleft()
        node_id = ref.node_id

        if node_id in visited:
            continue
        visited.add(node_id)

        node = _fetch_node(extractor, ref, result)
        if node is None:
            continue

        # Follow further dependencies from external nodes
        if node.sql:
            deps = extract_table_references(node.sql)
            for dep_dataset, dep_name in deps:
                resolved_dataset = _resolve_dataset(
                    dep_dataset, ref.dataset, extractor.project_id
                )
                dep_id = f"{resolved_dataset}.{dep_name}"

                if dep_id in visited:
                    continue

                dep_hops = ref.dataset_hops
                if resolved_dataset != ref.dataset:
                    dep_hops += 1

                if config.depth is not None and dep_hops > config.depth:
                    _add_truncated_node(
                        extractor,
                        _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=dep_hops),
                        result,
                        f"Depth limit reached ({dep_hops} dataset hops > {config.depth})",
                    )
                    continue

                external_queue.append(
                    _NodeRef(dataset=resolved_dataset, name=dep_name, dataset_hops=dep_hops)
                )


def _fetch_node(
    extractor: BigQueryExtractor,
    ref: _NodeRef,
    result: ScanResult,
) -> LineageNode | None:
    """Fetch a node's metadata from BigQuery and add to result."""
    node_id = ref.node_id

    # Get table type
    try:
        table_type = extractor.get_table_type(ref.dataset, ref.name)
    except Exception:
        msg = f"Failed to get type for {node_id}"
        logger.warning(msg)
        result.errors.append(msg)
        result.nodes[node_id] = LineageNode(
            id=node_id,
            type="table",
            dataset=ref.dataset,
            name=ref.name,
            source="unknown",
            status="error",
            status_message=f"Failed to access {node_id}",
        )
        return result.nodes[node_id]

    if table_type is None:
        # Table doesn't exist or no permissions
        result.nodes[node_id] = LineageNode(
            id=node_id,
            type="table",
            dataset=ref.dataset,
            name=ref.name,
            source="unknown",
            status="error",
            status_message=f"Table {node_id} not found",
        )
        result.errors.append(f"Table {node_id} not found")
        return result.nodes[node_id]

    # Get SQL if it's a view
    sql: str | None = None
    source = "ingestion"
    if table_type == "view":
        sql = extractor.get_view_sql(ref.dataset, ref.name)
        source = "bigquery_view"
        if sql is None:
            source = "unknown"

    # Get columns
    columns_map = extractor.get_columns(ref.dataset, ref.name)
    columns = columns_map.get(ref.name, [])

    node = LineageNode(
        id=node_id,
        type=table_type,
        dataset=ref.dataset,
        name=ref.name,
        columns=columns,
        source=source,
        sql=sql,
    )

    result.nodes[node_id] = node
    return node


def _add_truncated_node(
    extractor: BigQueryExtractor,
    ref: _NodeRef,
    result: ScanResult,
    message: str,
) -> None:
    """Add a node marked as truncated (not fully explored)."""
    node_id = ref.node_id
    if node_id in result.nodes:
        return

    result.nodes[node_id] = LineageNode(
        id=node_id,
        type="table",  # Unknown — could be table or view
        dataset=ref.dataset,
        name=ref.name,
        source="unknown",
        status="truncated",
        status_message=message,
    )
