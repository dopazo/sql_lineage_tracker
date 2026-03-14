"""Persistence module for saving/loading lineage graphs to/from disk."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path

from lineage_tracker.models import (
    ColumnInfo,
    ColumnMapping,
    GraphMetadata,
    LineageEdge,
    LineageGraph,
    LineageNode,
    ScanConfig,
    ScanStats,
)

logger = logging.getLogger(__name__)

GRAPH_FILENAME = "graph_data.json"


def get_project_dir(data_dir: Path, project_id: str) -> Path:
    """Get the project-specific directory for persistence."""
    return data_dir / project_id


def get_graph_path(data_dir: Path, project_id: str) -> Path:
    """Get the full path to the graph JSON file."""
    return get_project_dir(data_dir, project_id) / GRAPH_FILENAME


def save_graph(graph: LineageGraph, data_dir: Path, project_id: str) -> Path:
    """Save a LineageGraph to disk as JSON.

    Creates the project directory if it doesn't exist.

    Returns:
        Path to the saved file.
    """
    project_dir = get_project_dir(data_dir, project_id)
    project_dir.mkdir(parents=True, exist_ok=True)

    file_path = project_dir / GRAPH_FILENAME
    data = graph_to_dict(graph)

    file_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Graph saved to %s", file_path)

    return file_path


def load_graph(data_dir: Path, project_id: str) -> LineageGraph | None:
    """Load a LineageGraph from disk.

    Returns:
        The loaded graph, or None if no saved graph exists.
    """
    file_path = get_graph_path(data_dir, project_id)

    if not file_path.exists():
        logger.info("No saved graph found at %s", file_path)
        return None

    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        graph = dict_to_graph(data)
        logger.info(
            "Graph loaded from %s (%d nodes, %d edges)",
            file_path,
            len(graph.nodes),
            len(graph.edges),
        )
        return graph
    except Exception:
        logger.exception("Failed to load graph from %s", file_path)
        return None


def graph_to_dict(graph: LineageGraph) -> dict:
    """Convert a LineageGraph to a JSON-serializable dict."""
    return {
        "metadata": _metadata_to_dict(graph.metadata),
        "nodes": {
            node_id: _node_to_dict(node)
            for node_id, node in graph.nodes.items()
        },
        "edges": [_edge_to_dict(edge) for edge in graph.edges],
    }


def dict_to_graph(data: dict) -> LineageGraph:
    """Reconstruct a LineageGraph from a dict (loaded from JSON)."""
    metadata = _dict_to_metadata(data["metadata"])

    nodes: dict[str, LineageNode] = {}
    for node_id, node_data in data.get("nodes", {}).items():
        nodes[node_id] = _dict_to_node(node_data, node_id)

    edges = [_dict_to_edge(e) for e in data.get("edges", [])]

    return LineageGraph(metadata=metadata, nodes=nodes, edges=edges)


# -- Serialization helpers --

def _metadata_to_dict(meta: GraphMetadata) -> dict:
    return {
        "project_id": meta.project_id,
        "generated_at": meta.generated_at,
        "description": meta.description,
        "scan_config": {
            "target": meta.scan_config.target,
            "datasets": meta.scan_config.datasets,
            "depth": meta.scan_config.depth,
        },
        "scan_stats": asdict(meta.scan_stats),
    }


def _node_to_dict(node: LineageNode) -> dict:
    return {
        "type": node.type,
        "dataset": node.dataset,
        "name": node.name,
        "columns": [
            {"name": c.name, "data_type": c.data_type, "lineage_status": c.lineage_status}
            for c in node.columns
        ],
        "source": node.source,
        "sql": node.sql,
        "description": node.description,
        "status": node.status,
        "status_message": node.status_message,
    }


def _edge_to_dict(edge: LineageEdge) -> dict:
    return {
        "id": edge.id,
        "source_node": edge.source_node,
        "target_node": edge.target_node,
        "edge_type": edge.edge_type,
        "description": edge.description,
        "column_mappings": [
            {
                "source_columns": m.source_columns,
                "target_column": m.target_column,
                "transformation": m.transformation,
                "expression": m.expression,
                "description": m.description,
            }
            for m in edge.column_mappings
        ],
    }


# -- Deserialization helpers --

def _dict_to_metadata(data: dict) -> GraphMetadata:
    sc = data.get("scan_config", {})
    ss = data.get("scan_stats", {})
    return GraphMetadata(
        project_id=data["project_id"],
        generated_at=data["generated_at"],
        description=data.get("description"),
        scan_config=ScanConfig(
            target=sc.get("target"),
            datasets=sc.get("datasets", []),
            depth=sc.get("depth"),
        ),
        scan_stats=ScanStats(
            total_nodes=ss.get("total_nodes", 0),
            total_edges=ss.get("total_edges", 0),
            nodes_by_type=ss.get("nodes_by_type", {}),
            orphan_nodes=ss.get("orphan_nodes", 0),
            terminal_nodes=ss.get("terminal_nodes", 0),
            truncated_nodes=ss.get("truncated_nodes", 0),
            parse_errors=ss.get("parse_errors", 0),
        ),
    )


def _dict_to_node(data: dict, node_id: str) -> LineageNode:
    columns = [
        ColumnInfo(
            name=c["name"],
            data_type=c["data_type"],
            lineage_status=c.get("lineage_status", "resolved"),
        )
        for c in data.get("columns", [])
    ]
    return LineageNode(
        id=node_id,
        type=data["type"],
        dataset=data["dataset"],
        name=data["name"],
        columns=columns,
        source=data.get("source", "unknown"),
        sql=data.get("sql"),
        description=data.get("description"),
        status=data.get("status", "ok"),
        status_message=data.get("status_message"),
    )


SCANS_DIRNAME = "scans"


def get_scans_dir(data_dir: Path, project_id: str) -> Path:
    """Get the directory for named scans."""
    return get_project_dir(data_dir, project_id) / SCANS_DIRNAME


def list_named_scans(data_dir: Path, project_id: str) -> list[dict]:
    """List all named scans for a project.

    Returns a list of dicts with 'name' and basic metadata.
    """
    scans_dir = get_scans_dir(data_dir, project_id)
    if not scans_dir.exists():
        return []

    results = []
    for f in sorted(scans_dir.glob("*.json")):
        name = f.stem
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            meta = data.get("metadata", {})
            sc = meta.get("scan_config", {})
            ss = meta.get("scan_stats", {})
            results.append({
                "name": name,
                "target": sc.get("target"),
                "datasets": sc.get("datasets", []),
                "depth": sc.get("depth"),
                "total_nodes": ss.get("total_nodes", 0),
                "total_edges": ss.get("total_edges", 0),
                "generated_at": meta.get("generated_at"),
            })
        except Exception:
            logger.warning("Failed to read scan metadata from %s", f)
            results.append({"name": name})

    return results


def save_named_scan(graph: LineageGraph, data_dir: Path, project_id: str, name: str) -> Path:
    """Save the current graph as a named scan."""
    scans_dir = get_scans_dir(data_dir, project_id)
    scans_dir.mkdir(parents=True, exist_ok=True)

    file_path = scans_dir / f"{name}.json"
    data = graph_to_dict(graph)
    file_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Named scan '%s' saved to %s", name, file_path)
    return file_path


def load_named_scan(data_dir: Path, project_id: str, name: str) -> LineageGraph | None:
    """Load a named scan from disk."""
    file_path = get_scans_dir(data_dir, project_id) / f"{name}.json"
    if not file_path.exists():
        return None

    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        graph = dict_to_graph(data)
        logger.info("Named scan '%s' loaded (%d nodes, %d edges)", name, len(graph.nodes), len(graph.edges))
        return graph
    except Exception:
        logger.exception("Failed to load named scan '%s'", name)
        return None


def named_scan_exists(data_dir: Path, project_id: str, name: str) -> bool:
    """Check if a named scan already exists."""
    return (get_scans_dir(data_dir, project_id) / f"{name}.json").exists()


def delete_named_scan(data_dir: Path, project_id: str, name: str) -> bool:
    """Delete a named scan. Returns True if deleted, False if not found."""
    file_path = get_scans_dir(data_dir, project_id) / f"{name}.json"
    if file_path.exists():
        file_path.unlink()
        logger.info("Named scan '%s' deleted", name)
        return True
    return False


def _dict_to_edge(data: dict) -> LineageEdge:
    mappings = [
        ColumnMapping(
            source_columns=m["source_columns"],
            target_column=m["target_column"],
            transformation=m.get("transformation", "unknown"),
            expression=m.get("expression"),
            description=m.get("description"),
        )
        for m in data.get("column_mappings", [])
    ]
    return LineageEdge(
        id=data["id"],
        source_node=data["source_node"],
        target_node=data["target_node"],
        edge_type=data.get("edge_type", "automatic"),
        description=data.get("description"),
        column_mappings=mappings,
    )
