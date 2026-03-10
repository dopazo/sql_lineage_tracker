"""SQL lineage parser using sqlglot.

Parses SQL view definitions and extracts column-level lineage,
producing LineageEdge objects with ColumnMapping details.

Supports: SELECT, alias/rename, JOIN, CTE, expressions, aggregations.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage

from lineage_tracker.models import ColumnMapping, LineageEdge

logger = logging.getLogger(__name__)

# SQL aggregate function names
_AGG_FUNCTIONS = frozenset({
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "COUNT_IF", "COUNTIF", "APPROX_COUNT_DISTINCT",
    "ANY_VALUE", "ARRAY_AGG", "STRING_AGG",
    "STDDEV", "VARIANCE", "CORR",
})


def parse_view_lineage(
    view_id: str,
    sql: str,
    schemas: dict[str, dict[str, str]],
) -> list[LineageEdge]:
    """Parse a view's SQL and extract column-level lineage edges.

    Args:
        view_id: ID of the view being parsed (e.g. "staging.orders_clean").
        sql: The SQL definition of the view.
        schemas: Known schemas as {"dataset.table": {"col_name": "DATA_TYPE"}}.

    Returns:
        List of LineageEdge objects with column_mappings populated.
    """
    # Normalize SQL: strip project prefixes from fully-qualified table refs
    normalized_sql = _strip_project_prefix(sql)

    # Build schema in sqlglot nested format: {db: {table: {col: type}}}
    sg_schema = _build_sqlglot_schema(schemas)

    # Get output columns for this view
    output_cols = list(schemas.get(view_id, {}).keys())
    if not output_cols:
        logger.warning("No schema found for view %s, skipping lineage", view_id)
        return []

    # Get source tables from the SQL (for fallback when lineage() can't trace)
    from lineage_tracker.scanner import extract_table_references

    raw_source_tables = extract_table_references(normalized_sql)
    # Resolve to dataset.table format, filtering out CTE names
    cte_names = _get_cte_names(normalized_sql)
    source_tables = [
        f"{ds}.{tbl}" if ds else tbl
        for ds, tbl in raw_source_tables
        if tbl not in cte_names
    ]

    # Trace each output column and collect mappings grouped by source table
    # Key: source_node_id, Value: dict keyed by target_column
    edge_data: dict[str, dict[str, ColumnMapping]] = defaultdict(dict)

    for col in output_cols:
        try:
            result = lineage(
                col, normalized_sql, schema=sg_schema, dialect="bigquery"
            )
        except Exception:
            logger.warning("lineage() failed for %s.%s", view_id, col)
            _add_unknown_mapping(edge_data, source_tables, col)
            continue

        # Collect leaf nodes and all intermediate expressions in the chain
        leaves, chain_exprs = _collect_leaves_and_exprs(result)

        if not leaves:
            # No source traced (e.g., COUNT(*) or literal)
            _handle_no_source(edge_data, result, chain_exprs, source_tables, col)
            continue

        # Group source columns by source table for this target column
        sources_by_table: dict[str, list[str]] = defaultdict(list)
        for leaf in leaves:
            source_table_id = _extract_table_id(leaf.source)
            if not source_table_id:
                continue
            source_col = leaf.name.split(".")[-1]
            sources_by_table[source_table_id].append(source_col)

        for source_table_id, source_cols in sources_by_table.items():
            transformation, expr_str = _classify_transformation(
                chain_exprs, col, source_cols
            )
            edge_data[source_table_id][col] = ColumnMapping(
                source_columns=source_cols,
                target_column=col,
                transformation=transformation,
                expression=expr_str,
            )

    # Build LineageEdge objects
    edges: list[LineageEdge] = []
    for source_node, mappings_dict in edge_data.items():
        edge_id = f"edge_{source_node}__{view_id}"
        edges.append(
            LineageEdge(
                id=edge_id,
                source_node=source_node,
                target_node=view_id,
                edge_type="automatic",
                column_mappings=list(mappings_dict.values()),
            )
        )

    return edges


def _strip_project_prefix(sql: str) -> str:
    """Remove project (catalog) prefixes from table references in SQL.

    Converts `project.dataset.table` to `dataset.table` so that
    sqlglot can match against our schema dict.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return sql

    if parsed is None:
        return sql

    for table in parsed.find_all(exp.Table):
        if table.args.get("catalog"):
            table.set("catalog", None)

    return parsed.sql(dialect="bigquery")


def _build_sqlglot_schema(
    schemas: dict[str, dict[str, str]],
) -> dict[str, dict[str, dict[str, str]]]:
    """Convert flat schemas to sqlglot nested format.

    Input:  {"dataset.table": {"col": "TYPE"}}
    Output: {"dataset": {"table": {"col": "TYPE"}}}
    """
    nested: dict[str, dict[str, dict[str, str]]] = {}
    for table_id, columns in schemas.items():
        parts = table_id.split(".", 1)
        if len(parts) != 2:
            continue
        dataset, table = parts
        nested.setdefault(dataset, {})[table] = columns
    return nested


def _get_cte_names(sql: str) -> set[str]:
    """Extract CTE names from SQL to avoid treating them as real tables."""
    names: set[str] = set()
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return names

    if parsed is None:
        return names

    with_clause = parsed.find(exp.With)
    if with_clause:
        for cte in with_clause.expressions:
            if cte.alias:
                names.add(cte.alias)
    return names


def _collect_leaves_and_exprs(node) -> tuple[list, list[exp.Expression]]:
    """Recursively collect leaf nodes and all intermediate expressions.

    Returns:
        (leaves, chain_expressions) where chain_expressions includes
        all expressions encountered along the lineage path.
    """
    all_exprs: list[exp.Expression] = [node.expression]
    leaves: list = []

    if not node.downstream:
        if isinstance(node.source, exp.Table) and node.source.name:
            leaves.append(node)
        return leaves, all_exprs

    for child in node.downstream:
        child_leaves, child_exprs = _collect_leaves_and_exprs(child)
        all_exprs.extend(child_exprs)
        if child_leaves:
            leaves.extend(child_leaves)
        elif isinstance(child.source, exp.Table) and child.source.name:
            leaves.append(child)

    return leaves, all_exprs


def _extract_table_id(source: exp.Expression) -> str | None:
    """Extract 'dataset.table' from a Table expression."""
    if not isinstance(source, exp.Table):
        return None

    table_name = source.name
    db = source.db

    if not table_name:
        return None

    if db:
        return f"{db}.{table_name}"
    return table_name


def _classify_transformation(
    chain_exprs: list[exp.Expression],
    target_col: str,
    source_cols: list[str],
) -> tuple[str, str | None]:
    """Classify the transformation type from all expressions in the lineage chain.

    Checks all intermediate expressions (including CTE pass-throughs) to
    correctly detect aggregations and expressions that aren't visible at
    the top-level SELECT.

    Returns:
        (transformation_type, expression_string_or_none)
    """
    # Check the top-level expression first. If it's a non-trivial expression
    # (function, arithmetic, etc.), classify based on it directly.
    # If it's a simple column reference (CTE pass-through), look deeper in the chain.
    top_inner = chain_exprs[0].unalias() if chain_exprs else None

    if top_inner is not None and not isinstance(top_inner, (exp.Column, exp.Table)):
        # Top-level is a real expression — classify it directly
        if _contains_aggregate(top_inner):
            return "aggregation", top_inner.sql(dialect="bigquery")
        if _is_expression(top_inner):
            return "expression", top_inner.sql(dialect="bigquery")

    # Top-level is a simple column (CTE pass-through or direct ref).
    # Check intermediate expressions for aggregates/functions.
    for expr in chain_exprs[1:]:
        inner = expr.unalias() if hasattr(expr, "unalias") else expr
        if isinstance(inner, exp.Table):
            continue
        if _contains_aggregate(inner):
            return "aggregation", inner.sql(dialect="bigquery")
        if _is_expression(inner):
            return "expression", inner.sql(dialect="bigquery")

    # Simple column reference: direct or rename
    if len(source_cols) == 1 and source_cols[0] == target_col:
        return "direct", None
    if len(source_cols) == 1:
        return "rename", None

    return "expression", None


def _contains_aggregate(expr: exp.Expression) -> bool:
    """Check if an expression contains aggregate function calls."""
    if isinstance(expr, exp.AggFunc):
        return True

    func_name = None
    if isinstance(expr, exp.Anonymous):
        func_name = expr.name.upper()
    elif isinstance(expr, exp.Func):
        func_name = type(expr).__name__.upper()

    if func_name and func_name in _AGG_FUNCTIONS:
        return True

    # Check children
    for child in expr.iter_expressions():
        if _contains_aggregate(child):
            return True

    return False


def _is_expression(expr: exp.Expression) -> bool:
    """Check if an expression is more than a simple column reference."""
    if isinstance(expr, exp.Column):
        return False
    if isinstance(expr, (exp.Func, exp.Anonymous, exp.Binary)):
        return True
    return not isinstance(expr, (exp.Column, exp.Literal))


def _handle_no_source(
    edge_data: dict[str, dict[str, ColumnMapping]],
    result,
    chain_exprs: list[exp.Expression],
    source_tables: list[str],
    target_col: str,
) -> None:
    """Handle columns where lineage() found no downstream source.

    This happens for COUNT(*) and similar expressions that don't
    reference specific columns.
    """
    if not source_tables:
        return

    # Check all expressions in the chain for aggregates
    for expr in chain_exprs:
        inner = expr.unalias() if hasattr(expr, "unalias") else expr
        if isinstance(inner, exp.Table):
            continue
        if _contains_aggregate(inner):
            expr_str = inner.sql(dialect="bigquery")
            primary_table = source_tables[0]
            edge_data[primary_table][target_col] = ColumnMapping(
                source_columns=["*"],
                target_column=target_col,
                transformation="aggregation",
                expression=expr_str,
            )
            return

    _add_unknown_mapping(edge_data, source_tables, target_col)


def _add_unknown_mapping(
    edge_data: dict[str, dict[str, ColumnMapping]],
    source_tables: list[str],
    target_col: str,
) -> None:
    """Add an unknown mapping as fallback."""
    if not source_tables:
        return
    primary_table = source_tables[0]
    edge_data[primary_table][target_col] = ColumnMapping(
        source_columns=[],
        target_column=target_col,
        transformation="unknown",
    )
