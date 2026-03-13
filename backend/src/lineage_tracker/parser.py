"""SQL lineage parser using sqlglot.

Parses SQL view definitions and extracts column-level lineage,
producing LineageEdge objects with ColumnMapping details.

Supports: SELECT, alias/rename, JOIN, CTE, expressions, aggregations,
SELECT * with schema expansion, UNION ALL/UNION (columns by position),
subqueries (derived tables, scalar subqueries, WHERE IN/EXISTS, nested),
window functions (ROW_NUMBER, RANK, SUM OVER, LAG/LEAD, etc.),
UNNEST (array flattening), STRUCT (field access and creation),
dynamic SQL detection (EXECUTE IMMEDIATE — flagged as warning).
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.schema import MappingSchema

from lineage_tracker.models import ColumnMapping, LineageEdge

logger = logging.getLogger(__name__)

# Regex to detect EXECUTE IMMEDIATE (BigQuery dynamic SQL)
_DYNAMIC_SQL_RE = re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE)


def contains_dynamic_sql(sql: str) -> bool:
    """Check if SQL contains dynamic SQL constructs (EXECUTE IMMEDIATE).

    Dynamic SQL builds and executes queries at runtime from string
    expressions, making static lineage analysis impossible. These
    should be flagged as warnings so users can resolve lineage manually.
    """
    return bool(_DYNAMIC_SQL_RE.search(sql))


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

    # Assign aliases to anonymous derived tables (subqueries in FROM/JOIN)
    # so that sqlglot can resolve columns through nested subqueries
    normalized_sql = _normalize_derived_tables(normalized_sql)

    # Build schema in sqlglot nested format: {db: {table: {col: type}}}
    sg_schema = _build_sqlglot_schema(schemas)

    # Expand SELECT * using schema before tracing lineage
    normalized_sql = _expand_star(normalized_sql, sg_schema)

    # Handle UNION / UNION ALL: decompose into branches and parse each
    union_branches = _extract_union_branches(normalized_sql)
    if union_branches is not None:
        return _handle_union_lineage(view_id, union_branches, normalized_sql, schemas)

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
        leaves, chain_exprs, has_unnest = _collect_leaves_and_exprs(result)

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
            # Improve expression strings for UNNEST paths
            if has_unnest:
                expr_str = _build_unnest_expression(
                    transformation, expr_str, source_cols, col
                )
            # Clean internal sqlglot aliases from expression strings
            if expr_str:
                expr_str = _clean_expression(expr_str)
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


def _expand_star(
    sql: str,
    sg_schema: dict[str, dict[str, dict[str, str]]],
) -> str:
    """Expand SELECT * into explicit column references using known schemas.

    Uses sqlglot's qualify_columns optimizer pass which resolves
    `SELECT *` and `SELECT t.*` into individual column references
    based on the provided schema.

    Before qualifying, assigns synthetic aliases to anonymous derived
    tables (subqueries in FROM/JOIN without aliases) so that
    qualify_columns can resolve columns through nested subqueries.

    Returns the original SQL unchanged if expansion fails or if
    there are no star expressions.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return sql

    if parsed is None:
        return sql

    # Quick check: skip if no star expressions exist
    if not list(parsed.find_all(exp.Star)):
        return sql

    # Assign aliases to anonymous derived tables so qualify_columns
    # can resolve columns through nested subqueries
    _assign_derived_table_aliases(parsed)

    try:
        schema_obj = MappingSchema(schema=sg_schema, dialect="bigquery")
        qualified = qualify_columns(parsed, schema=schema_obj, dialect="bigquery")
        return qualified.sql(dialect="bigquery")
    except Exception:
        logger.debug("Failed to expand SELECT * for SQL: %s...", sql[:80])
        return sql


def _normalize_derived_tables(sql: str) -> str:
    """Assign synthetic aliases to anonymous derived tables in the SQL string.

    Parses the SQL, assigns aliases to unnamed subqueries in FROM/JOIN,
    and returns the modified SQL. Returns the original SQL if parsing fails.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return sql

    if parsed is None:
        return sql

    _assign_derived_table_aliases(parsed)
    return parsed.sql(dialect="bigquery")


def _assign_derived_table_aliases(parsed: exp.Expression) -> None:
    """Assign synthetic aliases to anonymous derived tables (subqueries in FROM/JOIN).

    sqlglot's qualify_columns needs named sources to resolve column references.
    Unnamed subqueries in FROM or JOIN clauses cause qualify_columns to produce
    empty-identifier qualifiers (e.g. ``.col). This function walks the AST and
    assigns names like _subq_0, _subq_1, etc. to any Subquery that lacks an alias.

    Modifies the AST in place.
    """
    counter = 0
    for node in parsed.find_all(exp.Subquery):
        if isinstance(node.parent, (exp.From, exp.Join)) and not node.alias:
            node.set("alias", exp.TableAlias(this=exp.to_identifier(f"_subq_{counter}")))
            counter += 1


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


def _collect_leaves_and_exprs(node) -> tuple[list, list[exp.Expression], bool]:
    """Recursively collect leaf nodes, intermediate expressions, and UNNEST info.

    Returns:
        (leaves, chain_expressions, has_unnest) where chain_expressions includes
        all expressions encountered along the lineage path, and has_unnest
        indicates if the path traverses an UNNEST operation.
    """
    all_exprs: list[exp.Expression] = [node.expression]
    leaves: list = []
    has_unnest = isinstance(node.source, exp.Unnest)

    if not node.downstream:
        if isinstance(node.source, exp.Table) and node.source.name:
            leaves.append(node)
        return leaves, all_exprs, has_unnest

    for child in node.downstream:
        child_leaves, child_exprs, child_unnest = _collect_leaves_and_exprs(child)
        all_exprs.extend(child_exprs)
        has_unnest = has_unnest or child_unnest
        if child_leaves:
            leaves.extend(child_leaves)
        elif isinstance(child.source, exp.Table) and child.source.name:
            leaves.append(child)

    return leaves, all_exprs, has_unnest


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
        # Window functions (OVER) take priority over aggregation detection
        # because SUM(x) OVER (...) is a window function, not an aggregation.
        if _contains_window(top_inner):
            return "expression", top_inner.sql(dialect="bigquery")
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
        if _contains_window(inner):
            return "expression", inner.sql(dialect="bigquery")
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


def _contains_window(expr: exp.Expression) -> bool:
    """Check if an expression contains a window function (OVER clause)."""
    if isinstance(expr, exp.Window):
        return True
    for child in expr.iter_expressions():
        if _contains_window(child):
            return True
    return False


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


def _build_unnest_expression(
    transformation: str,
    expr_str: str | None,
    source_cols: list[str],
    target_col: str,
) -> str | None:
    """Build a descriptive expression string for UNNEST lineage paths.

    When the lineage traverses UNNEST, the raw expression from sqlglot
    is often just the alias name (e.g., "tag"). This replaces it with
    a more informative expression like "UNNEST(tags)".

    For aggregations/expressions that already contain meaningful function
    calls (e.g., "COUNT(tag)"), the string is returned as-is (internal
    alias cleanup is handled separately by _clean_expression).
    """
    if transformation in ("direct", "rename") or expr_str is None:
        return expr_str

    # If the expression is just the alias/column name (trivial), replace with UNNEST()
    stripped = expr_str.strip()
    is_trivial = (
        stripped == target_col
        or stripped.isidentifier()
        and "(" not in stripped
        and "." not in stripped
    )
    if is_trivial and len(source_cols) == 1:
        return f"UNNEST({source_cols[0]})"

    return expr_str


# Pattern for internal sqlglot-generated aliases like _0., _1., _t0., etc.
_INTERNAL_ALIAS_RE = re.compile(r"\b_\d+\.")


def _clean_expression(expr_str: str) -> str:
    """Remove internal sqlglot-generated aliases from expression strings.

    sqlglot introduces aliases like _0, _t0 for UNNEST and subquery
    rewrites. These are not meaningful to the user.

    Examples:
        "COUNT(_0.tag)"  -> "COUNT(tag)"
        "_0.e.name"      -> "e.name"
    """
    return _INTERNAL_ALIAS_RE.sub("", expr_str)


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

    # Check all expressions in the chain for window functions or aggregates
    for expr in chain_exprs:
        inner = expr.unalias() if hasattr(expr, "unalias") else expr
        if isinstance(inner, exp.Table):
            continue
        # Window functions (e.g. ROW_NUMBER() OVER) take priority
        if _contains_window(inner):
            expr_str = inner.sql(dialect="bigquery")
            primary_table = source_tables[0]
            edge_data[primary_table][target_col] = ColumnMapping(
                source_columns=["*"],
                target_column=target_col,
                transformation="expression",
                expression=expr_str,
            )
            return
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


def _extract_union_branches(sql: str) -> list[exp.Select] | None:
    """Detect UNION/UNION ALL and return individual SELECT branches.

    Returns None if the top-level statement is not a UNION.
    Returns a list of Select expressions (one per branch) if it is.
    Handles CTEs by preserving them on the first branch.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return None

    if parsed is None:
        return None

    # Unwrap CREATE TABLE AS SELECT if present
    stmt = parsed
    if isinstance(stmt, (exp.Create,)):
        inner = stmt.find(exp.Union) or stmt.find(exp.Select)
        if inner is not None:
            stmt = inner

    if not isinstance(stmt, exp.Union):
        return None

    # Collect all branches by traversing the Union tree
    branches: list[exp.Select] = []
    _collect_union_branches(stmt, branches)
    return branches if len(branches) >= 2 else None


def _collect_union_branches(node: exp.Expression, branches: list[exp.Select]) -> None:
    """Recursively collect SELECT branches from a Union tree.

    UNION chains are left-associative: (A UNION B) UNION C
    The `this` side may be another Union, while `expression` is always a Select.
    """
    if isinstance(node, exp.Union):
        _collect_union_branches(node.this, branches)
        _collect_union_branches(node.expression, branches)
    elif isinstance(node, exp.Select):
        branches.append(node)


def _handle_union_lineage(
    view_id: str,
    branches: list[exp.Select],
    original_sql: str,
    schemas: dict[str, dict[str, str]],
) -> list[LineageEdge]:
    """Parse lineage for a UNION query by processing each branch independently.

    Each branch is converted to standalone SQL and parsed via parse_view_lineage.
    Output columns are mapped by position: branch column N -> output column N.
    """
    output_cols = list(schemas.get(view_id, {}).keys())
    if not output_cols:
        logger.warning("No schema found for view %s, skipping UNION lineage", view_id)
        return []

    # Extract CTE definitions from the original SQL to prepend to each branch
    cte_prefix = _extract_cte_prefix(original_sql)

    # Accumulate edge data across all branches
    merged_edge_data: dict[str, dict[str, ColumnMapping]] = defaultdict(dict)

    for branch in branches:
        branch_sql_raw = branch.sql(dialect="bigquery")
        branch_sql = f"{cte_prefix}\n{branch_sql_raw}" if cte_prefix else branch_sql_raw

        # Determine output column names for this branch by position
        branch_col_names = _get_branch_output_columns(branch, schemas)

        # Build a synthetic schema for this branch mapping branch cols -> output cols
        branch_view_schema: dict[str, str] = {}
        view_schema = schemas.get(view_id, {})
        for i, out_col in enumerate(output_cols):
            if i < len(branch_col_names):
                branch_view_schema[branch_col_names[i]] = view_schema.get(out_col, "STRING")

        # Parse the branch as a standalone query
        branch_schemas = dict(schemas)
        branch_view_id = f"__union_branch_{id(branch)}"
        branch_schemas[branch_view_id] = branch_view_schema

        branch_edges = parse_view_lineage(branch_view_id, branch_sql, branch_schemas)

        # Remap branch column names back to output column names (by position)
        pos_map = {}
        for i, bcol in enumerate(branch_col_names):
            if i < len(output_cols):
                pos_map[bcol] = output_cols[i]

        for edge in branch_edges:
            for mapping in edge.column_mappings:
                remapped_target = pos_map.get(mapping.target_column, mapping.target_column)
                mapping.target_column = remapped_target
                # If the target column was renamed by position mapping,
                # a "direct" may now be a "rename" and vice versa
                if mapping.transformation in ("direct", "rename"):
                    if len(mapping.source_columns) == 1 and mapping.source_columns[0] == remapped_target:
                        mapping.transformation = "direct"
                    elif len(mapping.source_columns) == 1:
                        mapping.transformation = "rename"

            # Merge into accumulated edge data
            for mapping in edge.column_mappings:
                merged_edge_data[edge.source_node][mapping.target_column] = mapping

    # Build final LineageEdge objects
    edges: list[LineageEdge] = []
    for source_node, mappings_dict in merged_edge_data.items():
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


def _get_branch_output_columns(
    branch: exp.Select,
    schemas: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    """Extract output column names/aliases from a SELECT branch.

    If the branch contains SELECT * and schemas are provided,
    expands the star using source table schemas.
    """
    has_star = any(isinstance(e, exp.Star) for e in branch.expressions)

    if has_star and schemas:
        # Try to expand star using qualify_columns
        sg_schema = _build_sqlglot_schema(schemas)
        branch_sql = branch.sql(dialect="bigquery")
        expanded_sql = _expand_star(branch_sql, sg_schema)
        if expanded_sql != branch_sql:
            try:
                expanded = sqlglot.parse(expanded_sql, dialect="bigquery")[0]
                if isinstance(expanded, exp.Select):
                    return _get_branch_output_columns(expanded, None)
            except Exception:
                pass

    cols: list[str] = []
    for expr in branch.expressions:
        if isinstance(expr, exp.Alias):
            cols.append(expr.alias)
        elif isinstance(expr, exp.Column):
            cols.append(expr.name)
        elif isinstance(expr, exp.Star):
            # Couldn't expand — try to infer from source tables in the branch
            if schemas:
                for table in branch.find_all(exp.Table):
                    table_id = _extract_table_id(table)
                    if table_id and table_id in schemas:
                        cols.extend(schemas[table_id].keys())
            if not cols:
                cols.append("*")
        else:
            cols.append(expr.sql(dialect="bigquery"))
    return cols


def _extract_cte_prefix(sql: str) -> str:
    """Extract the WITH ... AS (...) prefix from SQL if present.

    Returns the CTE clause as a string, or empty string if none.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return ""

    if parsed is None:
        return ""

    # For UNION inside CREATE, unwrap first
    stmt = parsed
    if isinstance(stmt, exp.Create):
        stmt = stmt.find(exp.Union) or stmt.find(exp.Select)
        if stmt is None:
            return ""

    with_clause = stmt.find(exp.With)
    if with_clause is None:
        return ""

    return with_clause.sql(dialect="bigquery")
