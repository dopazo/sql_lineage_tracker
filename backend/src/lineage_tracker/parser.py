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
from sqlglot.errors import OptimizeError
from sqlglot.lineage import lineage
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.schema import MappingSchema

from lineage_tracker.models import ColumnMapping, LineageEdge

logger = logging.getLogger(__name__)

# Regex to detect EXECUTE IMMEDIATE (BigQuery dynamic SQL)
_DYNAMIC_SQL_RE = re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE)

# Regex to extract column name from OptimizeError "Unknown column: xxx" messages
_UNKNOWN_COL_RE = re.compile(r"Unknown column[:\s]+['\"]?(\w+)['\"]?", re.IGNORECASE)

# Max retries when patching schema for unknown columns
_MAX_SCHEMA_PATCH_RETRIES = 20


def contains_dynamic_sql(sql: str) -> bool:
    """Check if SQL contains dynamic SQL constructs (EXECUTE IMMEDIATE).

    Dynamic SQL builds and executes queries at runtime from string
    expressions, making static lineage analysis impossible. These
    should be flagged as warnings so users can resolve lineage manually.
    """
    return bool(_DYNAMIC_SQL_RE.search(sql))


def _extract_unknown_column(error_msg: str) -> str | None:
    """Extract column name from an OptimizeError 'Unknown column' message."""
    match = _UNKNOWN_COL_RE.search(error_msg)
    return match.group(1) if match else None


def _add_missing_column_to_schema(
    col_name: str,
    sql: str,
    sg_schema: dict[str, dict[str, dict[str, str]]],
) -> bool:
    """Add a missing column to the correct table in the schema.

    Parses the SQL to find qualified column references (e.g., CUAD.q_sg_lider),
    resolves the table alias to the actual dataset.table, and adds the column
    with type STRING as a placeholder.

    Mutates sg_schema in place. Returns True if the column was added.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return False

    if not parsed:
        return False

    # Build alias -> (db, table_name) map from all table references
    alias_map: dict[str, tuple[str, str]] = {}
    for table in parsed.find_all(exp.Table):
        tbl_name = table.name
        db = table.db
        alias = table.alias or tbl_name
        if db and tbl_name:
            alias_map[alias.lower()] = (db, tbl_name)

    col_lower = col_name.lower()

    # Find qualified column references (e.g., cuad.q_sg_lider)
    for col in parsed.find_all(exp.Column):
        if col.name.lower() == col_lower and col.table:
            alias_key = col.table.lower()
            if alias_key in alias_map:
                db, tbl = alias_map[alias_key]
                if db in sg_schema and tbl in sg_schema[db]:
                    sg_schema[db][tbl][col_lower] = "STRING"
                    logger.warning(
                        "Schema patched: added missing column '%s' to %s.%s",
                        col_name, db, tbl,
                    )
                    return True

    return False


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

    # Normalize identifiers for BigQuery: lowercase column names and aliases
    # while preserving table/dataset names (which are case-sensitive in BQ).
    # This is required because MappingSchema normalizes column names to
    # lowercase, but the SQL may have uppercase column names (common in BQ).
    # Without this, qualify_columns() in _expand_star fails with
    # "Unknown column" errors for any SQL with JOINs.
    normalized_sql = _normalize_bq_identifiers(normalized_sql)

    # Build schema in sqlglot nested format: {db: {table: {col: type}}}
    sg_schema = _build_sqlglot_schema(schemas)

    # Pre-populate schema with columns explicitly referenced in the SQL
    # so that qualify_columns (used by _expand_star) can properly qualify them
    _pre_populate_schema_from_sql(normalized_sql, sg_schema)

    # Expand ARRAY_AGG(STRUCT(...))[OFFSET(N)].* into individual columns
    normalized_sql = _expand_array_agg_struct_star(normalized_sql)

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
        result = None
        already_patched: set[str] = set()

        for _attempt in range(_MAX_SCHEMA_PATCH_RETRIES):
            try:
                result = lineage(
                    col, normalized_sql, schema=sg_schema, dialect="bigquery"
                )
                break
            except OptimizeError as e:
                error_msg = str(e)
                if "unknown column" in error_msg.lower():
                    missing_col = _extract_unknown_column(error_msg)
                    if (
                        missing_col
                        and missing_col not in already_patched
                        and _add_missing_column_to_schema(
                            missing_col, normalized_sql, sg_schema
                        )
                    ):
                        already_patched.add(missing_col)
                        continue
                logger.warning("lineage() failed for %s.%s: %s", view_id, col, e)
                _add_unknown_mapping(edge_data, source_tables, col)
                break
            except Exception:
                logger.warning("lineage() failed for %s.%s", view_id, col)
                _add_unknown_mapping(edge_data, source_tables, col)
                break
        else:
            # Exhausted retries
            logger.warning("lineage() exhausted retries for %s.%s", view_id, col)
            _add_unknown_mapping(edge_data, source_tables, col)

        if result is None:
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


def _normalize_bq_identifiers(sql: str) -> str:
    """Normalize BigQuery SQL identifiers for case-insensitive matching.

    BigQuery column names are case-insensitive, but sqlglot's MappingSchema
    normalizes them to lowercase. This function normalizes the SQL to match:
    - Column names and aliases → lowercase
    - Table and dataset names → preserved (case-sensitive in BigQuery)

    Without this, qualify_columns() and lineage() can fail when SQL uses
    uppercase column names (common in BigQuery) because they don't match
    the lowercase schema keys.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return sql

    if parsed is None:
        return sql

    try:
        normalized = normalize_identifiers(parsed, dialect="bigquery")
        return normalized.sql(dialect="bigquery")
    except Exception:
        return sql


def _pre_populate_schema_from_sql(
    sql: str,
    sg_schema: dict[str, dict[str, dict[str, str]]],
) -> None:
    """Pre-populate schema with columns explicitly referenced in single-table SELECTs.

    When a SELECT references columns from a single table (no JOINs), all column
    references must come from that table. If some columns are not in the schema
    (e.g., the table was scanned with limited column info), qualify_columns will
    leave them unqualified, breaking lineage tracing.

    This function finds such columns and adds them to the schema as STRING
    placeholders so qualify_columns can properly qualify them.

    Mutates sg_schema in place.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return

    if not parsed:
        return

    # Build alias -> (db, table_name) map
    alias_map: dict[str, tuple[str, str]] = {}
    for table in parsed.find_all(exp.Table):
        tbl_name = table.name
        db = table.db
        if db and tbl_name:
            alias = (table.alias or tbl_name).lower()
            alias_map[alias] = (db, tbl_name)

    for select_node in parsed.find_all(exp.Select):
        from_clause = select_node.args.get("from_")
        if not from_clause:
            continue

        source = from_clause.this
        if not isinstance(source, exp.Table):
            continue

        # Skip if this SELECT has JOINs (ambiguous column source)
        if select_node.args.get("joins"):
            continue

        db = source.db
        tbl_name = source.name
        if not db or not tbl_name:
            continue

        # Ensure the table exists in the schema
        if db not in sg_schema or tbl_name not in sg_schema.get(db, {}):
            continue

        table_schema = sg_schema[db][tbl_name]

        # Collect all column references in this SELECT scope, excluding
        # columns inside nested subqueries (which belong to a different scope)
        for col in select_node.find_all(exp.Column):
            # Skip columns inside nested Select nodes (subqueries)
            parent = col.parent
            in_nested = False
            while parent is not select_node and parent is not None:
                if isinstance(parent, exp.Select):
                    in_nested = True
                    break
                parent = parent.parent
            if in_nested:
                continue

            col_name = col.name.lower()
            if col_name and col_name not in table_schema:
                table_schema[col_name] = "STRING"
                logger.debug(
                    "Schema pre-populated: added '%s' to %s.%s",
                    col_name, db, tbl_name,
                )


def _expand_array_agg_struct_star(sql: str) -> str:
    """Expand ARRAY_AGG(STRUCT(...))[OFFSET(N)].* into individual columns.

    BigQuery allows struct unpacking via .* on an array element access.
    sqlglot.lineage() cannot trace columns through this pattern, so we
    rewrite each struct field into its own ARRAY_AGG expression.

    Example:
        ARRAY_AGG(STRUCT(a, expr AS b) ORDER BY x LIMIT 1)[OFFSET(0)].*
    Becomes:
        ARRAY_AGG(a ORDER BY x LIMIT 1)[OFFSET(0)] AS a,
        ARRAY_AGG(expr ORDER BY x LIMIT 1)[OFFSET(0)] AS b
    """
    try:
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
    except Exception:
        return sql

    if parsed is None:
        return sql

    # Process all SELECT scopes (main query + CTEs)
    modified = False
    for select_node in parsed.find_all(exp.Select):
        new_selects: list[exp.Expression] = []
        scope_modified = False

        for select_expr in select_node.expressions:
            # Detect pattern: Dot(Bracket(ArrayAgg(...)), Star)
            if not (
                isinstance(select_expr, exp.Dot)
                and isinstance(select_expr.expression, exp.Star)
                and isinstance(select_expr.this, exp.Bracket)
            ):
                new_selects.append(select_expr)
                continue

            bracket = select_expr.this
            inner = bracket.this
            if not isinstance(inner, exp.ArrayAgg):
                new_selects.append(select_expr)
                continue

            # Navigate: ArrayAgg > (Limit?) > (Order?) > Struct
            agg_inner = inner.this
            order_node = None
            limit_node = None
            if isinstance(agg_inner, exp.Limit):
                limit_node = agg_inner
                agg_inner = agg_inner.this
            if isinstance(agg_inner, exp.Order):
                order_node = agg_inner
                agg_inner = agg_inner.this

            if not isinstance(agg_inner, exp.Struct):
                new_selects.append(select_expr)
                continue

            scope_modified = True
            offset_exprs = bracket.expressions

            # For each field in the struct, create an individual ARRAY_AGG
            for field in agg_inner.expressions:
                if isinstance(field, exp.PropertyEQ):
                    field_name = field.this.name
                    field_expr = field.expression
                elif isinstance(field, exp.Alias):
                    field_name = field.alias
                    field_expr = field.this
                elif isinstance(field, exp.Column):
                    field_name = field.name
                    field_expr = field
                else:
                    field_name = field.sql(dialect="bigquery")
                    field_expr = field

                # Reconstruct: ARRAY_AGG(field_expr ORDER BY ... LIMIT N)[OFFSET(M)]
                core = field_expr.copy()
                if order_node:
                    core = exp.Order(
                        this=core,
                        expressions=[o.copy() for o in order_node.expressions],
                    )
                if limit_node:
                    core = exp.Limit(
                        this=core,
                        expression=limit_node.expression.copy(),
                    )

                new_agg = exp.ArrayAgg(this=core)
                new_bracket = exp.Bracket(
                    this=new_agg,
                    expressions=[o.copy() for o in offset_exprs],
                )
                aliased = exp.Alias(
                    this=new_bracket,
                    alias=exp.to_identifier(field_name),
                )
                new_selects.append(aliased)

        if scope_modified:
            select_node.set("expressions", new_selects)
            modified = True

    if not modified:
        return sql
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

    If qualify_columns fails with "Unknown column" (common when views
    reference columns that no longer exist in source tables), the
    schema is patched with placeholder columns and the operation is
    retried. This prevents a single missing column from aborting
    star expansion for the entire SQL.

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

    already_patched: set[str] = set()

    for _attempt in range(_MAX_SCHEMA_PATCH_RETRIES):
        # Re-parse on each attempt (qualify_columns modifies AST in place)
        try:
            parsed = sqlglot.parse(sql, dialect="bigquery")[0]
        except Exception:
            return sql

        if parsed is None:
            return sql

        # Assign aliases to anonymous derived tables so qualify_columns
        # can resolve columns through nested subqueries
        _assign_derived_table_aliases(parsed)

        # Normalize identifiers (lowercase columns/aliases, preserve table names)
        # so they match MappingSchema's normalized column names.
        parsed = normalize_identifiers(parsed, dialect="bigquery")

        try:
            schema_obj = MappingSchema(schema=sg_schema, dialect="bigquery")
            qualified = qualify_columns(parsed, schema=schema_obj, dialect="bigquery")
            return qualified.sql(dialect="bigquery")
        except OptimizeError as e:
            error_msg = str(e)
            if "unknown column" in error_msg.lower():
                missing_col = _extract_unknown_column(error_msg)
                if (
                    missing_col
                    and missing_col not in already_patched
                    and _add_missing_column_to_schema(missing_col, sql, sg_schema)
                ):
                    already_patched.add(missing_col)
                    continue
            logger.debug("Failed to expand SELECT * for SQL: %s...", sql[:80])
            return sql
        except Exception:
            logger.debug("Failed to expand SELECT * for SQL: %s...", sql[:80])
            return sql

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
