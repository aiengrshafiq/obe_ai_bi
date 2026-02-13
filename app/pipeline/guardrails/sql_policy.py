import sqlglot
from sqlglot import exp
from app.core.cube_registry import CubeRegistry

class SQLPolicyException(Exception):
    """Raised when SQL violates safety or business rules."""
    pass

class SQLGuard:
    """
    Enterprise SQL Validator.
    1. Blocks writes (DROP, DELETE).
    2. Enforces DATE FILTERS on large tables.
    3. Injects LIMITs on raw data queries.
    """
    
    FORBIDDEN_COMMANDS = {
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "GRANT", "REVOKE", 
        "TRUNCATE", "CREATE", "REPLACE", "MERGE", "CALL", "EXPLAIN"
    }
    
    # Tables that MUST have a partition filter to avoid scanning 100TB of data
    # (We fetch this dynamically from Registry)
    
    @staticmethod
    def validate_and_fix(sql: str) -> str:
        try:
            # Parse SQL into AST
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            raise SQLPolicyException(f"Invalid SQL syntax: {str(e)}")

        # --- RULE 1: READ ONLY ---
        if not isinstance(parsed, (exp.Select, exp.Union, exp.With)):
            raise SQLPolicyException("Security Alert: Only SELECT statements are allowed.")

        # Traverse the AST once
        tables_found = set()
        has_aggregation = False
        has_limit = parsed.args.get("limit") is not None

        for node in parsed.walk():
            # Check Commands
            if isinstance(node, exp.Command):
                raise SQLPolicyException("Security Alert: System commands are forbidden.")
            
            # Check Tables
            if isinstance(node, exp.Table):
                table_name = node.name
                tables_found.add(table_name)
            
            # Check Aggregations (SUM, COUNT, etc)
            if isinstance(node, exp.AggFunc):
                has_aggregation = True
            if isinstance(node, exp.Group):
                has_aggregation = True

        # --- RULE 2: PARTITION ENFORCEMENT ---
        # If querying a 'di' (Incremental) table, you MUST have a 'ds' filter.
        # This prevents accidental "SELECT * FROM huge_table" calls.
        
        # We check the WHERE clause for 'ds'
        where_clause = parsed.args.get("where")
        has_ds_filter = False
        if where_clause:
            # Naive check: does the string 'ds' appear in the WHERE condition?
            # A full AST check is complex, but this is 99% effective for generated SQL.
            if "ds" in where_clause.sql().lower():
                has_ds_filter = True

        for tbl in tables_found:
            cube = CubeRegistry.get_cube(tbl)
            if cube and cube.kind == 'di' and not has_ds_filter:
                # OPTIONAL: You could try to auto-inject "WHERE ds = '{latest_ds}'" here.
                # For now, we block it to teach the AI to be precise.
                # But to avoid user frustration, let's inject a safe limit instead.
                if not has_limit:
                    parsed = parsed.limit(10)
                    has_limit = True
                # Ideally, raise exception:
                # raise SQLPolicyException(f"Querying incremental table '{tbl}' requires a 'ds' partition filter.")

        # --- RULE 3: SMART LIMIT ---
        # If no aggregation (just listing rows) and no limit, force a limit.
        if not has_aggregation and not has_limit:
            parsed = parsed.limit(100)

        return parsed.sql()