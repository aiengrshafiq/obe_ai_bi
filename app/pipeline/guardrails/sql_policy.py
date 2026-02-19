# app/pipeline/guardrails/sql_policy.py
import re
import sqlglot
from sqlglot import exp
from app.core.cube_registry import CubeRegistry

class SQLPolicyException(Exception):
    """Raised when SQL violates safety or business rules."""
    pass

class SQLGuard:
    """
    Enterprise SQL Validator & Optimizer (The Iron Wall).
    1. Pre-processes LLM syntax hallucinations (Intervals, NOW).
    2. Blocks writes (DROP, DELETE).
    3. Enforces strict df/di partitioning rules.
    4. Smartly manages LIMITs.
    """
    
    FORBIDDEN_COMMANDS = {
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "GRANT", "REVOKE", 
        "TRUNCATE", "CREATE", "REPLACE", "MERGE", "CALL", "EXPLAIN"
    }

    @staticmethod
    def _preprocess_sql(sql: str) -> str:
        """Fixes common LLM SQL hallucinations before parsing."""
        
        # 1. Fix broken intervals: `INTERVAL '30' days` -> `INTERVAL '30 days'`
        sql = re.sub(
            r"INTERVAL\s+'(\d+)'\s+(day|month|year|hour|minute)s?", 
            r"INTERVAL '\1 \2'", 
            sql, 
            flags=re.IGNORECASE
        )
        
        # 2. Normalize NOW() / CURRENT_DATE to our deterministic variable
        # The Orchestrator will replace {today_iso} later in the pipeline
        sql = re.sub(
            r"\b(NOW\(\)|CURRENT_DATE|CURRENT_TIMESTAMP)\b", 
            "'{today_iso}'", 
            sql, 
            flags=re.IGNORECASE
        )
        
        return sql

    @staticmethod
    def validate_and_fix(sql: str) -> str:
        # Step 1: Pre-process string
        sql = SQLGuard._preprocess_sql(sql)
        
        # Step 2: Parse into AST
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            raise SQLPolicyException(f"Invalid SQL syntax: {str(e)}")

        # --- RULE 1: READ ONLY ---
        if not isinstance(parsed, (exp.Select, exp.Union, exp.With)):
            raise SQLPolicyException("Security Alert: Only SELECT statements are allowed.")

        tables_found = set()
        has_aggregation = False
        has_limit = parsed.args.get("limit") is not None
        has_group_by = parsed.args.get("group") is not None

        # Detect ORDER BY ... DESC
        has_desc_order = False
        order_clause = parsed.args.get("order")
        if order_clause:
            for expression in order_clause.expressions:
                if expression.args.get("desc"):
                    has_desc_order = True

        for node in parsed.walk():
            if isinstance(node, exp.Command):
                raise SQLPolicyException("Security Alert: System commands are forbidden.")
            
            if isinstance(node, exp.Table):
                tables_found.add(node.name)
            
            if isinstance(node, (exp.AggFunc, exp.Group)):
                has_aggregation = True

        # --- RULE 2: SMART LIMIT ENFORCER ---
        if has_group_by and has_limit:
            # If it's grouped and HAS a DESC order, it's a "Top 10" query -> Keep limit
            # If it's grouped and ASC order (or no order), it's a "Trend" query -> STRIP limit
            if not has_desc_order:
                parsed.set("limit", None) # Remove the limit entirely
                has_limit = False
        elif not has_aggregation and not has_limit:
            # If raw list of data without a limit, inject safe limit
            parsed = parsed.limit(100)

        # --- RULE 3: STRICT PARTITION ENFORCEMENT (df vs di) ---
        where_clause = parsed.args.get("where")
        where_sql = where_clause.sql().lower() if where_clause else ""

        for tbl in tables_found:
            cube = CubeRegistry.get_cube(tbl)
            if not cube:
                continue

            # A. SNAPSHOT TABLES (df)
            if cube.kind == 'df':
                if "between" in where_sql and "ds " in where_sql:
                    # Reject it. The AI will catch this Exception and try again using the error message.
                    raise SQLPolicyException(
                        f"CRITICAL ERROR: '{tbl}' is a SNAPSHOT table. You CANNOT use 'ds BETWEEN'. "
                        f"You MUST use 'ds = ''{{latest_ds}}''' and apply any date range to the '{cube.time_column}' column."
                    )
            
            # B. INCREMENTAL TABLES (di)
            elif cube.kind == 'di':
                if "ds" not in where_sql:
                    raise SQLPolicyException(
                        f"CRITICAL ERROR: '{tbl}' is an INCREMENTAL table. You MUST include a 'ds' filter (e.g., ds BETWEEN)."
                    )

        # Return the optimized, safe, standardized SQL
        return parsed.sql()