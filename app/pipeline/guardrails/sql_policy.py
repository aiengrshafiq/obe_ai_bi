import sqlglot
from sqlglot import exp

class SQLPolicyException(Exception):
    """Raised when SQL violates safety rules."""
    pass

class SQLGuard:
    """
    Production-grade SQL Validator using AST parsing.
    Enforces: Read-Only, Row Limits (Smart), No prohibited functions.
    """
    
    FORBIDDEN_COMMANDS = {
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "GRANT", "REVOKE", 
        "TRUNCATE", "CREATE", "REPLACE", "MERGE", "CALL", "EXPLAIN"
    }
    
    FORBIDDEN_FUNCTIONS = {
        "PG_SLEEP", "PG_TERMINATE_BACKEND", "VERSION", "CURRENT_USER", "DBLINK"
    }

    @staticmethod
    def validate_and_fix(sql: str) -> str:
        """
        Parses SQL, checks rules, and injects LIMIT only for non-aggregated queries.
        """
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            raise SQLPolicyException(f"Invalid SQL syntax: {str(e)}")

        # 1. Enforce SELECT / WITH only
        if not isinstance(parsed, (exp.Select, exp.Union, exp.With)):
            raise SQLPolicyException("Security Alert: Only SELECT statements are allowed.")

        # 2. Walk the tree for Security Checks & Aggregation Detection
        has_aggregation = False
        
        # Check if there is a GROUP BY clause immediately
        if parsed.args.get("group"):
            has_aggregation = True

        for node in parsed.walk():
            # Security: Check for forbidden commands
            if isinstance(node, exp.Command):
                raise SQLPolicyException("Security Alert: System commands are forbidden.")
            
            # Security: Check for forbidden functions
            if isinstance(node, exp.Func):
                func_name = node.sql().split('(')[0].upper()
                if func_name in SQLGuard.FORBIDDEN_FUNCTIONS:
                    raise SQLPolicyException(f"Security Alert: Function '{func_name}' is banned.")
                
                # Logic: Check for aggregate functions
                if func_name in {"SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE"}:
                    has_aggregation = True

        # 3. Smart Limit Injection
        # Only force LIMIT if it's a RAW data query (No aggregation/group by)
        limit_node = parsed.args.get("limit")
        
        if not limit_node:
            if not has_aggregation:
                # Raw data -> Force Safety Limit
                parsed = parsed.limit(100)
            else:
                # Aggregation -> Allow full result (Trend lines need all points)
                pass 
        else:
            # If User/LLM provided a limit, we generally trust it, 
            # but we could clamp it here if needed (e.g. max 5000).
            pass

        return parsed.sql()