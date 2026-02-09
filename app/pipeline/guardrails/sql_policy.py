import sqlglot
from sqlglot import exp

class SQLPolicyException(Exception):
    """Raised when SQL violates safety rules."""
    pass

class SQLGuard:
    """
    Production-grade SQL Validator using AST parsing.
    Enforces: Read-Only, Row Limits, No prohibited functions.
    """
    
    FORBIDDEN_COMMANDS = {
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "GRANT", "REVOKE", 
        "TRUNCATE", "CREATE", "REPLACE", "MERGE", "CALL", "EXPLAIN"
    }
    
    # Block sleep/system functions to prevent DoS
    FORBIDDEN_FUNCTIONS = {
        "PG_SLEEP", "PG_TERMINATE_BACKEND", "VERSION", "CURRENT_USER", "DBLINK"
    }

    @staticmethod
    def validate_and_fix(sql: str) -> str:
        """
        Parses SQL, checks rules, and injects LIMIT if missing.
        Returns the sanitized SQL string.
        """
        try:
            # Parse SQL into AST (Abstract Syntax Tree)
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            raise SQLPolicyException(f"Invalid SQL syntax: {str(e)}")

        # 1. Enforce SELECT / WITH only
        if not isinstance(parsed, (exp.Select, exp.Union, exp.With)):
            raise SQLPolicyException("Security Alert: Only SELECT statements are allowed.")

        # 2. Walk the tree to find forbidden commands/functions
        for node in parsed.walk():
            # Check for forbidden commands (nested)
            if isinstance(node, exp.Command):
                raise SQLPolicyException("Security Alert: System commands are forbidden.")
            
            # Check for forbidden functions
            if isinstance(node, exp.Func):
                func_name = node.sql().split('(')[0].upper()
                if func_name in ["NOW", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP"]:
                     pass # In Phase 2, we can auto-replace this with '2026-02-08'. For now, let the prompt handle it.
                     
                if func_name in SQLGuard.FORBIDDEN_FUNCTIONS:
                    raise SQLPolicyException(f"Security Alert: Function '{func_name}' is banned.")

        # 3. Enforce Row Limits (The "Cost" Guard)
        # We check if there is a 'limit' expression in the query
        limit_node = parsed.args.get("limit")
        
        if not limit_node:
            # .limit() returns a new expression with the limit applied
            parsed = parsed.limit(100)
        else:
            # Optional: If limit exists but is too high (e.g. 1,000,000), cap it
            # current_limit = int(limit_node.expression.this)
            # if current_limit > 1000:
            #     parsed = parsed.limit(1000)
            pass

        return parsed.sql()

# Usage Example:
# safe_sql = SQLGuard.validate_and_fix("SELECT * FROM users")
# print(safe_sql) # -> SELECT * FROM users LIMIT 100