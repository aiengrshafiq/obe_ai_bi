import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from app.services.vanna_wrapper import vn

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    """

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str) -> dict:
        if df is None or df.empty:
            return {"type": "none", "data": None, "thought": "No data found."}

        # 1. Profile Data
        row_count = len(df)
        col_count = len(df.columns)
        sql_upper = sql.upper()
        user_lower = user_question.lower()

        # 2. Detect Semantics
        has_aggregates = any(x in sql_upper for x in ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX("])
        is_raw_select = ("SELECT *" in sql_upper) or (not has_aggregates)
        
        # 3. Detect Keywords
        is_funnel = "funnel" in user_lower or "stage" in df.columns
        force_table = any(x in user_lower for x in ["table", "list", "records", "details", "sample"])
        force_chart = any(x in user_lower for x in ["chart", "graph", "plot", "trend", "visualize"])

        # --- DECISION TREE ---
        
        # A. Funnel
        if is_funnel:
            code = await vn.generate_plotly_code_async(
                "Create a Funnel Chart. Use text column for stages, numeric for values.", sql, df
            )
            fig_json = VisualizationAgent._execute_plotly_code(code, df)
            return {"type": "plotly", "data": fig_json, "thought": "Detected Funnel intent."}

        # B. Single Row -> Table
        if row_count == 1:
            return {"type": "table", "data": None, "thought": "Single row result."}

        # C. Forced Table / Raw Data
        if force_table or (is_raw_select and not force_chart):
            return {"type": "table", "data": None, "thought": "Raw data or user requested table."}

        # D. Forced Chart
        if force_chart:
            if col_count < 2:
                return {"type": "table", "data": None, "thought": "Data insufficient for chart (1 column)."}
            
            code = await vn.generate_plotly_code_async(
                f"Visualize this data. Question: {user_question}", sql, df
            )
            fig_json = VisualizationAgent._execute_plotly_code(code, df)
            return {"type": "plotly", "data": fig_json, "thought": "User requested chart."}

        # E. Analytical Data (Default to Chart)
        if has_aggregates and row_count > 1:
            instructions = (
                f"Visualize this data. Question: '{user_question}'. "
                f"CRITICAL PRE-PROCESSING: "
                f"1. If the dataframe has a column 'ds' (YYYYMMDD string), convert it first: "
                f"   df['ds'] = pd.to_datetime(df['ds'].astype(str), format='%Y%m%d') "
                f"2. Sort the dataframe by date. "
                f"VISUALIZATION RULES: "
                f"1. Use Grouped Bar for multiple metrics. "
                f"2. Use Line Chart for single metric trends. "
                f"3. X-axis tick format: '%Y-%m-%d'. "
                f"4. Assign the figure to variable 'fig'."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df)
            return {"type": "plotly", "data": fig_json, "thought": "Detected analytical trend."}

        # F. Fallback
        return {"type": "table", "data": None, "thought": "Standard list."}

    @staticmethod
    def _execute_plotly_code(code: str, df: pd.DataFrame) -> str:
        """
        Safely executes the LLM-generated Python code to produce a Plotly Figure JSON.
        """
        try:
            # Create a localized environment
            local_vars = {"df": df, "go": go, "px": px, "pd": pd}
            
            # Execute the string code
            exec(code, {}, local_vars)
            
            # The LLM is trained to create a variable named 'fig'
            fig = local_vars.get("fig")
            
            if fig:
                # Convert to JSON for the frontend
                return json.loads(fig.to_json())
            else:
                return None
        except Exception as e:
            print(f"Viz Error: {e}")
            return None