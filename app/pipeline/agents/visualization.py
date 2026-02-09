import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from app.services.vanna_wrapper import vn

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    Applies Professional/Fintech styling rules.
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
        
        # A. Funnel (Specific Intent)
        if is_funnel:
            instructions = (
                "Create a Funnel Chart using plotly.graph_objects. "
                "Use a modern color scale (Blues). "
                "Ensure text info shows value and percent."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df)
            return {"type": "plotly", "data": fig_json, "thought": "Detected Funnel intent."}

        # B. Single Row -> Table (KPI)
        if row_count == 1:
            return {"type": "table", "data": None, "thought": "Single row result (KPI)."}

        # C. Forced Table / Raw Data
        if force_table or (is_raw_select and not force_chart):
            return {"type": "table", "data": None, "thought": "Raw data or user requested table."}

        # D. Forced Chart / Analytical Data
        if force_chart or (has_aggregates and row_count > 1):
            if col_count < 2:
                return {"type": "table", "data": None, "thought": "Data insufficient for chart (1 column)."}
            
            # --- PROMPT ENGINEERING FOR PROFESSIONAL CHARTS ---
            # We explicitly guide the chart choice based on data volume
            chart_guidance = "Use a Bar Chart (px.bar)" if row_count < 25 else "Use an Area Chart (px.area)"
            
            instructions = (
                f"Goal: Create a professional Financial/BI Chart for: '{user_question}'. "
                f"Data Shape: {row_count} rows. "
                
                f"CRITICAL PRE-PROCESSING: "
                f"1. If 'ds' column exists (YYYYMMDD), convert it: df['ds'] = pd.to_datetime(df['ds'].astype(str), format='%Y%m%d'). "
                f"2. Sort dataframe by the x-axis column. "
                
                f"VISUALIZATION RULES: "
                f"1. {chart_guidance}. Do NOT use a simple line chart unless requested. "
                f"2. Color Scheme: Use professional deep blues (discrete_sequence=['#2563eb', '#1d4ed8']). "
                f"3. Template: Use 'plotly_white'. "
                f"4. Y-Axis: Format with commas (tickformat=','). "
                f"5. Layout: Minimalist. Remove X-axis grid lines. Add hover data. "
                f"6. Assign result to variable 'fig'."
            )
            
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df)
            
            # Fallback if code gen fails
            if not fig_json:
                return {"type": "table", "data": None, "thought": "Chart generation failed, falling back to table."}
                
            return {"type": "plotly", "data": fig_json, "thought": f"Generated professional {chart_guidance.split(' ')[2]}."}

        # E. Fallback
        return {"type": "table", "data": None, "thought": "Standard list."}

    @staticmethod
    def _execute_plotly_code(code: str, df: pd.DataFrame) -> str:
        """
        Safely executes the LLM-generated Python code to produce a Plotly Figure JSON.
        """
        try:
            # Inject dependencies including pandas for the pre-processing logic
            local_vars = {"df": df, "go": go, "px": px, "pd": pd}
            
            # Execute
            exec(code, {}, local_vars)
            
            # Extract 'fig'
            fig = local_vars.get("fig")
            
            if fig:
                # --- Post-Processing Injection (Safety Polish) ---
                # Even if the LLM forgets, we enforce the 'white' template and margins here
                fig.update_layout(
                    template="plotly_white",
                    margin=dict(l=40, r=40, t=40, b=40),
                    font=dict(family="Inter, sans-serif", color="#475569")
                )
                return json.loads(fig.to_json())
            else:
                return None
        except Exception as e:
            print(f"Viz Error: {e}")
            return None