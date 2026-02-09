import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from app.services.vanna_wrapper import vn

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    Uses Intent + Data Shape to make professional decisions.
    """

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        if df is None or df.empty:
            return {"type": "none", "data": None, "thought": "No data found."}

        # 1. Profile Data
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()
        
        # 2. Extract Intent
        intent_type = intent_result.get('intent_type') if intent_result else 'data_query'
        
        # 3. Decision Logic
        
        # A. KPI
        if row_count == 1 and col_count < 3:
            return {"type": "table", "data": None, "thought": "KPI / Single Record."}

        # B. Funnel Intent -> Funnel Chart
        if intent_type == 'funnel' or "funnel" in user_question.lower():
            instructions = (
                f"Data Columns: {columns}. "
                "Create a Funnel Chart using plotly.graph_objects. "
                "Ensure text info shows value and percent."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="funnel")
            return {"type": "plotly", "data": fig_json, "thought": "Detected Funnel intent."}

        # C. Trend/Analytics Intent -> Professional Chart
        # If intent is 'trend' OR data looks analytical (aggregates)
        has_aggregates = any(x in sql.upper() for x in ["GROUP BY", "SUM(", "COUNT(", "AVG("])
        
        if (intent_type == 'trend' or has_aggregates) and row_count > 1:
            if col_count < 2:
                return {"type": "table", "data": None, "thought": "Insufficient columns for chart."}
            
            # --- INTELLIGENT CHART SELECTION ---
            # Only use Area/Line if we have a Time Column.
            has_time_col = any(c.lower() in ['ds', 'date', 'day', 'time', 'created_at', 'trade_datetime', 'registration_date'] for c in columns)
            
            if has_time_col and row_count > 20:
                chart_type = "px.area"
                chart_name = "Area Trend"
            else:
                chart_type = "px.bar"
                chart_name = "Bar Chart"

            instructions = (
                f"Goal: Create a professional Financial Chart for: '{user_question}'. "
                f"Columns: {columns}. "
                
                f"CRITICAL PYTHON LOGIC (Strict execution order): "
                f"1. Identify the X-axis column (assign to `x_col`). Prefer date/time columns. "
                f"2. Identify the Y-axis numeric column (assign to `y_col`). "
                f"3. Force Numeric: df[y_col] = pd.to_numeric(df[y_col], errors='coerce'). "
                # Fix: Explicit date parsing to prevent '0-6 axis' issue
                f"4. Date Parsing: "
                f"   If x_col == 'ds': df[x_col] = pd.to_datetime(df[x_col].astype(str), format='%Y%m%d', errors='coerce'). "
                f"   Else: df[x_col] = pd.to_datetime(df[x_col], errors='coerce'). "
                f"5. Sort by x_col. "
                
                f"VISUALIZATION: "
                f"fig = {chart_type}(df, x=x_col, y=y_col). "
                f"Assign to 'fig'."
            )
            
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="standard")
            
            if fig_json:
                return {"type": "plotly", "data": fig_json, "thought": f"Generated {chart_name} based on intent."}

        # D. Fallback -> Table
        return {"type": "table", "data": None, "thought": "Standard data list."}

    @staticmethod
    def _execute_plotly_code(code: str, df: pd.DataFrame, chart_kind: str = "standard") -> str:
        try:
            local_vars = {"df": df, "go": go, "px": px, "pd": pd}
            exec(code, {}, local_vars)
            fig = local_vars.get("fig")
            
            if fig:
                # Smart Styling (Don't break Funnels/Pies with single color)
                if chart_kind == "standard":
                    try:
                        # Only override colors for Bar/Line/Area
                        fig.update_traces(marker=dict(color='#2563eb'), line=dict(color='#2563eb'))
                    except:
                        pass # Ignore if trace doesn't support marker/line props

                # Enforce Layout
                fig.update_layout(
                    template="plotly_white",
                    margin=dict(l=40, r=40, t=40, b=40),
                    font=dict(family="Inter, sans-serif", color="#475569"),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor='#f1f5f9', tickformat=',.2s'),
                    hovermode="x unified"
                )
                return json.loads(fig.to_json())
            return None
        except Exception as e:
            print(f"Viz Error: {e}")
            return None