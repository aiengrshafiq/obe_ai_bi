import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from app.services.vanna_wrapper import vn

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    Includes Guardrail A (Data Cleaning) and Guardrail B (Deterministic Axis Selection).
    """

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        if df is None or df.empty:
            return {"type": "none", "data": None, "thought": "No data found."}

        # --- GUARDRAIL A: Auto-Clean Data ---
        # Ensures numbers are floats and dates are datetimes before AI sees them.
        df = VisualizationAgent._clean_data_for_plotting(df)

        # 1. Profile Data
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()
        intent_type = intent_result.get('intent_type') if intent_result else 'data_query'

        # --- GUARDRAIL B: Deterministic Axis Selection ---
        # If we have exactly 2 columns, we don't need the AI to guess axes.
        forced_x, forced_y = VisualizationAgent._force_xy_for_two_columns(df)

        # 2. Decision Logic
        
        # A. KPI (Single Value)
        if row_count == 1 and col_count < 3:
            return {"type": "table", "data": None, "thought": "KPI / Single Record."}

        # B. Funnel Intent
        if intent_type == 'funnel' or "funnel" in user_question.lower():
            instructions = (
                f"Data Columns: {columns}. "
                "Create a Funnel Chart (plotly.graph_objects.Funnel). "
                "Use specific blue colors."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="funnel")
            return {"type": "plotly", "data": fig_json, "thought": "Detected Funnel intent."}

        # C. Trend / Analytics (The Professional Chart)
        has_aggregates = any(x in sql.upper() for x in ["GROUP BY", "SUM(", "COUNT(", "AVG("])
        
        if (intent_type == 'trend' or has_aggregates) and row_count > 1:
            if col_count < 2:
                return {"type": "table", "data": None, "thought": "Insufficient columns."}
            
            # Smart Chart Selection
            # Fix 3: Added 'registration_date', 'hour' to list
            has_time_col = any(c.lower() in ['ds', 'date', 'day', 'time', 'created_at', 'trade_datetime', 'registration_date', 'hour'] for c in columns)
            
            # Only use Area if Time is present. Otherwise Bar.
            if has_time_col and row_count > 20:
                chart_type = "px.area"
                chart_name = "Area Trend"
            else:
                chart_type = "px.bar"
                chart_name = "Bar Chart"

            # --- PROMPT GENERATION ---
            if forced_x and forced_y:
                # OPTION 1: Deterministic Mode (100% Safe)
                # Fix 4: Pre-sort in Python for safety
                df = df.sort_values(by=forced_x)
                
                instructions = (
                    f"Goal: Create a professional Financial Chart for: '{user_question}'. "
                    f"Use EXACTLY: x='{forced_x}', y='{forced_y}'. "
                    f"Data is already cleaned and sorted. "
                    f"VISUALIZATION: fig = {chart_type}(df, x='{forced_x}', y='{forced_y}'). "
                    f"Assign to 'fig'."
                )
            else:
                # OPTION 2: AI Guessing Mode (For complex tables)
                instructions = (
                    f"Goal: Create a professional Financial Chart for: '{user_question}'. "
                    f"Columns: {columns}. "
                    f"CRITICAL: 1. Identify x_col (date/cat) and y_col (numeric). "
                    f"2. Sort by x_col. "
                    f"VISUALIZATION: fig = {chart_type}(df, x=x_col, y=y_col). "
                    f"Assign to 'fig'."
                )
            
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="standard")
            
            if fig_json:
                return {"type": "plotly", "data": fig_json, "thought": f"Generated {chart_name}."}

        # D. Fallback
        return {"type": "table", "data": None, "thought": "Standard data list."}

    @staticmethod
    def _clean_data_for_plotting(df: pd.DataFrame) -> pd.DataFrame:
        """
        Deterministically converts columns to correct types.
        Fix 1 & 2: Robust numeric conversion & Fixed assignment bug.
        """
        df = df.copy() 
        for col in df.columns:
            # 1. Date Conversion
            if col.lower() in ['ds', 'date', 'day', 'created_at', 'trade_datetime', 'registration_date', 'hour']:
                try:
                    df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d', errors='coerce')
                    # Fallback for ISO strings or other formats
                    if df[col].isna().all():
                         df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    # Fix 1: Assign back to df[col]
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # 2. Smart Numeric Conversion (Fix 2)
            else:
                try:
                    # Remove commas just in case (1,000 -> 1000)
                    s = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
                    num = pd.to_numeric(s, errors='coerce')
                    
                    # Only convert if it actually looks numeric (>= 60% are valid numbers)
                    # This prevents destroying categorical columns like "User Segment"
                    if num.notna().mean() >= 0.60:
                        df[col] = num
                except:
                    pass
        return df

    @staticmethod
    def _force_xy_for_two_columns(df: pd.DataFrame):
        """
        If df has exactly 2 columns, deterministically find X (Date) and Y (Metric).
        Returns (x_col, y_col) or (None, None).
        """
        if df is None or len(df.columns) != 2:
            return None, None

        if not pd.api.types.is_numeric_dtype(df[y_col]):
            return None, None

        cols = list(df.columns)
        date_like = {'ds','date','day','time','created_at','trade_datetime','registration_date','hour'}

        # Find X (Date)
        x_candidates = [c for c in cols if c.lower() in date_like]
        if not x_candidates:
            # Try checking data type if name doesn't match
            x_candidates = [c for c in cols if pd.api.types.is_datetime64_any_dtype(df[c])]

        if not x_candidates:
            return None, None # Logic failed, let AI guess

        x_col = x_candidates[0]
        # Y is simply the "other" column
        y_col = cols[1] if cols[0] == x_col else cols[0]

        return x_col, y_col

    @staticmethod
    def _execute_plotly_code(code: str, df: pd.DataFrame, chart_kind: str = "standard") -> str:
        try:
            local_vars = {"df": df, "go": go, "px": px, "pd": pd}
            exec(code, {}, local_vars)
            fig = local_vars.get("fig")
            
            if fig:
                # Post-Processing
                if chart_kind == "standard":
                    try:
                        fig.update_traces(marker=dict(color='#2563eb'), line=dict(color='#2563eb'))
                    except: pass 

                fig.update_layout(
                    template="plotly_white",
                    margin=dict(l=40, r=40, t=40, b=40),
                    font=dict(family="Inter, sans-serif", color="#475569"),
                    xaxis=dict(showgrid=False, title=None),
                    yaxis=dict(showgrid=True, gridcolor='#f1f5f9', tickformat=',.2s'),
                    hovermode="x unified"
                )
                return json.loads(fig.to_json())
            return None
        except Exception as e:
            print(f"Viz Error: {e}")
            return None