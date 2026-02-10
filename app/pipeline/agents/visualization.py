import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
import numpy as np
import plotly.io as pio
from datetime import date, datetime, timedelta
from app.services.vanna_wrapper import vn

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    Hybrid Engine: Deterministic for simple data, LLM for complex.
    """

    @staticmethod
    def _make_jsonable(obj):
        """Recursively convert Plotly/Pandas/Numpy objects into JSON-safe types."""
        if isinstance(obj, (np.integer, int)): return int(obj)
        if isinstance(obj, (np.floating, float)): return float(obj)
        if isinstance(obj, (np.bool_, bool)): return bool(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime, date)): return obj.isoformat()
        if isinstance(obj, dict): return {k: VisualizationAgent._make_jsonable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)): return [VisualizationAgent._make_jsonable(v) for v in obj]
        return obj

    @staticmethod
    def _choose_chart(df: pd.DataFrame, x_col: str, y_col: str) -> str:
        """Determines the best chart type based on X-axis data type and density."""
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            if len(df) <= 15: return "bar"
            elif len(df) <= 100: return "line"
            else: return "area"
        return "bar"

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        print(f"✅ VIZ VERSION: 2026-02-10-LISTS-ONLY-FIX") 
        
        if df is None or df.empty:
            return {"visual_type": "none", "plotly_code": None, "thought": "No data found."}

        # --- GUARDRAIL A: Auto-Clean Data ---
        df = VisualizationAgent._clean_data_for_plotting(df)
        
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()
        intent_type = intent_result.get('intent_type') if intent_result else 'data_query'

        # --- GUARDRAIL B: Detect Axis for Simple Logic ---
        forced_x, forced_y = VisualizationAgent._force_xy_for_two_columns(df)
        print(f"[VIZ CHECK] Forced X: {forced_x}, Forced Y: {forced_y}")

        # 1. KPI (Single Value)
        if row_count == 1:
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if len(numeric_cols) == 1 and col_count <= 2:
                return {"visual_type": "table", "plotly_code": None, "thought": "KPI / Single Record."}

        # 2. Funnel Intent (Always LLM)
        if intent_type == 'funnel' or "funnel" in user_question.lower():
            instructions = (
                f"Data Columns: {columns}. "
                "Create a Funnel Chart (plotly.graph_objects.Funnel). "
                "Use specific blue colors."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="funnel")
            return {"visual_type": "plotly", "plotly_code": fig_json, "thought": "Detected Funnel intent."}

        # 3. Trend / Analytics
        has_aggregates = any(x in sql.upper() for x in ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX("])
        
        if (intent_type == 'trend' or has_aggregates) and row_count > 1:
            if col_count < 2:
                return {"visual_type": "table", "plotly_code": None, "thought": "Insufficient columns."}

            # --- GUARDRAIL C: DETERMINISTIC PLOTTING (NO LLM) ---
            if forced_x and forced_y:
                try:
                    print("[VIZ CHECK] ⚡ Entering Deterministic Mode (No LLM)")
                    
                    # 1. Sort
                    df = df.sort_values(by=forced_x)
                    
                    # 2. Convert Y to Numeric (Strip formatting)
                    df[forced_y] = pd.to_numeric(df[forced_y].astype(str).str.replace(',', ''), errors='coerce')
                    
                    # 3. EXTRACT PYTHON LISTS (CRITICAL FIX FOR BINARY DATA)
                    # Handle X (Dates to Strings)
                    if pd.api.types.is_datetime64_any_dtype(df[forced_x]):
                        x_list = df[forced_x].dt.strftime('%Y-%m-%d').tolist()
                    else:
                        x_list = df[forced_x].tolist()
                        
                    # Handle Y (Floats to List)
                    y_list = df[forced_y].tolist()
                    
                    # 4. Choose Chart Type
                    chart_type = VisualizationAgent._choose_chart(df, forced_x, forced_y)
                    
                    # 5. Build Chart using LISTS (Not DataFrame)
                    # NOTE: Passing x_list/y_list prevents Plotly from using 'bdata' compression
                    if chart_type == "area":
                        fig = px.area(x=x_list, y=y_list)
                        fig.update_traces(line=dict(color='#2563eb'))
                    elif chart_type == "line":
                        fig = px.line(x=x_list, y=y_list)
                        fig.update_traces(line=dict(color='#2563eb'))
                    else: # Bar
                        fig = px.bar(x=x_list, y=y_list)
                        fig.update_traces(marker=dict(color='#2563eb'))
                    
                    # 6. Apply Layout (Re-add titles since lists lost column names)
                    fig.update_layout(
                        template="plotly_white",
                        margin=dict(l=40, r=40, t=40, b=40),
                        font=dict(family="Inter, sans-serif", color="#475569"),
                        xaxis=dict(showgrid=False, title=dict(text=forced_x)),
                        yaxis=dict(showgrid=True, gridcolor='#f1f5f9', tickformat=',.2s', title=dict(text=forced_y)), 
                        hovermode="x unified"
                    )
                    
                    # 7. Safe Serialization
                    fig_dict = fig.to_dict()
                    return {
                        "visual_type": "plotly", 
                        "plotly_code": VisualizationAgent._make_jsonable(fig_dict), 
                        "thought": f"Generated Deterministic {chart_type.capitalize()} (Lists)."
                    }
                except Exception as e:
                    print(f"[VIZ ERROR] Deterministic Plot Failed: {e}. Falling back to LLM.")

            # --- FALLBACK: LLM GENERATION ---
            print("[VIZ CHECK] ⚠️ Fallback to LLM Generation")
            instructions = (
                f"Goal: Create a professional Financial Chart for: '{user_question}'. "
                f"Columns: {columns}. "
                f"CRITICAL: 1. Identify x_col (date/cat) and y_col (numeric). "
                f"2. Sort by x_col. "
                f"VISUALIZATION: fig = px.bar(df, x=x_col, y=y_col) (or px.area if time series). "
                f"Assign to 'fig'."
            )
            
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="standard")
            
            if fig_json:
                return {"visual_type": "plotly", "plotly_code": fig_json, "thought": "Generated Professional Chart (AI)."}

        # D. Fallback
        return {"visual_type": "table", "plotly_code": None, "thought": "Standard data list."}

    @staticmethod
    def _clean_data_for_plotting(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy() 
        date_like_names = {'ds','date','day','time','created_at','trade_datetime','registration_date','hour','trade_date'}

        for col in df.columns:
            col_lower = col.lower()
            
            # --- Date Conversion ---
            if col_lower in date_like_names or 'date' in col_lower:
                try:
                    df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d', errors='coerce')
                    if df[col].isna().all():
                         df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except: pass
            
            # --- Numeric Conversion ---
            else:
                try:
                    s = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
                    num = pd.to_numeric(s, errors='coerce')
                    if num.notna().mean() >= 0.60:
                        df[col] = num
                except:
                    pass
        return df

    @staticmethod
    def _force_xy_for_two_columns(df: pd.DataFrame):
        if df is None or len(df.columns) != 2:
            return None, None

        cols = list(df.columns)
        date_like = {'ds','date','day','time','created_at','trade_datetime','registration_date','hour','trade_date'}

        x_candidates = []
        for c in cols:
            if c.lower() in date_like:
                x_candidates.append(c)
                continue
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                x_candidates.append(c)
                continue
            try:
                first_val = df[c].dropna().iloc[0]
                if isinstance(first_val, (date, datetime)):
                    x_candidates.append(c)
            except: pass

        if not x_candidates:
            return None, None 

        x_col = x_candidates[0]
        y_col = cols[1] if cols[0] == x_col else cols[0]

        return x_col, y_col

    @staticmethod
    def _execute_plotly_code(code: str, df: pd.DataFrame, chart_kind: str = "standard") -> str:
        try:
            local_vars = {"df": df, "go": go, "px": px, "pd": pd}
            exec(code, {}, local_vars)
            fig = local_vars.get("fig")
            
            if fig:
                if chart_kind == "standard":
                    try:
                        fig.update_layout(template="plotly_white")
                        fig.update_traces(marker_color='#2563eb') 
                    except: pass 

                fig.update_layout(
                    margin=dict(l=40, r=40, t=40, b=40),
                    font=dict(family="Inter, sans-serif", color="#475569"),
                    xaxis=dict(showgrid=False, title=None),
                    yaxis=dict(showgrid=True, gridcolor='#f1f5f9', tickformat=',.2s'),
                    hovermode="x unified"
                )
                
                fig_dict = fig.to_dict()
                return VisualizationAgent._make_jsonable(fig_dict)
                
            return None
        except Exception as e:
            print(f"Viz Error: {e}")
            return None