import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from app.services.vanna_wrapper import vn
from datetime import date, datetime, timedelta
import re

class VisualizationAgent:
    """
    Decides format and Executes Plotly code to return JSON.
    Hybrid Engine: Deterministic for simple data, LLM for complex.
    """

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        print(f"✅ VIZ VERSION: 2026-02-09-GUARDRAIL-C-ACTIVE") # <--- PROOF OF DEPLOYMENT
        
        if df is None or df.empty:
            return {"type": "none", "data": None, "thought": "No data found."}

        # --- GUARDRAIL A: Auto-Clean Data ---
        df = VisualizationAgent._clean_data_for_plotting(df)
        
        # Debug Prints to Console
        print(f"[VIZ CHECK] Columns: {df.columns.tolist()}")
        print(f"[VIZ CHECK] Sample Row: {df.head(1).to_dict(orient='records')}")

        # 1. Profile Data
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()
        intent_type = intent_result.get('intent_type') if intent_result else 'data_query'

        # --- GUARDRAIL B: Detect Axis for Simple Logic ---
        forced_x, forced_y = VisualizationAgent._force_xy_for_two_columns(df)
        print(f"[VIZ CHECK] Forced X: {forced_x}, Forced Y: {forced_y}")

        # 2. Decision Logic
        
        # A. KPI (Single Value)
        if row_count == 1 and col_count < 3:
            return {"type": "table", "data": None, "thought": "KPI / Single Record."}

        # B. Funnel Intent (Always LLM)
        if intent_type == 'funnel' or "funnel" in user_question.lower():
            instructions = (
                f"Data Columns: {columns}. "
                "Create a Funnel Chart (plotly.graph_objects.Funnel). "
                "Use specific blue colors."
            )
            code = await vn.generate_plotly_code_async(instructions, sql, df)
            fig_json = VisualizationAgent._execute_plotly_code(code, df, chart_kind="funnel")
            return {"type": "plotly", "data": fig_json, "thought": "Detected Funnel intent."}

        # C. Trend / Analytics
        has_aggregates = any(x in sql.upper() for x in ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX("])
        
        if (intent_type == 'trend' or has_aggregates) and row_count > 1:
            if col_count < 2:
                return {"type": "table", "data": None, "thought": "Insufficient columns."}
            
            # Smart Chart Type Selection
            has_time_col = any(c.lower() in ['ds', 'date', 'day', 'time', 'created_at', 'trade_datetime', 'registration_date', 'hour'] for c in columns)
            chart_type = "area" if (has_time_col and row_count > 20) else "bar"

            # --- GUARDRAIL C: DETERMINISTIC PLOTTING (NO LLM) ---
            # This is the "Industry Standard" fix. Don't ask AI to plot 2 columns. Just do it.
            if forced_x and forced_y:
                try:
                    print("[VIZ CHECK] ⚡ Entering Deterministic Mode (No LLM)")
                    # 1. Strict Sort
                    df = df.sort_values(by=forced_x)
                    
                    # 2. Strict Numeric Conversion (Strip commas)
                    # This handles the '193,880,587.59' string format issue
                    df[forced_y] = pd.to_numeric(df[forced_y].astype(str).str.replace(',', ''), errors='coerce')
                    
                    # 3. Build Chart Directly
                    if chart_type == "area":
                        fig = px.area(df, x=forced_x, y=forced_y)
                        fig.update_traces(line=dict(color='#2563eb'))
                    else:
                        fig = px.bar(df, x=forced_x, y=forced_y)
                        fig.update_traces(marker=dict(color='#2563eb'))
                    
                    # 4. Apply Layout
                    fig.update_layout(
                        template="plotly_white",
                        margin=dict(l=40, r=40, t=40, b=40),
                        font=dict(family="Inter, sans-serif", color="#475569"),
                        xaxis=dict(showgrid=False, title=None),
                        yaxis=dict(showgrid=True, gridcolor='#f1f5f9', tickformat=',.2s'), 
                        hovermode="x unified"
                    )
                    
                    return {"type": "plotly", "data": json.loads(fig.to_json()), "thought": "Generated Deterministic Chart (100% Accurate)."}
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
                return {"type": "plotly", "data": fig_json, "thought": "Generated Professional Chart (AI)."}

        # D. Fallback
        return {"type": "table", "data": None, "thought": "Standard data list."}


    @staticmethod
    def _clean_data_for_plotting(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        date_like_names = {
            'ds','date','day','time','created_at','trade_datetime',
            'registration_date','hour','trade_date'
        }

        for col in df.columns:
            col_lower = col.lower()

            # ---------- DATE / TIME CONVERSION ----------
            is_name_date_like = (
                col_lower in date_like_names
                or "date" in col_lower
                or col_lower.endswith("_date")
                or col_lower.endswith("_dt")
            )

            if is_name_date_like:
                # 1) If it's ds-like 'YYYYMMDD', parse with format first
                try:
                    s = df[col].astype(str).str.strip()
                    dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
                    # 2) Fallback to generic parser (handles ISO strings and python date objects)
                    if dt.isna().all():
                        dt = pd.to_datetime(df[col], errors="coerce")
                    df[col] = dt
                except:
                    try:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    except:
                        pass
                continue  # done with date column

            # ---------- NUMERIC CONVERSION ----------
            try:
                s = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
                num = pd.to_numeric(s, errors='coerce')
                # convert only if mostly numeric
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
        date_like = {'ds','date','day','time','created_at','trade_datetime','registration_date','trade_date','hour'}
        #date_like = {'ds','date','day','time','created_at','trade_datetime','registration_date','trade_date'}

        x_candidates = []
        for c in cols:
            # 1. Name Match
            if c.lower() in date_like:
                x_candidates.append(c)
                continue
            
            # 2. Type Match (Timestamp OR Date Object)
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                x_candidates.append(c)
                continue
            
            # 3. Object Check (Is it a datetime.date object?)
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
                    # Fix: Apply styling generally, but don't crash on invalid props
                    try:
                        fig.update_layout(template="plotly_white")
                        fig.update_traces(marker_color='#2563eb') # Safer than specific dicts
                    except: pass 

                fig.update_layout(
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