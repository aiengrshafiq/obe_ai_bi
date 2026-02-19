import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import re
import numpy as np
from app.services.vanna_wrapper import vn
import asyncio
import base64

# --- 1. COMPILE REGEX ONCE (Performance) ---
IDENTIFIER_NAME_RE = re.compile(
    r"(?:^|_)(id|code|user_code|uid|order_id|request_id|visitor_id|linked_id|address|hash|txid|wallet)(?:$|_)",
    re.IGNORECASE
)

class VisualizationAgent:
    """
    Safe Visualization Engine.
    Uses LLM to generate a JSON Spec, then builds the chart deterministically in Python.
    No code execution allowed.
    """

    @staticmethod
    def _is_identifier_column(df: pd.DataFrame, col: str) -> bool:
        """Heuristically detects if a column is an Identifier (not a metric)."""
        name = col.lower()

        # 1) Name-based (strong signal)
        if IDENTIFIER_NAME_RE.search(name):
            return True

        # 2) Data-based (fallback): mostly unique + integer-like strings
        try:
            s = df[col].dropna()
            if len(s) < 5: return False
            
            # High uniqueness ratio (>90% unique) is suspicious for a metric
            uniq_ratio = s.nunique() / len(s)
            if uniq_ratio > 0.90:
                # If it looks like integers (even if stored as float/string)
                s_as_str = s.astype(str)
                # Check if 80% match digits
                int_like_ratio = s_as_str.str.fullmatch(r"\d+").mean()
                if int_like_ratio > 0.80:
                    return True
        except:
            pass

        return False

    @staticmethod
    def _clean_data_for_plotting(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares data for plotting:
        - Forces Identifiers to Strings (prevents plotting them).
        - Converts valid numerics to Numbers.
        - Converts dates.
        """
        df = df.copy()
        df = df.reset_index(drop=True) 

        date_like_names = {'ds','date','day','time','created_at','trade_datetime','registration_date','hour','trade_date'}

        for col in df.columns:
            # ✅ RULE 1: If identifier -> force string (Kill the chart candidate)
            if VisualizationAgent._is_identifier_column(df, col):
                df[col] = df[col].astype(str).str.strip()
                continue

            col_lower = col.lower()
            
            # ✅ RULE 2: Date handling
            if col_lower in date_like_names or 'date' in col_lower:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except: pass
            
            # ✅ RULE 3: Numeric handling (Only if NOT an identifier)
            else:
                try:
                    # Clean currency symbols if any
                    s = df[col].astype(str).str.replace(',', '', regex=False).str.replace('$', '', regex=False).str.strip()
                    num = pd.to_numeric(s, errors='coerce')
                    # If >60% are valid numbers, treat as numeric
                    if num.notna().mean() >= 0.60:
                        df[col] = num
                except: pass

        return df

    @staticmethod
    def _decode_plotly_typed_array(obj):
        """Recursively finds and converts {'dtype': '...', 'bdata': '...'} to lists."""
        if isinstance(obj, dict) and "dtype" in obj and "bdata" in obj:
            try:
                raw = base64.b64decode(obj["bdata"])
                dtype = np.dtype(obj["dtype"])
                arr = np.frombuffer(raw, dtype=dtype)
                if "shape" in obj and obj["shape"]:
                    arr = arr.reshape(obj["shape"])
                return arr.tolist()
            except Exception:
                return [] 

        if isinstance(obj, dict):
            return {k: VisualizationAgent._decode_plotly_typed_array(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [VisualizationAgent._decode_plotly_typed_array(v) for v in obj]
        return obj

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        """
        Main entry point.
        """
        if df is None or df.empty:
            return {"visual_type": "none", "plotly_code": None, "thought": "No data found."}

        # 1. Clean & Type the Data
        df = VisualizationAgent._clean_data_for_plotting(df)
        
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()

        # 2. KPI Check
        if row_count == 1 and col_count <= 2:
            return {"visual_type": "table", "plotly_code": None, "thought": "KPI / Single Record."}

        # 3. Chartability Gate (Semantic Guardrail)
        # Identify true metrics (Numeric columns that are NOT identifiers)
        identifier_cols = [c for c in df.columns if VisualizationAgent._is_identifier_column(df, c)]
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        measure_cols = [c for c in numeric_cols if c not in identifier_cols]

        # ⛔ HARD STOP: If we have no numbers to plot, force Table.
        if len(measure_cols) == 0:
            return {
                "visual_type": "table", 
                "plotly_code": None, 
                "thought": "Data contains identifiers but no plottable metrics. Forcing Table."
            }

        # 4. LLM Chart Logic
        prompt = f"""
        You are a strict Data Visualization API. You do not speak English. You ONLY output raw JSON.
        User Question: "{user_question}"
        Data Columns: {columns}
        Data Sample (Top 3 rows):
        {df.head(3).to_string(index=False)}

        CRITICAL RULES:
        1. OUTPUT EXACTLY ONE JSON OBJECT. 
        2. DO NOT WRITE ANY EXPLANATIONS, GREETINGS, OR TEXT BEFORE OR AFTER THE JSON.
        3. DO NOT OUTPUT SQL.
        4. Choose best 'chart_type': 'bar', 'line', 'scatter', 'area'. (DO NOT USE PIE. If pie is requested, default to 'bar').
        5. Identify 'x_column' (categories/dates) and 'y_column' (values) from the Data Columns provided.
        6. Provide a short 'title'.

        Response Format (JSON ONLY):
        {{
            "chart_type": "bar",
            "x_column": "root_user_code",
            "y_column": "total_referral_volume",
            "title": "Top Partners by Volume",
            "color_column": null
        }}
        
        """

        try:
            response = await asyncio.to_thread(vn.generate_summary, question=prompt, df=df)
            
            # Clean Markdown formatting if present
            json_str = response.replace("```json", "").replace("```", "").strip()
            
            # Safely extract JSON boundaries
            start = json_str.find("{")
            end = json_str.rfind("}") + 1
            
            # 🛡️ THE FIX: Ensure we actually found a JSON object before parsing
            if start != -1 and end != -1 and start < end:
                json_str = json_str[start:end]
                spec = json.loads(json_str)
            else:
                # If LLM hallucinates SQL or pure text, force the fallback
                raise ValueError(f"LLM did not return a valid JSON object. Raw output: {response[:50]}...")
            
            fig = VisualizationAgent._build_plotly_figure(df, spec)
            
            if fig:
                raw_json = json.loads(fig.to_json())
                clean_json = VisualizationAgent._decode_plotly_typed_array(raw_json)
                
                return {
                    "visual_type": "plotly", 
                    "plotly_code": clean_json, 
                    "thought": f"Generated {spec.get('chart_type')} chart."
                }

        except Exception as e:
            # Silent fallback to Table (no scary console errors)
            print(f"⚠️ Visualization Fallback: {e}")
        
        return {"visual_type": "table", "plotly_code": None, "thought": "Standard data table."}

    @staticmethod
    def _build_plotly_figure(df: pd.DataFrame, spec: dict):
        """
        Executes safe Plotly Express calls based on the dictionary spec.
        """
        chart_type = spec.get("chart_type", "bar").lower()
        x = spec.get("x_column")
        y = spec.get("y_column")
        title = spec.get("title", "Data Visualization")
        color = spec.get("color_column")

        if x and x not in df.columns: return None
        
        # Ensure Y is valid
        if isinstance(y, list):
            valid_y = [c for c in y if c in df.columns]
            if not valid_y: return None
            y = valid_y
        elif y and y not in df.columns: 
            return None

        fig = None
        try:
            if chart_type == "bar":
                fig = px.bar(df, x=x, y=y, color=color, title=title, template="plotly_white")
            elif chart_type == "line":
                fig = px.line(df, x=x, y=y, color=color, title=title, template="plotly_white")
            elif chart_type == "area":
                fig = px.area(df, x=x, y=y, color=color, title=title, template="plotly_white")
            elif chart_type == "pie":
                fig = px.pie(df, names=x, values=y, title=title, template="plotly_white")
            elif chart_type == "scatter":
                fig = px.scatter(df, x=x, y=y, color=color, title=title, template="plotly_white")
            elif chart_type == "funnel":
                fig = px.funnel(df, x=y, y=x, title=title, template="plotly_white")
            
            if fig:
                fig.update_layout(
                    margin=dict(l=40, r=40, t=40, b=40),
                    font=dict(family="Inter, sans-serif", color="#475569")
                )
        except Exception as e:
            print(f"⚠️ Plot Build Error: {e}")
            return None

        return fig