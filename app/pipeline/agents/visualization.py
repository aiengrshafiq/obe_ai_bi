import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import re
import numpy as np
from app.services.vanna_wrapper import vn
import asyncio

class VisualizationAgent:
    """
    Safe Visualization Engine.
    Uses LLM to generate a JSON Spec, then builds the chart deterministically in Python.
    No code execution allowed.
    """

    @staticmethod
    def _make_jsonable(obj):
        """Recursively convert objects to JSON-safe types."""
        if isinstance(obj, (pd.Timestamp, np.datetime64)):
            return str(obj)
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj) if not (np.isnan(obj) or np.isinf(obj)) else None
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    @staticmethod
    async def determine_format(df: pd.DataFrame, sql: str, user_question: str, intent_result: dict = None) -> dict:
        """
        Main entry point.
        1. Analyzes Data shape.
        2. If complex, asks LLM for a JSON Spec.
        3. Builds Plotly JSON.
        """
        if df is None or df.empty:
            return {"visual_type": "none", "plotly_code": None, "thought": "No data found."}

        # Clean Data
        df = df.copy()
        # Convert all date-like columns
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower() or col == 'ds':
                try:
                    df[col] = pd.to_datetime(df[col])
                except: pass
        
        row_count = len(df)
        col_count = len(df.columns)
        columns = df.columns.tolist()

        # --- A. Simple Rules (Fast Path) ---
        # 1. KPI (Single Value)
        if row_count == 1 and col_count <= 2:
            return {"visual_type": "table", "plotly_code": None, "thought": "KPI / Single Record."}

        # --- B. LLM Chart Logic (JSON Path) ---
        # We ask the LLM to map columns to axes.
        
        prompt = f"""
        You are a Data Visualization Expert.
        User Question: "{user_question}"
        Data Columns: {columns}
        Data Sample (Top 3 rows):
        {df.head(3).to_string(index=False)}

        Task: Return a JSON object (NO CODE) to visualize this data.
        
        Rules:
        1. Choose best 'chart_type': 'bar', 'line', 'pie', 'scatter', 'funnel', 'area'.
        2. Identify 'x_column' (categories/dates) and 'y_column' (values).
        3. If multiple series (e.g. Buy vs Sell), set 'color_column' or use 'y_columns': ['col1', 'col2'].
        4. Provide a 'title'.

        Response Format (JSON ONLY):
        {{
            "chart_type": "bar",
            "x_column": "registration_date",
            "y_column": "user_count",
            "title": "Daily Registrations",
            "color_column": null
        }}
        """

        try:
            # 1. Get JSON from LLM
            
            response = await asyncio.to_thread(vn.generate_summary, question=prompt, df=df)
            
            # Clean response to ensure valid JSON
            json_str = response.replace("```json", "").replace("```", "").strip()
            # Find the first { and last }
            start = json_str.find("{")
            end = json_str.rfind("}") + 1
            if start != -1 and end != -1:
                json_str = json_str[start:end]
            
            spec = json.loads(json_str)
            
            # 2. Build Chart Deterministically
            fig = VisualizationAgent._build_plotly_figure(df, spec)
            
            if fig:
                # Convert to JSON for Frontend
                fig_json = json.loads(fig.to_json())
                return {"visual_type": "plotly", "plotly_code": fig_json, "thought": f"Generated {spec.get('chart_type')} chart."}

        except Exception as e:
            print(f"⚠️ Visualization Fallback: {e}")
        
        # Fallback to Table if anything fails
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

        # Validate columns exist
        if x and x not in df.columns: return None
        # Handle Y being a list or string
        if isinstance(y, list):
            for col in y:
                if col not in df.columns: return None
        elif y and y not in df.columns: return None

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