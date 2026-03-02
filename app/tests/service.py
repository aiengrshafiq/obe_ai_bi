# app/tests/service.py
import json
import asyncio
from datetime import datetime
from sqlalchemy import text
from openai import OpenAI
from app.core.config import settings
from app.core.cube_registry import CubeRegistry
from app.services.vanna_wrapper import vn
# FIX: Import 'engine' explicitly
from app.db.safe_sql_runner import runner, engine 

# Separate client for the Judge to ensure unbiased grading
judge_client = OpenAI(
    api_key=settings.DASHSCOPE_API_KEY,
    base_url=settings.AI_BASE_URL,
)

class QAService:
    
    @staticmethod
    def _get_schema_context():
        """
        Fetches DDL + Docs for the Judge.
        This provides the 'Ground Truth' for the AI Critic.
        """
        context = ""
        for name, cube in CubeRegistry._registry.items():
            context += f"\nTABLE: {name} (KIND: {cube.kind})\nDDL: {cube.ddl}\nRULES: {cube.docs}\n"
        return context

    @staticmethod
    async def generate_test_cases(n=5):
        """
        Agent A: The Red Team Generator.
        Creates 'Trap' questions designed to break the system.
        """
        schema = QAService._get_schema_context()
        prompt = f"""
        You are a QA Lead for a BI Tool. 
        SCHEMA: {schema}
        
        Generate {n} diverse SQL test questions.
        
        MANDATORY CATEGORIES:
        1. **Date Logic:** "Last Month", "Since Jan 1st" (Tests strict date handling).
        2. **Join Logic:** "Total volume for Partner X" (Tests if it uses Snapshot totals vs Incremental joins).
        3. **Filters:** "High risk users" (Tests boolean flags).
        4. **Aggregation:** "Daily trend" (Tests LIMIT handling).
        5. **Exceptions:** "Blacklisted users" (Tests the 'No ds partition' exception).
        
        OUTPUT format: JSON array of strings ["q1", "q2"...]
        """
        
        try:
            response = judge_client.chat.completions.create(
                model=settings.AI_MODEL_NAME, 
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7 
            )
            content = response.choices[0].message.content.replace("```json", "").replace("```", "").strip()
            return json.loads(content)
        except Exception as e:
            print(f"QA Gen Error: {e}")
            # Fallback questions if Generator fails
            return [
                "Show total trading volume for partner 10000047",
                "Show user registration trend for last month",
                "List top 10 users by risk score",
                "Show daily trading volume for the last 7 days",
                "Show reason why user 10005727 is blacklisted"
            ]

    @staticmethod
    def evaluate_sql(question, sql):
        """
        Agent B: The Judge.
        Grades the SQL against the 'Iron Wall' rules without running it.
        """
        schema = QAService._get_schema_context()
        prompt = f"""
        You are a Senior SQL Code Reviewer for a Python-based BI Tool.
        
        USER QUESTION: "{question}"
        GENERATED SQL: {sql}
        
        SCHEMA CONTEXT (Pay attention to KIND: 'df' vs 'di'):
        {schema}
        
        STRICT GRADING RUBRIC:
        1. **Templating is VALID & REQUIRED:** - The SQL MUST use Python placeholders like `{{latest_ds}}`, `{{start_30d_dash}}`, etc.
           - **DO NOT** fail the test because of curly braces `{{...}}`. They are correct.
            **Templating vs SQL Math:** - Python placeholders like `{{latest_ds}}` are PREFERRED.
           - **HOWEVER:** If a specific timeframe (e.g. "15 days") is not in the variables, standard SQL date logic (Subqueries, `TO_CHAR`, `DATE_ADD`) is **ALLOWED**. Do NOT fail valid SQL just because it uses date math.
           
        2. **Partitioning Rules (Based on KIND):**
           - **If KIND = 'df' (Snapshot):** MUST filter `ds = '{{latest_ds}}'` (unless querying specific history).
                - **CRITICAL EXCEPTION 1:** If calculating a trend based on a **static date column** (like `registration_date`,`registration_date_only`, `created_at`, `first_deposit_date`) inside the Snapshot, it is **CORRECT** to use `ds = '{{latest_ds}}'`. Do NOT demand a `ds` range filter for these attributes.
                - **CRITICAL EXCEPTION 2 (Metric Trends):** If user asks for "Trend" of a snapshot metric (e.g. "Daily Active Traders", "Balance History"), querying a **range of partitions** (`ds >= ...`) is **CORRECT**.
           - **If KIND = 'di' (Incremental):** MUST have a `ds` range filter (e.g., `BETWEEN`, `>=`).
           - **Exception:** `risk_campaign_blacklist` has no `ds` column.
           
       
        3. **Risk vs. Blacklist (CRITICAL CONTEXT RULE):**
           - **Explicit Ban:** Only if user asks for "Banned", "Blocked", or "Blacklisted", MUST use `risk_campaign_blacklist`.
           - **Attribute-Based Risk:** If the user **defines** "High Risk" using profile attributes (e.g., "High risk users who are not KYC verified", "High volume users", "Bad Users"), you **MUST** use `user_profile_360`.
           - **Reasoning:** A user can be "High Risk" (Behavioral) without being "Banned" (Blacklisted). Do NOT fail the test just because the word "Risk" appears.

        4. **Join & Source Logic:** - **Lifetime/Total Stats:** If user asks for "Total", "Lifetime", or "Current Balance", you **MUST** use the Snapshot (`_df`). Do NOT sum the incremental table.
           - **Daily/Period Stats:** If user asks for "Yesterday", "Daily", or "Last 7 Days", querying the Incremental table (`_di`) is **CORRECT** and preferred. Do NOT suggest diffing two snapshots.
        
        OUTPUT JSON:
        {{
            "score": 1 (Fail) or 5 (Pass),
            "reason": "Brief explanation of failure (or 'LGTM' if pass)",
            "category": "Date" | "Join" | "Syntax" | "Logic"
        }}
        """
        
        try:
            response = judge_client.chat.completions.create(
                model=settings.AI_MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )
            return json.loads(response.choices[0].message.content.replace("```json", "").replace("```", ""))
        except:
            return {"score": 0, "reason": "Judge Crashed", "category": "System"}



    @staticmethod
    async def run_suite(count=5):
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        questions = await QAService.generate_test_cases(n=count)
        
        passed_count = 0
        results_to_save = []
        
        # 1. Run Tests
        for q in questions:
            start_time = datetime.now()
            try:
                # Generate SQL using the real App Pipeline
                sql = await vn.generate_sql_async(question=q)
                # NEW (Add the flag)
                #sql = await vn.generate_sql_async(question=q, allow_llm_to_see_data=True)
                
                # Judge the result
                verdict = QAService.evaluate_sql(q, sql)
                
                score = verdict.get('score', 0)
                if score == 5: passed_count += 1
                
                results_to_save.append({
                    "run_id": run_id,
                    "question": q,
                    "generated_sql": sql,
                    "status": "PASS" if score == 5 else "FAIL",
                    "failure_reason": verdict.get('reason', 'Unknown'),
                    "failure_category": verdict.get('category', 'General'),
                    "latency_ms": int((datetime.now() - start_time).total_seconds() * 1000)
                })
            except Exception as e:
                results_to_save.append({
                    "run_id": run_id,
                    "question": q,
                    "generated_sql": "ERROR",
                    "status": "FAIL",
                    "failure_reason": str(e),
                    "failure_category": "Crash",
                    "latency_ms": 0
                })

        # 2. Calculate Stats
        total = len(questions)
        accuracy = (passed_count / total) * 100 if total > 0 else 0
        
        # 3. Write to Hologres (Synchronously via safe engine)
        # FIX: Use 'engine' directly, not 'runner.engine'
        with engine.connect() as conn:
            # A. Insert Summary
            conn.execute(text("""
                INSERT INTO ai_pilot.qa_test_runs 
                (run_id, start_time, end_time, total_tests, passed_tests, accuracy_score, triggered_by)
                VALUES (:rid, :start, :end, :total, :passed, :acc, 'admin')
            """), {
                "rid": run_id,
                "start": datetime.now(), 
                "end": datetime.now(),
                "total": total,
                "passed": passed_count,
                "acc": accuracy
            })
            
            # B. Insert Details
            for res in results_to_save:
                conn.execute(text("""
                    INSERT INTO ai_pilot.qa_test_results 
                    (run_id, question, generated_sql, status, failure_reason, failure_category, latency_ms)
                    VALUES (:rid, :q, :sql, :stat, :reason, :cat, :lat)
                """), {
                    "rid": res["run_id"],
                    "q": res["question"],
                    "sql": res["generated_sql"],
                    "stat": res["status"],
                    "reason": res["failure_reason"],
                    "cat": res["failure_category"],
                    "lat": res["latency_ms"]
                })
            
            conn.commit()

        # 4. Return Data for Frontend Display
        return {
            "id": run_id,
            "passed": passed_count,
            "failed": total - passed_count,
            "total": total,
            "details": [{
                "question": r["question"],
                "sql": r["generated_sql"],
                "score": 5 if r["status"] == "PASS" else 1,
                "reason": r["failure_reason"],
                "category": r["failure_category"]
            } for r in results_to_save]
        }

    @staticmethod
    def get_history():
        """Fetches last 10 runs from Hologres."""
        # FIX: Use 'engine' directly
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT run_id, start_time, total_tests, passed_tests, accuracy_score 
                FROM ai_pilot.qa_test_runs 
                ORDER BY start_time DESC 
                LIMIT 10
            """))
            
            return [{
                "id": row.run_id,
                "timestamp": row.start_time.isoformat(),
                "total": row.total_tests,
                "passed": row.passed_tests,
                "accuracy": f"{row.accuracy_score}%"
            } for row in result]

    

    @staticmethod
    def get_run_details(run_id: str):
        """Fetches the detailed results for a specific historical run."""
        with engine.connect() as conn:
            # 1. Get Summary
            summary = conn.execute(text("""
                SELECT passed_tests, total_tests, accuracy_score 
                FROM ai_pilot.qa_test_runs 
                WHERE run_id = :rid
            """), {"rid": run_id}).fetchone()
            
            if not summary:
                return None

            # 2. Get Details
            rows = conn.execute(text("""
                SELECT question, generated_sql, status, failure_reason, failure_category 
                FROM ai_pilot.qa_test_results 
                WHERE run_id = :rid
            """), {"rid": run_id})
            
            return {
                "passed": summary.passed_tests,
                "total": summary.total_tests,
                "accuracy": summary.accuracy_score,
                "details": [{
                    "question": r.question,
                    "sql": r.generated_sql,
                    "score": 5 if r.status == "PASS" else 1,
                    "reason": r.failure_reason, # Ensures DB value maps to JSON 'reason'
                    "category": r.failure_category
                } for r in rows]
            }