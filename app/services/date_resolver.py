# app/services/date_resolver.py
import time
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
from app.db.app_models import SessionLocal  # <--- FIXED IMPORT (Matches Orchestrator)
from app.services.cache import cache

class DateResolver:
    """
    The Timekeeper.
    Ensures all SQL queries use the correct, deterministic 'latest_ds' partition.
    """

    @staticmethod
    def _fetch_ds_from_db():
        """
        Synchronous helper to run in a thread.
        """
        db = SessionLocal()
        try:
            # We query user_profile_360 as the "Anchor Table"
            result = db.execute(text("SELECT MAX(ds) FROM public.user_profile_360"))
            row = result.fetchone()
            if row and row[0]:
                return str(row[0])
            return None
        except Exception as e:
            print(f"⚠️ DateResolver DB Error: {e}")
            return None
        finally:
            db.close()

    @staticmethod
    async def get_latest_ds() -> str:
        """
        Returns the latest available partition 'YYYYMMDD'.
        Strategies:
        1. Memory Cache (Fastest)
        2. DB Query (Source of Truth) - Non-blocking
        3. Fallback (Yesterday)
        """
        # 1. Check Cache
        cached_ds = cache.get("latest_ds")
        if cached_ds:
            return str(cached_ds)

        # 2. Check DB (Run sync DB call in a separate thread)
        new_ds = await asyncio.to_thread(DateResolver._fetch_ds_from_db)
        
        if new_ds:
            # Cache for 1 hour
            cache.set("latest_ds", new_ds, ttl_seconds=3600)
            print(f"🔄 DateResolver: Refreshed latest_ds = {new_ds}")
            return new_ds

        # 3. Fallback: Yesterday
        fallback = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        print(f"⚠️ DateResolver: Using Fallback {fallback}")
        return fallback

    
    @staticmethod
    async def get_date_context() -> dict:
        latest_ds = await DateResolver.get_latest_ds()
        dt_current = datetime.strptime(latest_ds, "%Y%m%d")
        
        # Existing formats
        latest_ds_dash = dt_current.strftime("%Y-%m-%d")
        start_7d = (dt_current - timedelta(days=6)).strftime("%Y%m%d")
        start_7d_dash = (dt_current - timedelta(days=6)).strftime("%Y-%m-%d")
        start_30d = (dt_current - timedelta(days=29)).strftime("%Y%m%d")
        start_30d_dash = (dt_current - timedelta(days=29)).strftime("%Y-%m-%d")
        
        # NEW: Precise Calendar Month Logic
        # This Month Start
        start_this_month = dt_current.replace(day=1)
        
        # Last Month Start & End
        # Go back 1 day from the start of this month to get the last day of last month
        end_last_month = start_this_month - timedelta(days=1)
        start_last_month = end_last_month.replace(day=1)

        return {
            "latest_ds": latest_ds,             
            "latest_ds_dash": latest_ds_dash,   
            "today_iso": datetime.now().strftime("%Y-%m-%d"),
            "start_7d": start_7d,
            "start_7d_dash": start_7d_dash,
            "start_30d": start_30d,
            "start_30d_dash": start_30d_dash,
            "start_this_month_dash": start_this_month.strftime("%Y-%m-%d"),
            "start_last_month_dash": start_last_month.strftime("%Y-%m-%d"),
            "end_last_month_dash": end_last_month.strftime("%Y-%m-%d")
        }