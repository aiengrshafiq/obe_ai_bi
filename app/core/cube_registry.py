# app/core/cube_registry.py
import re
from typing import Dict, List, Optional, Any
from pydantic import BaseModel

# Import your cubes
import app.cubes.user_profile as user_cube
import app.cubes.transaction_detail as trans_cube
import app.cubes.trade_activity as trade_cube
import app.cubes.referral_performance as referral_cube
import app.cubes.points_system as points_cube
import app.cubes.login_history as login_cube
import app.cubes.device_log as device_cube
import app.cubes.risk_blacklist as risk_cube

class CubeMetadata(BaseModel):
    name: str
    table_name: str
    kind: str  # 'di' (Incremental) or 'df' (Snapshot)
    time_column: Optional[str]
    description: str
    ddl: str
    docs: str
    examples: List[Dict[str, str]]

class CubeRegistry:
    _registry: Dict[str, CubeMetadata] = {}
    _initialized = False

    @classmethod
    def initialize(cls):
        """
        Loads all cubes into memory and parses their metadata.
        """
        if cls._initialized:
            return

        cubes = [
            user_cube, trans_cube, trade_cube, referral_cube, 
            points_cube, login_cube, device_cube, risk_cube
        ]

        print("📦 Initializing Cube Registry...")
        
        for cube in cubes:
            table_name = cls._extract_table_name(cube.DDL)
            if not table_name:
                print(f"⚠️ Warning: Could not parse table name for cube {cube.NAME}")
                continue

            # Infer Kind: 'di' (Daily Incremental) vs 'df' (Daily Snapshot)
            # Special Rule: user_profile_360 is a Snapshot ('df') logic even without suffix
            kind = "di"
            if table_name.endswith("_df") or "user_profile_360" or "blacklist" in table_name:
                kind = "df"
            elif table_name.endswith("_di"):
                kind = "di"

            metadata = CubeMetadata(
                name=cube.NAME,
                table_name=table_name,
                kind=kind,
                time_column=getattr(cube, "TIME_COLUMN", None),
                description=cube.DESCRIPTION,
                ddl=cube.DDL,
                docs=cube.DOCS,
                examples=cube.EXAMPLES
            )

            cls._registry[table_name] = metadata
            print(f"   -> Registered: {table_name} [{kind.upper()}]")

        cls._initialized = True

    @staticmethod
    def _extract_table_name(ddl: str) -> Optional[str]:
        """Regex to find 'CREATE TABLE public.xxx'"""
        match = re.search(r"CREATE\s+TABLE\s+(?:public\.)?([a-zA-Z0-9_]+)", ddl, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    @classmethod
    def get_cube(cls, table_name: str) -> Optional[CubeMetadata]:
        return cls._registry.get(table_name)

    @classmethod
    def get_all_tables(cls) -> List[str]:
        return list(cls._registry.keys())

    @classmethod
    def is_snapshot(cls, table_name: str) -> bool:
        """Returns True if table is a Snapshot (_df), False if Incremental (_di)"""
        cube = cls.get_cube(table_name)
        return cube.kind == "df" if cube else False

# Auto-initialize on import
CubeRegistry.initialize()