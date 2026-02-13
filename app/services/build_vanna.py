# app/services/build_vanna.py
import sys
import os

# Ensure the app directory is in the path so imports work
sys.path.append(os.getcwd())

from app.services.vanna_wrapper import vn
from app.core.cube_registry import CubeRegistry

def build_knowledge_base():
    """
    Runs during Docker Build.
    Populates the local ChromaDB (vanna_storage) with all Cube definitions.
    """
    print("🏗️ BUILD-TIME TRAINING: Starting...")
    
    # 1. Get all registered cubes
    # Registry is auto-initialized on import
    tables = CubeRegistry.get_all_tables()
    
    if not tables:
        print("❌ Error: No cubes found in Registry.")
        sys.exit(1)
        
    print(f"📦 Found {len(tables)} cubes to train.")
    
    # 2. Train Loop
    count = 0
    for table_name in tables:
        cube = CubeRegistry.get_cube(table_name)
        print(f"   -> Training: {cube.name} ({cube.kind})...")
        
        # A. Train DDL (Schema)
        vn.train(ddl=cube.ddl)
        
        # B. Train Docs (Business Logic)
        vn.train(documentation=cube.docs)
        
        # C. Train Examples (SQL Patterns)
        for ex in cube.examples:
            vn.train(question=ex['question'], sql=ex['sql'])
            
        count += 1

    print(f"✅ BUILD COMPLETE: {count} cubes baked into 'vanna_storage'.")

if __name__ == "__main__":
    build_knowledge_base()