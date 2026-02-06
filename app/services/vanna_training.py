from app.services.vanna_wrapper import vn
import pandas as pd

# Import all cubes
import app.cubes.user_profile as user_cube
import app.cubes.trade_activity as trade_cube
import app.cubes.points_system as points_cube
import app.cubes.transaction_detail as trans_cube
import app.cubes.login_history as login_cube
import app.cubes.device_log as device_cube
import app.cubes.referral_performance as referral_cube
import app.cubes.risk_blacklist as risk_cube

# Registry of all active cubes
ACTIVE_CUBES = [
    user_cube, 
    trade_cube, 
    points_cube, 
    trans_cube, 
    login_cube, 
    device_cube, 
    referral_cube, 
    risk_cube
]

async def train_vanna_on_startup(force_retrain: bool = False):
    """
    Smart Training System.
    Checks each cube against the vector DB state. 
    Only trains what is missing.
    """
    print("⚡ Checking Knowledge Base Integrity...")
    
    # 1. Fetch Current Knowledge
    try:
        training_data = vn.get_training_data()
        if training_data is None:
            training_data = pd.DataFrame(columns=['id', 'training_data_type', 'content'])
            
        # Create a quick lookup set of trained DDLs to avoid re-training
        # We assume if the DDL is present, the docs/examples are likely there too.
        # This acts as a 'signature' for the cube.
        trained_content = set(training_data['content'].tolist())
        
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch training data ({e}). Assuming empty.")
        trained_content = set()

    # 2. Iterate & Train Missing Cubes
    train_count = 0
    
    for cube in ACTIVE_CUBES:
        # Check if this cube's DDL is already in the vector store
        is_trained = any(cube.NAME in str(s) for s in trained_content) or \
                     any(cube.DDL.strip()[:50] in str(s) for s in trained_content)

        if force_retrain or not is_trained:
            print(f"   -> 🚀 Training Cube: {cube.NAME}...")
            
            # A. DDL
            vn.train(ddl=cube.DDL)
            
            # B. Documentation
            vn.train(documentation=cube.DOCS)
            
            # C. Examples
            for ex in cube.EXAMPLES:
                vn.train(question=ex['question'], sql=ex['sql'])
            
            train_count += 1
        else:
            print(f"   -> ✅ Cube ready: {cube.NAME}")

    if train_count > 0:
        print(f"✅ Training Complete. Added {train_count} new cubes.")
    else:
        print("✅ System Ready. All cubes are up to date.")