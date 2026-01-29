# app/services/vanna_training.py
from app.services.vanna_wrapper import vn
# Import your cubes here.
import app.cubes.user_profile as user_cube

async def train_vanna_on_startup(force_retrain: bool = False):
    """
    Checks if Vanna is trained. If empty or forced, loads data from the Cube Registry.
    param force_retrain: Set to True to overwrite/update existing training.
    """
    print("⚡ Checking Vanna Knowledge Base...")
    
    # 1. Check if Knowledge Base is empty
    existing_training = vn.get_training_data()
    
    # If we have data and are NOT forcing a retrain, skip.
    if not force_retrain and existing_training is not None and not existing_training.empty:
        print(f"✅ Vanna is already trained ({len(existing_training)} records). Ready.")
        return

    print(f"🚀 Starting Auto-Training from Cubes (Force Retrain: {force_retrain})...")

    # Optional: Clear old training data if forcing update
    if force_retrain:
        vn.remove_training_data(id='all') 

    # 2. Train User Profile Cube
    print(f"   -> Training {user_cube.NAME}...")
    vn.train(ddl=user_cube.DDL)
    vn.train(documentation=user_cube.DOCS)
    for ex in user_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])

    # 3. Future Cubes go here
    # import app.cubes.trade_cube as trade_cube
    # vn.train(ddl=trade_cube.DDL...)

    print("✅ Auto-Training Complete!")