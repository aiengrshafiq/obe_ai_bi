# app/services/vanna_training.py
from app.services.vanna_wrapper import vn
# Import your cubes here. In the future, you can dynamically import all files in the directory.
import app.cubes.user_profile as user_cube

async def train_vanna_on_startup():
    """
    Checks if Vanna is trained. If empty, loads data from the Cube Registry.
    """
    print("⚡ Checking Vanna Knowledge Base...")
    
    # 1. Check if Knowledge Base is empty
    existing_training = vn.get_training_data()
    if existing_training is not None and not existing_training.empty:
        print(f"✅ Vanna is already trained ({len(existing_training)} records). Ready.")
        return

    print("🚀 Knowledge Base empty. Starting Auto-Training from Cubes...")

    # 2. Train User Profile Cube
    print(f"   -> Training {user_cube.NAME}...")
    vn.train(ddl=user_cube.DDL)
    vn.train(documentation=user_cube.DOCS)
    for ex in user_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])

    # 3. (Future) Train Trade Cube
    # import app.cubes.trade_cube as trade_cube
    # vn.train(ddl=trade_cube.DDL...)

    print("✅ Auto-Training Complete!")