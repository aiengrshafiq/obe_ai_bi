from app.services.vanna_wrapper import vn
import app.cubes.user_profile as user_cube
import app.cubes.trade_activity as trade_cube
import app.cubes.points_system as points_cube
import app.cubes.transaction_detail as trans_cube
import app.cubes.login_history as login_cube
import app.cubes.device_log as device_cube
import app.cubes.referral_performance as referral_cube

async def train_vanna_on_startup(force_retrain: bool = False):
    print("⚡ Checking Vanna Knowledge Base...")
    
    existing_training = vn.get_training_data()
    
    # Smart Check: If we have data and NO force retrain, check if Trade Cube is missing
    if not force_retrain and existing_training is not None and not existing_training.empty:
        # Check if 'dws_all_trades_di' is already known. If not, we continue to training.
        # We check the 'content' column of the training dataframe
        if 'dws_all_trades_di' in existing_training['content'].to_string():
            print(f"✅ Vanna is already trained ({len(existing_training)} records). Ready.")
            return
        else:
             print("⚠️ Trade Cube missing. Starting Incremental Training...")

    print(f"🚀 Starting Auto-Training (Force: {force_retrain})...")

    # 1. Train User Cube
    print(f"   -> Training {user_cube.NAME}...")
    vn.train(ddl=user_cube.DDL)
    vn.train(documentation=user_cube.DOCS)
    for ex in user_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])

    # 2. Train Trade Cube (NEW)
    print(f"   -> Training {trade_cube.NAME}...")
    vn.train(ddl=trade_cube.DDL)
    vn.train(documentation=trade_cube.DOCS)
    for ex in trade_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])

    # --- 3. TRAIN POINTS CUBE (NEW) ---
    print(f"   -> Training {points_cube.NAME}...")
    vn.train(ddl=points_cube.DDL)
    vn.train(documentation=points_cube.DOCS)
    for ex in points_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])
    # ----------------------------------

    # --- 4. TRAIN TRANSACTION CUBE (NEW) ---
    print(f"   -> Training {trans_cube.NAME}...")
    vn.train(ddl=trans_cube.DDL)
    vn.train(documentation=trans_cube.DOCS)
    for ex in trans_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])
    # ---------------------------------------

    # --- 5. TRAIN LOGIN HISTORY CUBE (NEW) ---
    print(f"   -> Training {login_cube.NAME}...")
    vn.train(ddl=login_cube.DDL)
    vn.train(documentation=login_cube.DOCS)
    for ex in login_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])
    # -----------------------------------------

    # --- 6. TRAIN DEVICE LOG CUBE (NEW) ---
    print(f"   -> Training {device_cube.NAME}...")
    vn.train(ddl=device_cube.DDL)
    vn.train(documentation=device_cube.DOCS)
    for ex in device_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])
    # --------------------------------------

    # --- 7. TRAIN REFERRAL CUBE (NEW) ---
    print(f"   -> Training {referral_cube.NAME}...")
    vn.train(ddl=referral_cube.DDL)
    vn.train(documentation=referral_cube.DOCS)
    for ex in referral_cube.EXAMPLES:
        vn.train(question=ex['question'], sql=ex['sql'])
    # ------------------------------------
    


    print("✅ Auto-Training Complete!")