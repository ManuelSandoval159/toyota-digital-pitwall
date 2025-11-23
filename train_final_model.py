import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
import joblib
import os

# ===================================================================
# --- TRACK CONFIGURATION ---
# Uncomment the track you want to train
# ===================================================================

# TRACK_NAME = 'COTA'
# TRACK_NAME = 'Barber'
# TRACK_NAME = 'Sebring'
# TRACK_NAME = 'Sonoma'
# TRACK_NAME = 'Indianapolis'
# TRACK_NAME = 'RoadAmerica'
TRACK_NAME = 'VIR'

# ===================================================================

print(f"Starting Master Model training for: {TRACK_NAME}")

# --- 1. Load BOTH datasets for this track ---
R1_PATH = f'processed_data/{TRACK_NAME}/R1_processed.parquet'
R2_PATH = f'processed_data/{TRACK_NAME}/R2_processed.parquet'

try:
    df_r1 = pd.read_parquet(R1_PATH)
    df_r2 = pd.read_parquet(R2_PATH)
except Exception as e:
    print(f"Error reading parquet files for {TRACK_NAME}: {e}. SKIPPING.")
    sys.exit()

# --- 2. Combine data ---
df_master = pd.concat([df_r1, df_r2])
print(f"Combined data: {len(df_master)} total laps.")

# --- 3. SMART LOGIC: Feature Detection ---
target = 'LAP_DELTA'
features = [] 
features.append('Laps_on_this_Tireset') # Base feature

# Check for valid Temperature data
# (Detects issues like Barber where temp is 0)
if 'TRACK_TEMP' in df_master.columns and df_master['TRACK_TEMP'].mean() > 5: # Use 5°C as threshold
    print("TRACK_TEMP data found! Adding to model.")
    features.append('TRACK_TEMP')
else:
    print("WARNING: No valid TRACK_TEMP data found (e.g. Barber). Switching to simplified model.")

# Check for valid Aggression data
if 'avg_aggressiveness' in df_master.columns and not df_master['avg_aggressiveness'].isna().all():
    print("AGGRESSION data found! Adding to model.")
    features.append('avg_aggressiveness')
else:
    print("WARNING: No AGGRESSION data found.")

print(f"Training model with {len(features)} features: {features}")

# --- 4. Clean master data ---
df_master = df_master.dropna(subset=features + [target])
if df_master.empty:
    print("Error: Not enough data after cleaning. SKIPPING.")
    sys.exit()

X = df_master[features]
y = df_master[target]

# --- 5. Create and Train FINAL Model ---
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
print("Training Master Model...")
model.fit(X, y)

# --- 6. Evaluate the model ---
print("Training complete! Evaluating model on all data...")
y_pred = model.predict(X)
r2 = r2_score(y, y_pred)
mae = mean_absolute_error(y, y_pred)
print(f"Results (R2): {r2:.4f}, (MAE): {mae:.4f} seconds")

# --- 7. Save FINAL Model ---
model_filename = f'processed_data/{TRACK_NAME}/FINAL_model.pkl'
joblib.dump(model, model_filename)

print(f"Success! Model saved to {model_filename}")