import pandas as pd
import pytz
import os
import sys
import json
import re 

# ===================================================================
# --- TELEMETRY PROCESSING CONFIGURATION ---
# Paste one of the 14 telemetry configuration blocks here!
# ===================================================================

'''
# --- COTA (RACE 1) ---
TRACK_NAME_OUT = 'COTA'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 1/R1_cota_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- COTA (RACE 2) ---
TRACK_NAME_OUT = 'COTA'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 2/R2_cota_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- BARBER (RACE 1) ---
TRACK_NAME_OUT = 'Barber'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/barber-motorsports-park/barber/R1_barber_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- BARBER (RACE 2) ---
TRACK_NAME_OUT = 'Barber'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/barber-motorsports-park/barber/R2_barber_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- INDIANAPOLIS (RACE 1) ---
TRACK_NAME_OUT = 'Indianapolis'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/indianapolis/indianapolis/R1_indianapolis_motor_speedway_telemetry.csv'
TRACK_TIMEZONE = 'America/Indiana/Indianapolis'
TELEMETRY_FORMAT = 'LONG' 

# --- INDIANAPOLIS (RACE 2) ---
TRACK_NAME_OUT = 'Indianapolis'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/indianapolis/indianapolis/R2_indianapolis_motor_speedway_telemetry.csv'
TRACK_TIMEZONE = 'America/Indiana/Indianapolis'
TELEMETRY_FORMAT = 'LONG' 

# --- ROAD AMERICA (RACE 1) ---
TRACK_NAME_OUT = 'RoadAmerica'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/road-america/Road America/Race 1/R1_road_america_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- ROAD AMERICA (RACE 2) ---
TRACK_NAME_OUT = 'RoadAmerica'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/road-america/Road America/Race 2/R2_road_america_telemetry_data.csv'
TRACK_TIMEZONE = 'America/Chicago'
TELEMETRY_FORMAT = 'LONG' 

# --- SEBRING (RACE 1) ---
TRACK_NAME_OUT = 'Sebring'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/sebring/Sebring/Race 1/sebring_telemetry_R1.csv'
TRACK_TIMEZONE = 'America/New_York'
TELEMETRY_FORMAT = 'LONG' 

# --- SEBRING (RACE 2) ---
TRACK_NAME_OUT = 'Sebring'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/sebring/Sebring/Race 2/sebring_telemetry_R2.csv'
TRACK_TIMEZONE = 'America/New_York'
TELEMETRY_FORMAT = 'JSON' # <-- The only JSON file!

# --- SONOMA (RACE 1) ---
TRACK_NAME_OUT = 'Sonoma'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 1/sonoma_telemetry_R1.csv'
TRACK_TIMEZONE = 'America/Los_Angeles'
TELEMETRY_FORMAT = 'LONG' 

# --- SONOMA (RACE 2) ---
TRACK_NAME_OUT = 'Sonoma'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 2/sonoma_telemetry_R2.csv'
TRACK_TIMEZONE = 'America/Los_Angeles'
TELEMETRY_FORMAT = 'LONG' 

# --- VIR (RACE 1) ---
TRACK_NAME_OUT = 'VIR'
RACE_NUM_OUT = 'R1'
TELEMETRY_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 1/R1_vir_telemetry_data.csv'
TRACK_TIMEZONE = 'America/New_York'
TELEMETRY_FORMAT = 'LONG' 
'''
# --- VIR (RACE 2) ---
TRACK_NAME_OUT = 'VIR'
RACE_NUM_OUT = 'R2'
TELEMETRY_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 2/R2_vir_telemetry_data.csv'
TRACK_TIMEZONE = 'America/New_York'
TELEMETRY_FORMAT = 'LONG'

# ===================================================================
# --- END OF CONFIGURATION ---
# ===================================================================

# Define output path
OUTPUT_FILE = f'processed_data/{TRACK_NAME_OUT}/{RACE_NUM_OUT}_telemetry_agg.parquet'
CHUNKSIZE = 3_000_000 

def clean_col_names(df):
    df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
    return df
def clean_col_data(series):
    return series.astype(str).str.strip().str.replace('"', '', regex=False)
def extract_number_from_id(vehicle_id_str):
    """
    Extracts car number (e.g. '21') from ID (e.g. 'GR86-047-21')
    """
    try:
        vehicle_id_str = str(vehicle_id_str).strip().replace('"', '')
        # Logic: GR86-004-78 -> 78
        num_str = vehicle_id_str.split('-')[-1]
        # Handle '000' cases (e.g. GR86-002-000)
        if int(num_str) == 0 and 'vehicle_number' in globals():
             # Fallback to global vehicle_number if ID is 000
             return str(int(globals()['vehicle_number']))
        return str(int(num_str))
    except: 
        # General Fallback
        if 'vehicle_number' in globals():
            return str(int(globals()['vehicle_number']))
        return None

# --- Chunk Processors ---

def process_long_chunk(chunk_df):
    chunk_df = clean_col_names(chunk_df)
    
    # Clean data BEFORE filtering
    if 'telemetry_name' in chunk_df.columns:
         chunk_df['telemetry_name'] = clean_col_data(chunk_df['telemetry_name'])
    else:
        return None # Useless chunk
    
    chunk_df = chunk_df[chunk_df['telemetry_name'] == 'accy_can'].copy()
    if chunk_df.empty: return None

    # Clean join columns
    chunk_df['vehicle_id'] = clean_col_data(chunk_df['vehicle_id'])
    chunk_df['lap'] = pd.to_numeric(clean_col_data(chunk_df['lap']), errors='coerce')
    chunk_df['telemetry_value'] = pd.to_numeric(chunk_df['telemetry_value'], errors='coerce')
    
    # Handle 'vehicle_number' column if present (Road America style)
    if 'vehicle_number' in chunk_df.columns:
        chunk_df['NUMBER'] = clean_col_data(chunk_df['vehicle_number'])
    else:
        # Otherwise extract from ID
        chunk_df['NUMBER'] = chunk_df['vehicle_id'].apply(extract_number_from_id)

    return chunk_df[['NUMBER', 'lap', 'telemetry_value']]

def process_json_chunk(chunk_df):
    chunk_df = clean_col_names(chunk_df)
    
    def parse_json_value(json_str):
        try:
            data = json.loads(json_str)
            for item in data:
                name = item.get('name', '').strip().replace('"', '')
                if name == 'accy_can':
                    return item.get('value')
            return None
        except: return None
            
    chunk_df['telemetry_value'] = chunk_df['value'].apply(parse_json_value)
    chunk_df = chunk_df.dropna(subset=['telemetry_value'])
    if chunk_df.empty: return None

    # Clean join columns
    chunk_df['vehicle_id'] = clean_col_data(chunk_df['vehicle_id'])
    chunk_df['lap'] = pd.to_numeric(clean_col_data(chunk_df['lap']), errors='coerce')
    chunk_df['telemetry_value'] = pd.to_numeric(chunk_df['telemetry_value'], errors='coerce')
    
    # Extract car number
    chunk_df['NUMBER'] = chunk_df['vehicle_id'].apply(extract_number_from_id)

    return chunk_df[['NUMBER', 'lap', 'telemetry_value']]

# --- START TELEMETRY PROCESSING ---
print(f"Starting telemetry processing (v-FINAL-v2) for: {TRACK_NAME_OUT} - {RACE_NUM_OUT}")

print(f"Loading giant file in chunks: {TELEMETRY_FILE_PATH}")
all_agg_chunks = []
try:
    with pd.read_csv(TELEMETRY_FILE_PATH, sep=',', chunksize=CHUNKSIZE, on_bad_lines='skip') as reader:
        for i, chunk in enumerate(reader):
            print(f"  Processing chunk #{i+1}...")
            
            # Set global vehicle_number fallback if available
            if 'vehicle_number' in chunk.columns:
                globals()['vehicle_number'] = chunk['vehicle_number'].iloc[0]
            
            if TELEMETRY_FORMAT == 'LONG':
                agg_chunk = process_long_chunk(chunk)
            elif TELEMETRY_FORMAT == 'JSON':
                agg_chunk = process_json_chunk(chunk)
            else:
                print(f"Error: Unknown format '{TELEMETRY_FORMAT}'."); sys.exit()
            
            if agg_chunk is not None:
                if not agg_chunk.empty:
                    all_agg_chunks.append(agg_chunk)
                    print(f"    > Success! Found {len(agg_chunk)} 'accy_can' readings.")

except Exception as e:
    print(f"Unexpected error reading CSV: {e}"); sys.exit()

if not all_agg_chunks:
    print("Error: Could not process any telemetry data."); sys.exit()

# --- PHASE 2: Concatenate ---
print("Merging chunk results...")
df_full_agg = pd.concat(all_agg_chunks)

if df_full_agg.empty:
    print("Error: No matching 'accy_can' data found."); sys.exit()

# --- PHASE 3: Final Aggregation ---
print("Calculating average aggression per lap...")
df_full_agg = df_full_agg.dropna(subset=['NUMBER', 'lap', 'telemetry_value'])
df_full_agg['telemetry_value'] = df_full_agg['telemetry_value'].abs()
# Filter out invalid telemetry laps
df_full_agg = df_full_agg[df_full_agg['lap'] < 1000] 
    
df_final = df_full_agg.groupby(['NUMBER', 'lap'])['telemetry_value'].mean().reset_index()
df_final = df_final.rename(columns={'telemetry_value': 'avg_aggressiveness', 'lap': 'LAP_NUMBER'})

# --- PHASE 4: Save ---
os.makedirs(f'processed_data/{TRACK_NAME_OUT}', exist_ok=True)
df_final.to_parquet(OUTPUT_FILE, index=False)
print("---")
print(f"Success! Aggregated telemetry data saved to {OUTPUT_FILE}")