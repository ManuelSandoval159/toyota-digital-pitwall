import pandas as pd
import pytz
import os
import sys

# ===================================================================
# --- DATA PROCESSING CONFIGURATION ---
# Paste one of the 14 data configuration blocks here!
# ===================================================================

'''
# --- COTA (RACE 1) ---
TRACK_NAME_OUT = 'COTA'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 1/23_AnalysisEnduranceWithSections_Race1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 1/26_Weather_Race1_Anonymized.CSV'
RACE_DATE_STR = '2025-04-26' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- COTA (RACE 2) ---
TRACK_NAME_OUT = 'COTA'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 2/23_AnalysisEnduranceWithSections_ Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/circuit-of-the-americas/COTA/Race 2/26_Weather_ Race 2_Anonymized.CSV'
RACE_DATE_STR = '2025-04-27' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- SEBRING (RACE 1) ---
TRACK_NAME_OUT = 'Sebring'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/sebring/Sebring/Race 1/23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/sebring/Sebring/Race 1/26_Weather_Race 1_Anonymized.CSV'
RACE_DATE_STR = '2025-05-17' 
TRACK_TIMEZONE = 'America/New_York' 

# --- SEBRING (RACE 2) ---
TRACK_NAME_OUT = 'Sebring'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/sebring/Sebring/Race 2/23_AnalysisEnduranceWithSections_Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/sebring/Sebring/Race 2/26_Weather_Race 2_Anonymized.CSV'
RACE_DATE_STR = '2025-05-17' 
TRACK_TIMEZONE = 'America/New_York' 

# --- SONOMA (RACE 1) ---
TRACK_NAME_OUT = 'Sonoma'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 1/23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 1/26_Weather_Race 1_Anonymized.CSV'
RACE_DATE_STR = '2025-03-29' 
TRACK_TIMEZONE = 'America/Los_Angeles' 

# --- SONOMA (RACE 2) ---
TRACK_NAME_OUT = 'Sonoma'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 2/23_AnalysisEnduranceWithSections_Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/sonoma/Sonoma/Race 2/26_Weather_Race 2_Anonymized.CSV' 
RACE_DATE_STR = '2025-03-30' 
TRACK_TIMEZONE = 'America/Los_Angeles' 

# --- BARBER (RACE 1) ---
TRACK_NAME_OUT = 'Barber'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/barber-motorsports-park/barber/23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/barber-motorsports-park/barber/26_Weather_Race 1_Anonymized.CSV'
RACE_DATE_STR = '2025-09-06' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- BARBER (RACE 2) ---
TRACK_NAME_OUT = 'Barber'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/barber-motorsports-park/barber/23_AnalysisEnduranceWithSections_Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/barber-motorsports-park/barber/26_Weather_Race 2_Anonymized.CSV'
RACE_DATE_STR = '2025-09-07' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- INDIANAPOLIS (RACE 1) ---
TRACK_NAME_OUT = 'Indianapolis'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/indianapolis/indianapolis/23_AnalysisEnduranceWithSections_Race 1.CSV'
WEATHER_FILE_PATH = 'raw_data/indianapolis/indianapolis/26_Weather_Race 1.CSV'
RACE_DATE_STR = '2025-10-18' 
TRACK_TIMEZONE = 'America/Indiana/Indianapolis' 

# --- INDIANAPOLIS (RACE 2) ---
TRACK_NAME_OUT = 'Indianapolis'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/indianapolis/indianapolis/23_AnalysisEnduranceWithSections_Race 2.CSV'
WEATHER_FILE_PATH = 'raw_data/indianapolis/indianapolis/26_Weather_Race 2.CSV'
RACE_DATE_STR = '2025-10-19' 
TRACK_TIMEZONE = 'America/Indiana/Indianapolis' 

# --- ROAD AMERICA (RACE 1) ---
TRACK_NAME_OUT = 'RoadAmerica'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/road-america/Road America/Race 1/23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/road-america/Road America/Race 1/26_Weather_Race 1_Anonymized.CSV'
RACE_DATE_STR = '2025-08-16' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- ROAD AMERICA (RACE 2) ---
TRACK_NAME_OUT = 'RoadAmerica'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/road-america/Road America/Race 2/23_AnalysisEnduranceWithSections_Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/road-america/Road America/Race 2/26_Weather_Race 2_Anonymized.CSV'
RACE_DATE_STR = '2025-08-17' 
TRACK_TIMEZONE = 'America/Chicago' 

# --- VIR (RACE 1) ---
TRACK_NAME_OUT = 'VIR'
RACE_NUM_OUT = 'R1'
LAPS_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 1/23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 1/26_Weather_Race 1_Anonymized.CSV'
RACE_DATE_STR = '2025-07-19' 
TRACK_TIMEZONE = 'America/New_York' 
'''
# --- VIR (RACE 2) ---
TRACK_NAME_OUT = 'VIR'
RACE_NUM_OUT = 'R2'
LAPS_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 2/23_AnalysisEnduranceWithSections_Race 2_Anonymized.CSV'
WEATHER_FILE_PATH = 'raw_data/virginia-international-raceway/VIR/Race 2/26_Weather_Race 2_Anonymized.CSV'
RACE_DATE_STR = '2025-07-20' 
TRACK_TIMEZONE = 'America/New_York'


# ===================================================================
# --- END OF CONFIGURATION ---
# ===================================================================

# Define input/output paths
OUTPUT_FILE = f'processed_data/{TRACK_NAME_OUT}/{RACE_NUM_OUT}_processed.parquet'
TELEMETRY_AGG_FILE = f'processed_data/{TRACK_NAME_OUT}/{RACE_NUM_OUT}_telemetry_agg.parquet'


def clean_col_names(df):
    df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
    return df

def convert_time_to_seconds(time_val):
    if pd.isna(time_val): return None
    time_str = str(time_val).strip()
    if time_str == '': return None
    try:
        if ':' in time_str:
            parts = time_str.split(':'); minutes = int(parts[0]); seconds = float(parts[1]); return (minutes * 60) + seconds
        else:
            milliseconds = float(time_val); return milliseconds / 1000.0
    except Exception: return None

# --- START PROCESSING ---
print(f"Starting data processing (v13 - Sectors) for: {TRACK_NAME_OUT} - {RACE_NUM_OUT}")

# --- 1. Load Weather Data ---
try:
    df_weather = pd.read_csv(WEATHER_FILE_PATH, sep=';')
except Exception as e:
    print(f"Error reading weather file: {e}"); sys.exit()
df_weather = clean_col_names(df_weather)
df_weather['datetime'] = pd.to_datetime(df_weather['TIME_UTC_STR'], errors='coerce')
df_weather['datetime'] = df_weather['datetime'].dt.tz_localize('UTC').dt.tz_convert(TRACK_TIMEZONE)
df_weather = df_weather.sort_values(by='datetime')

# --- 2. Load Lap Data ---
try:
    df_laps = pd.read_csv(LAPS_FILE_PATH, sep=';')
except Exception as e:
    print(f"Error reading laps file: {e}"); sys.exit()
df_laps = clean_col_names(df_laps)

# --- 3. Calculate Ideal Times (Best Lap & Best Sectors) ---
print("Calculating ideal times (lap and sectors)...")
df_laps_gf = df_laps[df_laps['FLAG_AT_FL'] == 'GF'].copy()
df_laps_gf['LAP_TIME_SEC'] = df_laps_gf['LAP_TIME'].apply(convert_time_to_seconds)
# Convert sector times
df_laps_gf['S1_SEC'] = df_laps_gf['S1'].apply(convert_time_to_seconds)
df_laps_gf['S2_SEC'] = df_laps_gf['S2'].apply(convert_time_to_seconds)
df_laps_gf['S3_SEC'] = df_laps_gf['S3'].apply(convert_time_to_seconds)

# Calculate best times per driver
best_lap = df_laps_gf.groupby('NUMBER')['LAP_TIME_SEC'].min().reset_index().rename(columns={'LAP_TIME_SEC': 'BEST_LAP_TIME'})
best_s1 = df_laps_gf.groupby('NUMBER')['S1_SEC'].min().reset_index().rename(columns={'S1_SEC': 'BEST_S1'})
best_s2 = df_laps_gf.groupby('NUMBER')['S2_SEC'].min().reset_index().rename(columns={'S2_SEC': 'BEST_S2'})
best_s3 = df_laps_gf.groupby('NUMBER')['S3_SEC'].min().reset_index().rename(columns={'S3_SEC': 'BEST_S3'})

# --- 4. Merge Weather and Laps ---
print("Merging laps and weather data...")
df_laps['datetime_str'] = RACE_DATE_STR + ' ' + df_laps['HOUR']
df_laps['datetime'] = pd.to_datetime(df_laps['datetime_str'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
df_laps['datetime'] = df_laps['datetime'].dt.tz_localize(TRACK_TIMEZONE, ambiguous='infer')
df_laps = df_laps.sort_values(by='datetime')
df = pd.merge_asof(df_laps, df_weather[['datetime', 'AIR_TEMP', 'TRACK_TEMP', 'HUMIDITY', 'RAIN']], on='datetime')
df[['AIR_TEMP', 'TRACK_TEMP', 'HUMIDITY', 'RAIN']] = df[['AIR_TEMP', 'TRACK_TEMP', 'HUMIDITY', 'RAIN']].ffill()
df[['TRACK_TEMP']] = df[['TRACK_TEMP']].fillna(0) # Handle missing data (e.g. Barber)

# --- 5. Create "Stints" and "Deltas" ---
print("Cleaning data and creating stints/deltas...")
# Merge best times
df = pd.merge(df, best_lap, on='NUMBER', how='left')
df = pd.merge(df, best_s1, on='NUMBER', how='left')
df = pd.merge(df, best_s2, on='NUMBER', how='left')
df = pd.merge(df, best_s3, on='NUMBER', how='left')

# Convert times
df['LAP_TIME_SEC'] = df['LAP_TIME'].apply(convert_time_to_seconds)
df['S1_SEC'] = df['S1'].apply(convert_time_to_seconds)
df['S2_SEC'] = df['S2'].apply(convert_time_to_seconds)
df['S3_SEC'] = df['S3'].apply(convert_time_to_seconds)

# Calculate deltas
df['LAP_DELTA'] = df['LAP_TIME_SEC'] - df['BEST_LAP_TIME']
df['S1_DELTA'] = df['S1_SEC'] - df['BEST_S1']
df['S2_DELTA'] = df['S2_SEC'] - df['BEST_S2']
df['S3_DELTA'] = df['S3_SEC'] - df['BEST_S3']

# Filter laps
df = df[df['FLAG_AT_FL'] == 'GF'].copy()
df = df.dropna(subset=['LAP_TIME_SEC', 'LAP_DELTA', 'S1_DELTA', 'S2_DELTA', 'S3_DELTA', 'BEST_LAP_TIME', 'TRACK_TEMP'])
df = df[(df['LAP_DELTA'] >= 0) & (df['LAP_DELTA'] < 20)].copy() 

# Stint logic
df['CROSSING_FINISH_LINE_IN_PIT'] = pd.to_numeric(df['CROSSING_FINISH_LINE_IN_PIT'], errors='coerce').fillna(0).astype(int)
df = df.sort_values(by=['NUMBER', 'LAP_NUMBER'])
df['NEW_STINT_START'] = df.groupby('NUMBER')['CROSSING_FINISH_LINE_IN_PIT'].shift(1).fillna(0)
df['STINT_ID'] = df.groupby('NUMBER')['NEW_STINT_START'].cumsum().astype(int)
df['Laps_on_this_Tireset'] = df.groupby(['NUMBER', 'STINT_ID']).cumcount() + 1
df['NUMBER'] = df['NUMBER'].astype(str)

# --- 6. Merge with Telemetry (Aggression) Data ---
print("Merging telemetry data (aggression metric)...")
try:
    df_agg = pd.read_parquet(TELEMETRY_AGG_FILE)
    df_agg['NUMBER'] = df_agg['NUMBER'].astype(str)
    df_agg['LAP_NUMBER'] = df_agg['LAP_NUMBER'].astype(int)
    df_final = pd.merge(df, df_agg, on=['NUMBER', 'LAP_NUMBER'], how='left')
except FileNotFoundError:
    print(f"Warning: {TELEMETRY_AGG_FILE} not found. Aggression metric will be omitted.")
    df_final = df.copy()
    df_final['avg_aggressiveness'] = pd.NA

# --- 7. Save Output ---
print("Saving final dataset...")
columns_to_keep = [
    'NUMBER', 'LAP_NUMBER', 'LAP_TIME', 'LAP_TIME_SEC',
    'STINT_ID', 'Laps_on_this_Tireset',
    'S1_SEC', 'S2_SEC', 'S3_SEC', 'TOP_SPEED',
    'TRACK_TEMP', 'AIR_TEMP', 'RAIN',
    'BEST_LAP_TIME', 'LAP_DELTA',
    'S1_DELTA', 'S2_DELTA', 'S3_DELTA', 
    'avg_aggressiveness'
]
cols_exist = [col for col in columns_to_keep if col in df_final.columns]
df_final = df_final[cols_exist].copy()

os.makedirs(f'processed_data/{TRACK_NAME_OUT}', exist_ok=True)
df_final.to_parquet(OUTPUT_FILE, index=False)
print(f"Success! Data saved to {OUTPUT_FILE}")