import streamlit as st
import pandas as pd
import plotly.express as px
import joblib
import os

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Toyota GR Digital Pit Wall", page_icon="🏁")

# --- Calibration Constants ---
# Time lost in pit lane (excluding tire change time)
PIT_LANE_TIMES = {
    'Barber': 34.0, 'COTA': 36.0, 'Indianapolis': 63.0, 'RoadAmerica': 52.0,
    'Sebring': 39.0, 'Sonoma': 45.0, 'VIR': 25.0
}
# Estimated time lost under Full Course Yellow (FCY)
GLOBAL_YELLOW_PIT_COST = 20.0 

# --- Loading Functions (Cached) ---
@st.cache_resource
def load_model(track_name):
    model_path = f'processed_data/{track_name}/FINAL_model.pkl'
    if not os.path.exists(model_path): return None
    try: return joblib.load(model_path)
    except Exception: return None

@st.cache_data
def load_data(track_name):
    try:
        df_r1 = pd.read_parquet(f'processed_data/{track_name}/R1_processed.parquet')
        df_r2 = pd.read_parquet(f'processed_data/{track_name}/R2_processed.parquet')
        common_cols = list(set(df_r1.columns) & set(df_r2.columns))
        return pd.concat([df_r1[common_cols], df_r2[common_cols]])
    except FileNotFoundError: return None

# --- Simulation Logic Functions ---

def get_prediction_dataframe(model, laps, temp, aggressiveness):
    """Creates the input DataFrame for the model with correct feature names."""
    features = model.feature_names_in_
    data = {}
    if 'Laps_on_this_Tireset' in features: data['Laps_on_this_Tireset'] = [laps]
    if 'TRACK_TEMP' in features: data['TRACK_TEMP'] = [temp]
    if 'avg_aggressiveness' in features: data['avg_aggressiveness'] = [aggressiveness]
    return pd.DataFrame(data, columns=features)

def get_prediction(model, laps, temp, aggressiveness):
    """Returns the predicted LAP_DELTA for a specific lap."""
    pred_df = get_prediction_dataframe(model, laps, temp, aggressiveness)
    return model.predict(pred_df)[0]

def simulate_race_remaining(model, current_tire_laps, laps_remaining, track_temp, driver_aggression, pit_now, green_pit_lap, green_pit_cost, yellow_pit_cost):
    """
    Simulates the total time lost (delta) for the remainder of the race.
    """
    total_delta = 0.0
    current_laps = current_tire_laps
    
    # Logic: If we pit now, we pit on relative lap 1. 
    # If we stay out, we pit on the target green flag lap.
    target_pit_lap = 1 if pit_now else green_pit_lap
    actual_pit_cost = yellow_pit_cost if pit_now else green_pit_cost

    for i in range(1, laps_remaining + 1):
        if i == target_pit_lap:
            total_delta += actual_pit_cost
            current_laps = 1 # Reset tires
        else:
            current_laps += 1 # Tires get older
        
        # Add the degradation penalty for this lap
        total_delta += get_prediction(model, current_laps, track_temp, driver_aggression)
        
    return total_delta

def simulate_pit_battle(model, initial_tire_laps, track_temp, driver_aggression, pit_lap, green_pit_cost, cycle_laps=3):
    """
    Simulates a 3-lap battle (Undercut/Overcut) to calculate total time.
    """
    total_time = 0.0
    current_laps = initial_tire_laps
    
    for i in range(1, cycle_laps + 1):
        if i == pit_lap:
            total_time += green_pit_cost
            current_laps = 1 # Tires reset
        else:
            current_laps += 1
            
        total_time += get_prediction(model, current_laps, track_temp, driver_aggression)
        
    return total_time

# ==============================================================================
# USER INTERFACE (FRONTEND)
# ==============================================================================

# --- SIDEBAR - Global Config ---
with st.sidebar:
    if os.path.exists("Toyota_Gazoo_Racing_WRT_Logo.png"):
        st.image("Toyota_Gazoo_Racing_WRT_Logo.png", width=200)
    else:
        st.warning("Logo not found")
        
    st.title("Configuration")
    
    processed_tracks = [d for d in os.listdir('processed_data') if os.path.isdir(os.path.join('processed_data', d)) and 'cota_anterior' not in d]
    processed_tracks.sort()
    
    if not processed_tracks:
        st.error("⚠️ No processed data found.")
        st.stop()
        
    selected_track = st.selectbox("📍 Select Circuit:", processed_tracks)
    
    if selected_track:
        pit_loss = PIT_LANE_TIMES.get(selected_track, 36.0)
        st.markdown("---")
        st.markdown("### ⏱️ Track Calibration")
        st.metric("Pit Lane Loss (Green)", f"{pit_loss} sec")
        st.caption(f"Based on official track maps for {selected_track}.")

# --- MAIN BODY ---
st.title("🏁 Toyota GR Digital Pit Wall")
st.markdown(f"**Active Circuit:** {selected_track}")

if selected_track:
    model = load_model(selected_track)
    df = load_data(selected_track)

    if model is None or df is None:
        st.error("Error loading model or data. Please verify processing.")
    else:
        MODEL_FEATURES = model.feature_names_in_
        
        # --- MAIN TABS ---
        tab_sim, tab_analisis, tab_info = st.tabs(["🏁 Strategy Simulator", "📉 Historical Analysis", "ℹ️ Methodology"])

        # ----------------------------------------------------------------------
        # TAB 1: SIMULATOR
        # ----------------------------------------------------------------------
        with tab_sim:
            st.subheader("1. Current Conditions")
            
            col_input1, col_input2, col_input3 = st.columns(3)
            
            with col_input1:
                st.markdown("**Tire Status**")
                sim_laps = st.slider("Laps on Tires (Tire Age)", 1, 25, 10, key="main_laps")
            
            with col_input2:
                st.markdown("**Weather (Track)**")
                if 'TRACK_TEMP' in MODEL_FEATURES:
                    min_t, max_t = int(df['TRACK_TEMP'].min()), int(df['TRACK_TEMP'].max())
                    if min_t >= max_t: min_t, max_t = 0, 50
                    sim_temp = st.slider("Track Temp (°C)", min_t, max_t, (min_t + max_t) // 2, key="main_temp")
                else:
                    st.warning("⚠️ Weather data unavailable")
                    sim_temp = 0
            
            with col_input3:
                st.markdown("**Driving Style**")
                if 'avg_aggressiveness' in MODEL_FEATURES:
                    min_agg, max_agg = float(df['avg_aggressiveness'].min()), float(df['avg_aggressiveness'].max())
                    if min_agg >= max_agg: min_agg, max_agg = 0.5, 1.5
                    sim_agg = st.slider("Aggressiveness (Lat-G)", min_agg, max_agg, (min_agg + max_agg) / 2, 0.01, key="main_agg")
                else:
                    st.warning("⚠️ Telemetry data unavailable")
                    sim_agg = 0

            # Immediate Prediction
            delta = get_prediction(model, sim_laps + 1, sim_temp, sim_agg)
            st.markdown("---")
            col_metric1, col_metric2 = st.columns([1, 3])
            with col_metric1:
                st.metric("Predicted Delta (Next Lap)", f"+ {delta:.2f} s", delta_color="inverse")
            with col_metric2:
                st.info(f"💡 **Insight:** With **{sim_laps} laps** of wear and these conditions, the driver will lose **{delta:.2f} seconds** compared to their optimal lap time.")

            # 2. Decision Tools
            st.markdown("---")
            st.subheader("2. Strategic Decision Tools")
            
            col_tool1, col_tool2 = st.columns(2)

            # --- Tool A: Caution ---
            with col_tool1:
                with st.container(border=True):
                    st.markdown("### Caution Flag Calculator")
                    st.caption("Should we pit under the Safety Car?")
                    
                    caution_laps = st.number_input("Laps Remaining", 1, 40, 15)
                    
                    # Dynamic default value fix
                    default_pit = 10 if caution_laps >= 10 else caution_laps
                    
                    pit_target = st.number_input("If we stay out, pit in lap #:", 1, caution_laps, default_pit)
                    
                    if st.button("Simulate Caution Strategy", use_container_width=True):
                        track_pit_loss = PIT_LANE_TIMES.get(selected_track, 36.0)
                        
                        # Calculate both scenarios
                        t_now = simulate_race_remaining(model, sim_laps, caution_laps, sim_temp, sim_agg, True, pit_target, track_pit_loss, GLOBAL_YELLOW_PIT_COST)
                        t_later = simulate_race_remaining(model, sim_laps, caution_laps, sim_temp, sim_agg, False, pit_target, track_pit_loss, GLOBAL_YELLOW_PIT_COST)
                        
                        diff = t_later - t_now
                        if diff > 0:
                            st.success(f"**VERDICT: PIT NOW!** You save **{diff:.2f}s**")
                        else:
                            st.error(f"**VERDICT: STAY OUT.** Pitting costs **{abs(diff):.2f}s**")

            # --- Tool B: Undercut ---
            with col_tool2:
                with st.container(border=True):
                    st.markdown("### Battle Simulator (Undercut)")
                    st.caption("Can we gain track position by pitting?")
                    
                    rival_laps = st.slider("Rival's Tire Age (Laps)", 1, 25, 12)
                    gap = st.number_input("Gap to Rival (seconds)", 0.0, 10.0, 2.0, step=0.1)
                    
                    col_btn_u, col_btn_o = st.columns(2)
                    track_pit_loss = PIT_LANE_TIMES.get(selected_track, 36.0)

                    if col_btn_u.button("Simulate Undercut", use_container_width=True):
                        my_time = simulate_pit_battle(model, sim_laps, sim_temp, sim_agg, 1, track_pit_loss)
                        rival_time = simulate_pit_battle(model, rival_laps, sim_temp, sim_agg, 2, track_pit_loss)
                        net_gain = rival_time - my_time
                        
                        if net_gain > gap:
                            st.success(f"**SUCCESS!** You gain position by **{net_gain - gap:.2f}s**")
                        else:
                            st.error(f"**FAIL.** You miss by **{gap - net_gain:.2f}s**")
                            
                    if col_btn_o.button("Simulate Overcut", use_container_width=True):
                        my_time = simulate_pit_battle(model, sim_laps, sim_temp, sim_agg, 2, track_pit_loss)
                        rival_time = simulate_pit_battle(model, rival_laps, sim_temp, sim_agg, 1, track_pit_loss)
                        net_gain = rival_time - my_time
                        
                        st.metric("Net Gain", f"{net_gain:.2f}s")
                        if net_gain > gap:
                            st.success(f"**SUCCESS!** You gain position.")
                        else:
                            st.error(f"**FAIL.** Not enough pace.")

        # ----------------------------------------------------------------------
        # TAB 2: HISTORICAL ANALYSIS
        # ----------------------------------------------------------------------
        with tab_analisis:
            st.subheader(f"📊 Real Data Analysis: {selected_track}")
            
            color_var = None
            if 'avg_aggressiveness' in MODEL_FEATURES: color_var = 'avg_aggressiveness'
            elif 'TRACK_TEMP' in MODEL_FEATURES: color_var = 'TRACK_TEMP'
            
            fig_main = px.scatter(
                df, x='Laps_on_this_Tireset', y='LAP_DELTA', color=color_var,
                title=f'Tire Degradation Overview ({selected_track})',
                labels={
                    'Laps_on_this_Tireset': 'Tire Age (Laps)', 
                    'LAP_DELTA': 'Time Loss (s)', 
                    'avg_aggressiveness': 'Driver Aggression (Lat-G)', 
                    'TRACK_TEMP': 'Track Temp (°C)'
                }
            )
            st.plotly_chart(fig_main, use_container_width=True)
            
            # Sector Analysis
            if 'S1_DELTA' in df.columns:
                st.markdown("---")
                st.subheader("📍 Sector Analysis")
                st.caption("Identify exactly where on the track the driver is losing time due to wear.")
                
                drivers = sorted(df['NUMBER'].unique())
                sel_driver = st.selectbox("Filter by Driver:", drivers, index=0)
                
                df_driver = df[df['NUMBER'] == sel_driver]
                
                sc1, sc2, sc3 = st.columns(3)
                with sc1:
                    st.markdown("**Sector 1**")
                    st.plotly_chart(px.scatter(
                        df_driver, x='Laps_on_this_Tireset', y='S1_DELTA', trendline='ols',
                        labels={'Laps_on_this_Tireset': 'Laps', 'S1_DELTA': 'Delta S1 (s)'}
                    ), use_container_width=True)
                with sc2:
                    st.markdown("**Sector 2**")
                    st.plotly_chart(px.scatter(
                        df_driver, x='Laps_on_this_Tireset', y='S2_DELTA', trendline='ols',
                        labels={'Laps_on_this_Tireset': 'Laps', 'S2_DELTA': 'Delta S2 (s)'}
                    ), use_container_width=True)
                with sc3:
                    st.markdown("**Sector 3**")
                    st.plotly_chart(px.scatter(
                        df_driver, x='Laps_on_this_Tireset', y='S3_DELTA', trendline='ols',
                        labels={'Laps_on_this_Tireset': 'Laps', 'S3_DELTA': 'Delta S3 (s)'}
                    ), use_container_width=True)

        # ----------------------------------------------------------------------
        # TAB 3: METHODOLOGY
        # ----------------------------------------------------------------------
        with tab_info:
            st.header("Project Methodology")
            st.markdown("""
            ### Objective
            To develop a **Real-Time Analytics Tool** that empowers race engineers to make data-driven strategic decisions rather than relying on intuition.

            ### The "AI Brain"
            At the core of this simulator is a "Master AI Brain" (a `RandomForestRegressor` model) trained on historical data from Race 1 and Race 2.
            
            **Target Variable:** `LAP_DELTA` (Current Lap Time - Theoretical Best Lap).
            *We predict the delta rather than raw lap times to isolate tire degradation from traffic noise.*
            
            **Model Features (Predictive Variables):**
            1.  **Tire Wear (`Laps`):** The primary driver of degradation.
            2.  **Weather (`TRACK_TEMP`):** Critical environmental factor. *Note: The system includes an intelligent fallback mechanism. If it detects corrupt weather data (e.g., in the Barber dataset), it automatically simplifies the model to prevent crashes.*
            3.  **Aggression (`accy_can`):** We processed over **20GB of raw telemetry** to extract the average Lateral-G force per lap, quantifying the driver's style and its impact on tire life.

            ### Calibration
            The simulator is calibrated using exact Pit Lane transit times extracted from official track documentation:
            * **COTA:** 36s
            * **Sebring:** 39s
            * **Indianapolis:** 63s
            * **Barber:** 34s
            * **Road America:** 52s
            * **Sonoma:** 45s
            * **Virginia:** 25s
            """)