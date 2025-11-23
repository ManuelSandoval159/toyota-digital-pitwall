# Digital Pit Wall - Real-Time Strategy Tool

**Hackathon:** Hack the Track presented by Toyota GR
**Category:** Real-Time Analytics

## Project Overview

The Digital Pit Wall is a decision-support system designed for race engineers in the Toyota GR Cup. Unlike traditional telemetry viewers that display historical data, this application focuses on predictive analytics to assist in real-time strategic decision-making.

The core objective of this tool is to quantify tire degradation and simulate race scenarios (such as caution flags and undercuts) to optimize pit stop timing.

## Technical Approach

### 1. Predictive Modeling (Machine Learning)

The system relies on seven track-specific Random Forest Regressor models. Through data analysis, we determined that predicting raw lap times introduces significant noise due to traffic and driver variability.

Instead, the model targets **`LAP_DELTA`**: the pure time loss attributable to tire wear compared to a theoretical optimal lap.

**Model Features:**

- **Tire Age:** Cumulative laps on the current set.
- **Track Temperature:** Extracted from weather reports to account for thermal degradation.
- **Driver Aggression:** A custom metric derived from processing raw telemetry data (20GB+), calculating the average Lateral-G forces sustained per lap to quantify driving style impact on tire life.

### 2. Data Engineering and Robustness

The data pipeline is designed to handle inconsistencies found in real-world motorsport data.

- **Telemetry Alignment:** The system resolves timestamp discrepancies between ECU logs and timing loops using metadata synchronization.
- **Fault Tolerance:** During exploratory data analysis, corrupt weather data was identified in the Barber Motorsports Park dataset (reading 0.0°C). The application automatically detects feature availability for each track. If critical environmental data is missing, the system dynamically retrains and deploys a simplified model to maintain functionality without crashing.

### 3. Strategy Simulation Modules

The application translates predictions into actionable insights through two primary simulation engines:

- **Caution Flag Calculator:** Simulates the remaining race distance to calculate the net time delta between pitting under a Full Course Yellow versus a green flag stop.
- **Undercut/Overcut Simulator:** Projects the pace of the driver and a selected rival over a three-lap window to determine the probability of gaining track position via pit strategy.

**Calibration:** All simulations are calibrated using specific pit-lane transit times extracted from official track documentation (e.g., COTA: 36s, Indianapolis: 63s).

## Technology Stack

- **Language:** Python 3.10
- **Data Processing:** Pandas (ETL for race results, weather logs, and high-frequency telemetry).
- **Machine Learning:** Scikit-Learn (RandomForestRegressor).
- **Frontend:** Streamlit (Interactive dashboard interface).
- **Visualization:** Plotly.

## Installation and Usage

To run this project locally:

1.  Clone the repository.
2.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Launch the application:
    ```bash
    streamlit run app.py
    ```

## Project Structure

- `app.py`: Main entry point for the Streamlit application.
- `processed_data/`: Contains the pre-trained models (`.pkl`) and aggregated parquet files for each track.
- `process_data.py`: ETL script for cleaning and merging race and weather data.
- `process_telemetry.py`: Script for processing raw telemetry files and extracting the aggression metric.
- `train_final_model.py`: Script for training and serializing the machine learning models.
