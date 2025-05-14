import fastf1
import os
import pandas as pd
import time
from constants import DATA_DIR

# Create cache directory if it doesn't exist
cache_dir = os.path.join(DATA_DIR, "f1_cache")
os.makedirs(cache_dir, exist_ok=True)

# Enable FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

# Seasons and sessions to collect
seasons = [2022, 2023]
sessions = ['R']  # Race

# Initialize accumulators
all_laps = []
all_weather = []
all_telemetry = []

# Loop through events
for year in seasons:
    schedule = fastf1.get_event_schedule(year)

    for _, event in schedule.iterrows():
        round_num = event['RoundNumber']
        race_name = event['EventName']

        for session_type in sessions:
            try:
                print(f"Fetching {year} - {race_name} - {session_type}")
                session = fastf1.get_session(year, round_num, session_type)

                for attempt in range(3):
                    try:
                        session.load()
                        if not session.laps.empty:
                            break
                        time.sleep(2)
                    except Exception:
                        time.sleep(2)
                else:
                    raise ValueError("Failed to load session after multiple attempts.")

                # Laps
                laps = session.laps.copy()
                laps['Year'] = year
                laps['EventName'] = race_name
                laps['Session'] = session_type
                all_laps.append(laps)

                # Weather
                weather = session.weather_data
                if weather is not None:
                    weather = weather.copy()
                    weather['Year'] = year
                    weather['EventName'] = race_name
                    weather['Session'] = session_type
                    all_weather.append(weather)

                # Telemetry
                drivers = laps['Driver'].unique()
                for drv in drivers:
                    drv_laps = laps[laps['Driver'] == drv]
                    for _, lap in drv_laps.iterlaps():
                        tel = lap.get_car_data().add_distance()
                        tel['Driver'] = drv
                        tel['LapNumber'] = lap['LapNumber']
                        tel['Year'] = year
                        tel['EventName'] = race_name
                        tel['Session'] = session_type
                        all_telemetry.append(tel)

            except Exception as e:
                print(f"❌ Skipped {year} {race_name} {session_type}: {e}")

# Save all data after loops
if all_laps:
    pd.concat(all_laps, ignore_index=True).to_csv(os.path.join(DATA_DIR, "laps.csv"), index=False)

if all_weather:
    pd.concat(all_weather, ignore_index=True).to_csv(os.path.join(DATA_DIR, "weather.csv"), index=False)

if all_telemetry:
    pd.concat(all_telemetry, ignore_index=True).to_csv(os.path.join(DATA_DIR, "telemetry.csv"), index=False)

