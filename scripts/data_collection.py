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
sessions = ['Q', 'R']  # Qualifying, Race

# Define subfolders for data categories
subfolders = ['laps', 'weather', 'telemetry']
folder_paths = {name: os.path.join(DATA_DIR, name) for name in subfolders}

# Create the subdirectories
for path in folder_paths.values():
    os.makedirs(path, exist_ok=True)

# Loop through events
for year in seasons:
    schedule = fastf1.get_event_schedule(year)

    for _, event in schedule.iterrows():
        round_num = event['RoundNumber']
        race_name = event['EventName'].replace(' ', '_')

        # Loop through sessions
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
                laps = session.laps
                laps.to_csv(os.path.join(folder_paths['laps'], f"{year}_{race_name}_{session_type}.csv"), index=False)

                # Weather
                weather = session.weather_data
                if weather is not None:
                    weather.to_csv(os.path.join(folder_paths['weather'], f"{year}_{race_name}_{session_type}.csv"), index=False)

                # Telemetry
                drivers = laps['Driver'].unique()  # Get drivers in the session
                all_tel_data = []  # Initialize an empty list to collect session telemetry data
                for drv in drivers:
                    drv_laps = laps.pick_drivers(drv)
                    if drv_laps.empty:
                        continue

                    # Loop over each lap for the current driver
                    for _, lap in drv_laps.iterlaps():
                        tel = lap.get_car_data().add_distance()  # Get telemetry data and add distance
                        # Add driver and lap number to the telemetry data
                        tel['Driver'] = drv
                        tel['LapNumber'] = lap['LapNumber']
                        all_tel_data.append(tel)  # Append the data for this lap to the list

                # Concatenate all the telemetry data into a single DataFrame
                final_tel_data = pd.concat(all_tel_data, ignore_index=True)

                # Save the combined telemetry data to a single CSV file
                final_tel_data.to_csv(os.path.join(folder_paths['telemetry'], f"{year}_{race_name}_{session_type}.csv"), index=False)

            except Exception as e:
                print(f"❌ Skipped {year} {race_name} {session_type}: {e}")
