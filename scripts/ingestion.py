# Imports
import os
import fastf1 as ff1
import pandas as pd

# Set the relative directory path to "../data"
DIRECTORY = os.path.join("..", "data")

# Check if the script is running inside a directory named "scripts"
if os.path.basename(os.getcwd()) != "scripts":
    print("Error: Incorrect loading sequence, please run this script in its original folder.")
    sys.exit(1)
    
# Helper creates the folders data and cache if they do not exist, otherwise does nothing.
os.makedirs(os.path.join(DIRECTORY, 'data'), exist_ok=True)
os.makedirs(os.path.join(DIRECTORY, 'cache'), exist_ok=True)

# Max columns to make it easier to visualize the data
pd.set_option('display.max_columns', None)

# Cache enabled is necessary for larger file sizes
ff1.Cache.enable_cache('../data/cache')

# Race level dataframes
_RACE_LEVEL_DATAFRAMES = \
    [
        '_results'
        , '_track_status'
        , '_laps'
        , '_weather_data'
    ]

# Driver level dataframes
_DRIVER_LEVEL_DATAFRAMES = \
    [
        '_car_data'
        , 'pos_data'
    ]

# Range of years covered from 2018 to 2023
_YEAR_RANGE = range(2018, 2025)
# Types of events covered: Race, Qualifier, Sprint, Sprint Qualifier, Sprint Shootout
_EVENT_TYPE = ['R', 'Q', 'S', 'SQ', 'SS']

for current_year in _YEAR_RANGE:
    # Getting the schedule information
    current_schedule = ff1.get_event_schedule(current_year, include_testing=False)

    # Assigning filename
    filename = f'Data\\schedule_{current_year}.csv'

    # Storing each year's schedule into a .csv
    current_schedule.to_csv(filename, index=False, encoding='utf-8-sig')

    # Setting the number of rounds for the current years schedule
    n_rounds = len(current_schedule['RoundNumber'])

    # Set round variable to 0
    current_round = 0

    while current_round < n_rounds:
        current_round += 1

        for event_type in _EVENT_TYPE:

            try:
                # Set current session
                current_session = ff1.get_session(current_year, current_round, event_type)

            except ValueError:
                continue

            # Load the session
            current_session.load()

            # Setting the filename
            event_date = str(current_session.__getattribute__('date'))[:-9]

            current_event = current_session.__getattribute__('name')

            # Reassigning filename
            filename = f'Data\\ {event_date}_{current_round}_{current_event}'

            # Export race level dataframes
            for race_key in _RACE_LEVEL_DATAFRAMES:
                # Create column true_date
                current_session.__getattribute__(race_key)['true_date'] = event_date
                current_session.__getattribute__(race_key)['current_event'] = current_event

                # Save each driver's table for race_key
                current_session.__getattribute__(race_key).to_csv(
                    filename + race_key + '.csv', index=False, encoding='utf-8-sig'
                )

            # Export driver level dataframes
            for driver_keys in _DRIVER_LEVEL_DATAFRAMES:
                for driver in current_session.__getattribute__(driver_keys).keys():
                    # Create column true_date
                    current_session.__getattribute__(driver_keys)[driver]['true_date'] = event_date
                    current_session.__getattribute__(driver_keys)[driver]['current_event'] = current_event

                    # Create column driver
                    current_session.__getattribute__(driver_keys)[driver]['driver'] = driver

                    # Save each driver's table for driver_keys
                    current_session.__getattribute__(driver_keys)[driver].to_csv(
                        filename + driver_keys + '_' + driver + '.csv'
                        , index=False
                        , encoding='utf-8-sig'
                    )
