import pandas as pd
import os
from constants import DATA_DIR

# Paths to your CSV files
file1 = f'{DATA_DIR}/telemetry_2022_2023.csv'
file2 = f'{DATA_DIR}/telemetry_2024.csv'

# Read the CSV files
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Check if both have the same column names (ignoring order)
if set(df1.columns) != set(df2.columns):
    raise ValueError(
        f"Column names mismatch:\n"
        f"File 1 columns: {set(df1.columns)}\n"
        f"File 2 columns: {set(df2.columns)}"
    )

# Reorder columns in df2 to match df1
df2 = df2[df1.columns]

# Concatenate the DataFrames
combined_df = pd.concat([df1, df2], ignore_index=True)

# Save to a new CSV file
output_file = f'{DATA_DIR}/telemetry.csv'
combined_df.to_csv(output_file, index=False)

# Delete the original files
os.remove(file1)
os.remove(file2)

print(f"CSV files have been concatenated into '{output_file}' and original files have been deleted.")
