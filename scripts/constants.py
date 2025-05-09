from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

LAPS = DATA_DIR / "laps"
TELEMETRY = DATA_DIR / "telemetry"
WEATHER = DATA_DIR / "weather"