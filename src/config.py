import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"

# Database Configuration (SQLite analytical warehouse)
DB_PATH = DATA_DIR / "warehouse.db"

# Ensure directories exist
for path in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# Pipeline Settings
DEFAULT_BATCH_SIZE = 100
EXCHANGE_RATES_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.74,
    "PKR": 0.0036
}
