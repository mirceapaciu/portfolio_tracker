"""
Configuration settings for the portfolio tracking application.
"""
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
DB_DIR = DATA_DIR / "db"
LOG_DIR = PROJECT_ROOT / "log"

# Database configuration
DB_PATH = DB_DIR / "portfolio_tracker.db"

# Logging configuration
LOADER_LOG_PATH = LOG_DIR / "loader.log"

# Ensure directories exist
DB_DIR.mkdir(parents=True, exist_ok=True)
INPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
