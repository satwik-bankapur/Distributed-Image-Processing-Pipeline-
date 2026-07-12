# app/config.py

import os
from pathlib import Path

# --- Kafka ---
# Broker address is environment-driven so the same code runs locally (docker-compose)
# and across machines without edits. See .env.example.
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"

# Dedicated consumer group for the master's results consumer. Must stay distinct
# from the workers' group so the master never gets assigned task partitions.
MASTER_RESULTS_GROUP = os.getenv("MASTER_RESULTS_GROUP", "master-results")

# --- Image tiling ---
TILE_SIZE = int(os.getenv("TILE_SIZE", "512"))
MIN_IMAGE_SIZE = int(os.getenv("MIN_IMAGE_SIZE", "1024"))

# Paths are anchored to the package, not the CWD, so the app runs the same
# regardless of where it's launched from.
STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_DIR = STATIC_DIR / "uploads"
RESULTS_DIR = STATIC_DIR / "results"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Processing ---
TRANSFORMATIONS = [
    "grayscale",
    "blur",
    "edge_detection",
    "sharpen",
    "brightness_increase",
]
DEFAULT_TRANSFORMATION = "grayscale"

# Max seconds the upload request waits for all tiles to come back before failing.
RESULT_TIMEOUT_SEC = int(os.getenv("RESULT_TIMEOUT_SEC", "300"))
