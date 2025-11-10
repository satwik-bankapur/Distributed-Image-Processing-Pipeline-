# app/config.py

import os
from pathlib import Path

# Kafka Configuration
KAFKA_BROKER = "192.168.193.27:9092"
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"

# Image Configuration
TILE_SIZE = 512
MIN_IMAGE_SIZE = 1024
UPLOAD_DIR = Path("app/static/uploads")
RESULTS_DIR = Path("app/static/results")

# Ensure directories exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Transformations available
TRANSFORMATIONS = [
    "grayscale",
    "blur",
    "edge_detection",
    "sharpen",
    "brightness_increase"
]

# Default transformation
DEFAULT_TRANSFORMATION = "grayscale"

# Consumer group for workers
CONSUMER_GROUP = "image-processors"

