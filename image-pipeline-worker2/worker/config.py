# worker/config.py

import os
import socket

# Get hostname to identify this worker
WORKER_ID = "worker-rahul"


# Kafka Configuration
KAFKA_BROKER = "192.168.193.27:9092"  # Change to master IP if on different machine
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"
CONSUMER_GROUP = "image-processors-v2"

# Processing Configuration
BATCH_SIZE = 10  # Process tiles in batches
POLL_TIMEOUT = 1.0  # Kafka poll timeout in seconds

# Heartbeat Configuration
HEARTBEAT_INTERVAL = 5  # Send heartbeat every 5 seconds

# Logging
LOG_DIR = "worker/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Available image transformations
TRANSFORMATIONS = {
    'grayscale': 'Convert to grayscale',
    'blur': 'Apply Gaussian blur',
    'edge_detection': 'Canny edge detection',
    'sharpen': 'Sharpen image',
    'brightness_increase': 'Increase brightness by 20%'
}

