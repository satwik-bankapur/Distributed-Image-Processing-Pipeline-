import os
import socket

WORKER_ID = "worker-satwik"

KAFKA_BROKER = "192.168.193.27:9092"  # Replace with actual broker IP

KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"
CONSUMER_GROUP = "image-processor-v3"



BATCH_SIZE = 10
POLL_TIMEOUT = 5.0

HEARTBEAT_INTERVAL = 5

LOG_DIR = "worker/logs"
os.makedirs(LOG_DIR, exist_ok=True)

TRANSFORMATIONS = {
    'grayscale': 'Convert to grayscale',
    'blur': 'Apply Gaussian blur',
    'edge_detection': 'Canny edge detection',
    'sharpen': 'Sharpen image',
    'brightness_increase': 'Increase brightness by 20%'
}

