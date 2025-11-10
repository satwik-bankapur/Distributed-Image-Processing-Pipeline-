import os
import socket

WORKER_ID = "worker-rahul"  # Change this for each worker

KAFKA_BROKER = "192.168.x.x:9092"  # Set Sahil's Kafka broker IP address here

KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"
CONSUMER_GROUP = "image-processors"

BATCH_SIZE = 10
POLL_TIMEOUT = 1.0  # seconds

HEARTBEAT_INTERVAL = 5  # seconds

LOG_DIR = "worker/logs"
os.makedirs(LOG_DIR, exist_ok=True)

TRANSFORMATIONS = {
    'grayscale': 'Convert to grayscale',
    'blur': 'Apply Gaussian blur',
    'edge_detection': 'Canny edge detection',
    'sharpen': 'Sharpen image',
    'brightness_increase': 'Increase brightness by 20%'
}
