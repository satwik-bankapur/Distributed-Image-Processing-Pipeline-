# worker/config.py

import os

# Unique per worker instance (e.g. worker-1, worker-2). Only this differs
# between workers — everything else, including the consumer group, is shared.
WORKER_ID = os.getenv("WORKER_ID", "worker-1")

# --- Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"

# All workers MUST share one consumer group. Kafka then splits the tasks
# topic's partitions across the group members, so each tile is delivered to
# exactly one worker — that is what load-balances the pipeline.
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "image-processors")

# --- Processing ---
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

LOG_DIR = "worker/logs"
os.makedirs(LOG_DIR, exist_ok=True)

TRANSFORMATIONS = {
    "grayscale": "Convert to grayscale",
    "blur": "Apply Gaussian blur",
    "edge_detection": "Canny edge detection",
    "sharpen": "Sharpen image",
    "brightness_increase": "Increase brightness by 20%",
}
