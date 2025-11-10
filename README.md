# 🖼️ Distributed Image Processing Pipeline with Kafka

A scalable, fault-tolerant distributed image processing system that uses Apache Kafka for asynchronous communication between master and worker nodes. The system splits images into tiles, processes them in parallel across multiple workers, and reconstructs the final result.

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the System](#running-the-system)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [API Documentation](#api-documentation)

## 🎯 Overview

This project implements a distributed image processing pipeline with:
- **Master Node**: Handles image upload, tile splitting, and reconstruction
- **Kafka Broker**: Message queue for task distribution and result collection
- **Worker Nodes**: Process image tiles independently with various transformations
- **Web Interface**: User-friendly UI for image upload and monitoring

## 🏗️ System Architecture

```
┌─────────────┐
│   Client    │
│   (Web UI)  │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Master Node (Node 1)        │
│  • Image Upload & Splitting         │
│  • Task Distribution via Kafka      │
│  • Result Collection & Reconstruction│
│  • Worker Health Monitoring         │
└─────────────┬───────────────────────┘
              │
              ▼
     ┌────────────────┐
     │ Kafka Broker   │
     │   (Node 2)     │
     │                │
     │ Topics:        │
     │ • tasks        │
     │ • results      │
     │ • heartbeats   │
     └────────┬───────┘
              │
       ┏━━━━━━┻━━━━━━┓
       ▼              ▼
┌─────────────┐ ┌─────────────┐
│  Worker 1   │ │  Worker 2   │
│  (Node 3)   │ │  (Node 4)   │
│             │ │             │
│ • Consume   │ │ • Consume   │
│   Tasks     │ │   Tasks     │
│ • Process   │ │ • Process   │
│   Tiles     │ │   Tiles     │
│ • Publish   │ │ • Publish   │
│   Results   │ │   Results   │
│ • Send      │ │ • Send      │
│   Heartbeats│ │   Heartbeats│
└─────────────┘ └─────────────┘
```

## ✨ Features

### Image Processing Transformations
- **Grayscale**: Convert to black and white
- **Blur**: Apply Gaussian blur (5x5 kernel)
- **Edge Detection**: Canny edge detection
- **Sharpen**: Image sharpening filter
- **Brightness Increase**: Increase brightness by 20%

### System Features
- ✅ Asynchronous task distribution via Kafka
- ✅ Parallel processing across multiple workers
- ✅ Automatic load balancing using Kafka consumer groups
- ✅ Real-time worker health monitoring via heartbeats
- ✅ Fault-tolerant message delivery
- ✅ Web-based UI for easy interaction
- ✅ Job tracking with unique job IDs
- ✅ Configurable tile sizes (512x512 default)

## 📦 Prerequisites

### Software Requirements
- **Python**: 3.8 or higher
- **Apache Kafka**: 2.8 or higher
- **Apache Zookeeper**: 3.6 or higher (for Kafka)

### Python Libraries
```
fastapi
uvicorn
confluent-kafka==2.3.0
pillow==10.0.0
opencv-python==4.8.1.78
numpy==1.24.3
```

## 🚀 Installation

### 1. Install Apache Kafka and Zookeeper

#### On Ubuntu/Debian:
```bash
# Install Java (required for Kafka)
sudo apt update
sudo apt install openjdk-11-jdk

# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0
```

#### On macOS:
```bash
brew install kafka
```

### 2. Clone the Project
```bash
git clone https://github.com/sanjandeep77/16_Project3_BD.git
cd distributed-image-pipeline
```

### 3. Install Python Dependencies

#### For Master Node:
```bash
cd image-pipeline-master
pip install -r requirements.txt
```

#### For Worker Nodes:
```bash
cd image-pipeline-worker
pip install -r requirements.txt
```

## ⚙️ Configuration

### 1. Start Kafka and Zookeeper

```bash
# Start Zookeeper (Terminal 1)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker (Terminal 2)
bin/kafka-server-start.sh config/server.properties
```

### 2. Create Kafka Topics

```bash
# Create tasks topic (2 partitions for 2 workers)
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 2 \
  --topic tasks

# Create results topic
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 2 \
  --topic results

# Create heartbeats topic
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic heartbeats
```

### 3. Configure Network Settings

#### Master Node (`image-pipeline-master/app/config.py`):
```python
# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Or IP address if on different machine
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"

# Master consumer group (for results)
MASTER_CONSUMER_GROUP = "master-results-consumer"
```

#### Worker Nodes (`worker/config.py`):
```python
# Worker Configuration
WORKER_ID = "worker-1"  # Change to "worker-2" for second worker

# Kafka Configuration
KAFKA_BROKER = "192.168.x.x:9092"  # Master node IP address
KAFKA_TASKS_TOPIC = "tasks"
KAFKA_RESULTS_TOPIC = "results"
KAFKA_HEARTBEATS_TOPIC = "heartbeats"

# IMPORTANT: Both workers must use the same consumer group
CONSUMER_GROUP = "image-processors"
```

### 4. Update Master Consumer Group

In `image-pipeline-master/app/services/kafka_consumer.py`:
```python
self.consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'master-results-consumer',  # Different from workers
    'auto.offset.reset': 'earliest'
})
```

## 🏃 Running the System

### 1. Start the Master Node
```bash
cd image-pipeline-master
python run.py
```
The master will start on `http://localhost:8000`

### 2. Start Worker 1
```bash
cd image-pipeline-worker
python run_worker.py
```

### 3. Start Worker 2 (on different machine or terminal)
```bash
cd image-pipeline-worker1
python run_worker.py
```

### 4. Access the Web Interface
Open your browser and navigate to:
```
http://localhost:8000
```

## 📖 Usage

### Upload and Process an Image

1. **Open the Web UI** at `http://localhost:8000`
2. **Select an image** (minimum size: 1024x1024 pixels)
3. **Choose a transformation** from the dropdown:
   - Grayscale
   - Blur
   - Edge Detection
   - Sharpen
4. **Click "Upload & Process"**
5. **Monitor progress** in the dashboard
6. **View active workers** in the monitoring panel
7. **Download the result** once processing is complete

### Using the REST API

#### Upload Image
```bash
curl -X POST "http://localhost:8000/upload?transformation=grayscale" \
  -F "file=@/path/to/image.jpg"
```

Response:
```json
{
  "status": "success",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "tiles_count": 4,
  "transformation": "grayscale",
  "result_path": "/static/results/550e8400-e29b-41d4-a716-446655440000_result.png"
}
```

#### Check Job Status
```bash
curl "http://localhost:8000/status/{job_id}"
```

#### Get Result
```bash
curl "http://localhost:8000/result/{job_id}"
```

#### Get Active Workers
```bash
curl "http://localhost:8000/api/workers"
```

Response:
```json
{
  "active_workers": 2,
  "workers": [
    {
      "worker_id": "worker-1",
      "last_heartbeat": "2025-11-09T18:30:45",
      "age_seconds": 3
    },
    {
      "worker_id": "worker-2",
      "last_heartbeat": "2025-11-09T18:30:47",
      "age_seconds": 1
    }
  ]
}
```

## 🔧 Troubleshooting

### Workers Not Receiving Tasks

**Problem**: Workers are idle, not processing tiles.

**Solution**:
1. **Check consumer groups** - Both workers must use the same group:
   ```python
   CONSUMER_GROUP = "image-processors"
   ```

2. **Verify Kafka broker IP** in worker config:
   ```python
   KAFKA_BROKER = "192.168.x.x:9092"  # Use correct IP
   ```

3. **Check Kafka topics exist**:
   ```bash
   bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

4. **Reset consumer group offsets**:
   ```bash
   bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --group image-processors --reset-offsets --to-earliest \
     --topic tasks --execute
   ```

### Connection Refused Error

**Problem**: `Connection refused` to Kafka broker.

**Solution**:
1. Ensure Kafka is running:
   ```bash
   netstat -tulpn | grep 9092
   ```

2. Check firewall settings:
   ```bash
   sudo ufw allow 9092/tcp
   ```

3. Update Kafka's `server.properties`:
   ```properties
   listeners=PLAINTEXT://0.0.0.0:9092
   advertised.listeners=PLAINTEXT://192.168.x.x:9092
   ```

### Image Size Error

**Problem**: `Image must be at least 1024x1024`.

**Solution**: Ensure uploaded images meet minimum size requirements or adjust in `config.py`:
```python
MIN_IMAGE_SIZE = 512  # Lower if needed
```

### Worker Heartbeats Not Showing

**Problem**: No workers appear in the monitoring dashboard.

**Solution**:
1. Check heartbeat topic exists
2. Verify heartbeat interval in worker config
3. Check master heartbeat monitor is running
4. Review worker logs for errors

### Incomplete Processing

**Problem**: Not all tiles are processed.

**Solution**:
1. Check worker logs for errors
2. Increase timeout in `kafka_consumer.py`:
   ```python
   timeout_sec=600  # Increase timeout
   ```
3. Verify all workers are running
4. Check Kafka partition assignment

## 📁 Project Structure

```
distributed-image-pipeline/
├── image-pipeline-master/
│   ├── app/
│   │   ├── config.py                 # Master configuration
│   │   ├── main.py                   # FastAPI application
│   │   ├── routes/
│   │   │   └── upload.py             # Upload endpoints
│   │   ├── services/
│   │   │   ├── image_processor.py    # Image splitting/reconstruction
│   │   │   ├── kafka_producer.py     # Publish tasks
│   │   │   ├── kafka_consumer.py     # Consume results
│   │   │   └── heartbeat_monitor.py  # Monitor workers
│   │   └── static/
│   │       ├── index.html            # Web UI
│   │       ├── uploads/              # Uploaded images
│   │       └── results/              # Processed images
│   ├── requirements.txt
│   └── run.py                        # Start master
│
├── image-pipeline-worker/
│   ├── worker/
│   │   ├── config.py                 # Worker configuration
│   │   ├── worker_main.py            # Main worker logic
│   │   ├── services/
│   │   │   ├── kafka_consumer.py     # Consume tasks
│   │   │   ├── kafka_producer.py     # Publish results
│   │   │   ├── image_processor.py    # Process tiles
│   │   │   └── heartbeat_sender.py   # Send heartbeats
│   │   └── logs/
│   │       └── worker-1.log          # Worker logs
│   ├── requirements.txt
│   └── run_worker.py                 # Start worker
│
└── image-pipeline-worker1/           # Second worker (same structure)
    └── ...
```

## 📚 API Documentation

### Endpoints

#### `POST /upload`
Upload and process an image.

**Parameters**:
- `file`: Image file (multipart/form-data)
- `transformation`: Processing type (query parameter)

**Returns**: Job information with job_id

---

#### `GET /status/{job_id}`
Get processing status for a job.

**Returns**: 
```json
{
  "status": "completed" | "processing",
  "job_id": "string"
}
```

---

#### `GET /result/{job_id}`
Get processed image result.

**Returns**: Result path and job information

---

#### `GET /api/workers`
Get list of active workers.

**Returns**: Worker count and details

---

#### `GET /health`
Health check endpoint.

**Returns**: Service status

## 🤝 Contributors

- **Master Node**: R Sanjandeep
- **Broker**: Sahil
- **Worker 1**: Satwik
- **Worker 2**: Rahul

---

**Note**: Remember to start services in order: Zookeeper → Kafka → Master → Workers
