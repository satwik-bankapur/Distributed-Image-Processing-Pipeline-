from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import cv2
import numpy as np
import json
import pickle
import uuid
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import time
from io import BytesIO
import base64

app = Flask(__name__)
CORS(app)

# Kafka Configuration - Replace with Sahil's ZeroTier IP
KAFKA_BROKER = '192.168.193.27:9092'
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'

# Image processing settings
MIN_IMAGE_SIZE = 1024
TILE_SIZE = 512

# Storage for job tracking
active_jobs = {}
worker_status = {}

# Initialize Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'master-producer'
}
producer = Producer(producer_conf)

# Initialize Kafka Consumer for results
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'master-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}
result_consumer = Consumer(consumer_conf)
result_consumer.subscribe([RESULT_TOPIC, HEARTBEAT_TOPIC])

def create_topics():
    """Create Kafka topics if they don't exist"""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    
    topics = [
        NewTopic(TASK_TOPIC, num_partitions=2, replication_factor=1),
        NewTopic(RESULT_TOPIC, num_partitions=2, replication_factor=1),
        NewTopic(HEARTBEAT_TOPIC, num_partitions=1, replication_factor=1)
    ]
    
    try:
        fs = admin_client.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Topic {topic} already exists or error: {e}")
    except Exception as e:
        print(f"Error creating topics: {e}")

def split_image_into_tiles(image, tile_size=TILE_SIZE):
    """Split image into tiles of specified size"""
    height, width = image.shape[:2]
    
    # Ensure image meets minimum size
    if height < MIN_IMAGE_SIZE or width < MIN_IMAGE_SIZE:
        raise ValueError(f"Image must be at least {MIN_IMAGE_SIZE}x{MIN_IMAGE_SIZE}")
    
    tiles = []
    tile_positions = []
    
    # Calculate number of tiles
    num_tiles_y = height // tile_size
    num_tiles_x = width // tile_size
    
    for i in range(num_tiles_y):
        for j in range(num_tiles_x):
            y_start = i * tile_size
            x_start = j * tile_size
            y_end = y_start + tile_size
            x_end = x_start + tile_size
            
            tile = image[y_start:y_end, x_start:x_end]
            tiles.append(tile)
            tile_positions.append((i, j, y_start, x_start))
    
    return tiles, tile_positions, (num_tiles_y, num_tiles_x)

def delivery_report(err, msg):
    """Callback for delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def publish_tile_task(job_id, tile_id, tile_data, position, transformation='grayscale'):
    """Publish a tile processing task to Kafka"""
    # Encode tile as JPEG to reduce size
    _, buffer = cv2.imencode('.jpg', tile_data)
    tile_bytes = buffer.tobytes()
    
    task_data = {
        'job_id': job_id,
        'tile_id': tile_id,
        'tile_data': base64.b64encode(tile_bytes).decode('utf-8'),
        'position': position,
        'transformation': transformation,
        'timestamp': time.time()
    }
    
    # Serialize and send
    message = json.dumps(task_data)
    producer.produce(
        TASK_TOPIC,
        key=str(tile_id),
        value=message,
        callback=delivery_report
    )
    producer.poll(0)

def reconstruct_image(tiles_dict, grid_shape, tile_size):
    """Reconstruct image from processed tiles"""
    num_tiles_y, num_tiles_x = grid_shape
    height = num_tiles_y * tile_size
    width = num_tiles_x * tile_size
    
    # Determine if grayscale or color
    sample_tile = list(tiles_dict.values())[0]
    if len(sample_tile.shape) == 2:
        reconstructed = np.zeros((height, width), dtype=np.uint8)
    else:
        reconstructed = np.zeros((height, width, 3), dtype=np.uint8)
    
    for position, tile in tiles_dict.items():
        i, j, y_start, x_start = position
        y_end = y_start + tile_size
        x_end = x_start + tile_size
        reconstructed[y_start:y_end, x_start:x_end] = tile
    
    return reconstructed

@app.route('/upload', methods=['POST'])
def upload_image():
    """Handle image upload and initiate processing"""
    try:
        if 'image' not in request.files:
            return jsonify({'error': 'No image file provided'}), 400
        
        file = request.files['image']
        transformation = request.form.get('transformation', 'grayscale')
        
        # Read image
        file_bytes = np.frombuffer(file.read(), np.uint8)
        image = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
        
        if image is None:
            return jsonify({'error': 'Invalid image file'}), 400
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Split image into tiles
        tiles, positions, grid_shape = split_image_into_tiles(image)
        
        # Store job information
        active_jobs[job_id] = {
            'total_tiles': len(tiles),
            'received_tiles': 0,
            'tiles': {},
            'grid_shape': grid_shape,
            'tile_size': TILE_SIZE,
            'status': 'processing',
            'start_time': time.time()
        }
        
        # Publish tiles to Kafka
        for idx, (tile, position) in enumerate(zip(tiles, positions)):
            tile_id = f"{job_id}_{idx}"
            publish_tile_task(job_id, tile_id, tile, position, transformation)
        
        producer.flush()
        
        return jsonify({
            'job_id': job_id,
            'total_tiles': len(tiles),
            'message': 'Image processing started'
        }), 202
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get processing status of a job"""
    if job_id not in active_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = active_jobs[job_id]
    progress = (job['received_tiles'] / job['total_tiles']) * 100
    
    return jsonify({
        'job_id': job_id,
        'status': job['status'],
        'progress': progress,
        'received_tiles': job['received_tiles'],
        'total_tiles': job['total_tiles']
    })

@app.route('/result/<job_id>', methods=['GET'])
def get_result(job_id):
    """Retrieve processed image"""
    if job_id not in active_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = active_jobs[job_id]
    
    if job['status'] != 'completed':
        return jsonify({'error': 'Job not completed yet'}), 202
    
    # Reconstruct image
    reconstructed = reconstruct_image(
        job['tiles'],
        job['grid_shape'],
        job['tile_size']
    )
    
    # Encode as JPEG
    _, buffer = cv2.imencode('.jpg', reconstructed)
    
    return send_file(
        BytesIO(buffer.tobytes()),
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f'{job_id}_processed.jpg'
    )

@app.route('/workers', methods=['GET'])
def get_worker_status():
    """Get status of all workers"""
    current_time = time.time()
    active_workers = []
    
    for worker_id, status in worker_status.items():
        if current_time - status['last_heartbeat'] < 30:  # 30 second timeout
            active_workers.append({
                'worker_id': worker_id,
                'status': 'active',
                'last_seen': current_time - status['last_heartbeat'],
                'processed_count': status.get('processed_count', 0)
            })
    
    return jsonify({
        'active_workers': len(active_workers),
        'workers': active_workers
    })

def consume_results():
    """Background consumer for results and heartbeats"""
    while True:
        msg = result_consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue
        
        topic = msg.topic()
        
        if topic == RESULT_TOPIC:
            # Process result
            try:
                result_data = json.loads(msg.value().decode('utf-8'))
                job_id = result_data['job_id']
                
                if job_id in active_jobs:
                    # Decode tile
                    tile_bytes = base64.b64decode(result_data['tile_data'])
                    tile_array = np.frombuffer(tile_bytes, np.uint8)
                    tile = cv2.imdecode(tile_array, cv2.IMREAD_UNCHANGED)
                    
                    # Store tile
                    position = tuple(result_data['position'])
                    active_jobs[job_id]['tiles'][position] = tile
                    active_jobs[job_id]['received_tiles'] += 1
                    
                    # Check if job complete
                    if active_jobs[job_id]['received_tiles'] == active_jobs[job_id]['total_tiles']:
                        active_jobs[job_id]['status'] = 'completed'
                        processing_time = time.time() - active_jobs[job_id]['start_time']
                        print(f"Job {job_id} completed in {processing_time:.2f} seconds")
            
            except Exception as e:
                print(f"Error processing result: {e}")
        
        elif topic == HEARTBEAT_TOPIC:
            # Process heartbeat
            try:
                heartbeat = json.loads(msg.value().decode('utf-8'))
                worker_id = heartbeat['worker_id']
                worker_status[worker_id] = {
                    'last_heartbeat': time.time(),
                    'processed_count': heartbeat.get('processed_count', 0)
                }
            except Exception as e:
                print(f"Error processing heartbeat: {e}")

if __name__ == '__main__':
    # Create topics
    create_topics()
    time.sleep(2)
    
    # Start result consumer in background
    import threading
    consumer_thread = threading.Thread(target=consume_results, daemon=True)
    consumer_thread.start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=9092, debug=False)
