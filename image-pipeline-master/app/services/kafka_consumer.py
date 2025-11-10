# app/services/kafka_consumer.py

from confluent_kafka import Consumer, KafkaError
from app.config import (
    KAFKA_BROKER, KAFKA_RESULTS_TOPIC, 
    CONSUMER_GROUP, RESULTS_DIR
)
import json
import base64
import threading
import time
from PIL import Image
import io

class KafkaResultsConsumer:
    """Consumes processed tiles from workers"""
    
    def __init__(self):
        """Initialize Kafka consumer"""
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest'
        })
        
        self.consumer.subscribe([KAFKA_RESULTS_TOPIC])
        self.results_cache = {}  # Store results by job_id
        self.lock = threading.Lock()
    
    def consume_results(self, job_id, num_tiles, timeout_sec=300):
        """
        Consume processed tiles for a specific job
        
        Args:
            job_id: Job identifier
            num_tiles: Expected number of tiles to receive
            timeout_sec: Timeout in seconds
            
        Returns:
            Dictionary mapping tile_id to processed image data
        """
        print(f'Waiting for {num_tiles} processed tiles for job {job_id}...')
        
        collected_tiles = {}
        start_time = time.time()
        
        while len(collected_tiles) < num_tiles:
            # Check timeout
            if time.time() - start_time > timeout_sec:
                print(f'Timeout waiting for results. Got {len(collected_tiles)}/{num_tiles}')
                break
            
            # Poll for messages
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f'Error: {msg.error()}')
                continue
            
            try:
                # Decode message
                result = json.loads(msg.value().decode('utf-8'))
                
                # Only collect results for this job
                if result.get('job_id') != job_id:
                    continue
                
                tile_id = result['tile_id']
                
                # Decode image data
                img_base64 = result['processed_data']
                img_bytes = base64.b64decode(img_base64)
                img = Image.open(io.BytesIO(img_bytes))
                
                collected_tiles[tile_id] = {
                    'image': img,
                    'x': result['x'],
                    'y': result['y'],
                    'worker_id': result['worker_id']
                }
                
                print(f'Received tile {tile_id} from {result["worker_id"]}')
                
            except Exception as e:
                print(f'Error processing result: {str(e)}')
        
        print(f'Collected {len(collected_tiles)}/{num_tiles} tiles')
        return collected_tiles
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()

