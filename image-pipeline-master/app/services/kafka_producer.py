# app/services/kafka_producer.py

from confluent_kafka import Producer
from app.config import KAFKA_BROKER, KAFKA_TASKS_TOPIC
import json
import base64
from PIL import Image
import io

class KafkaImageProducer:
    def __init__(self):
        """Initialize Kafka producer"""
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'acks': 'all'  # Wait for all replicas to acknowledge
        })
        
    def delivery_report(self, err, msg):
        """Callback for producer delivery"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [partition {msg.partition()}]')
    
    def publish_tile(self, tile_id, tile_image, x, y, transformation, job_id):
        """
        Publish a single tile to Kafka
        
        Args:
            tile_id: Unique identifier for this tile
            tile_image: PIL Image object
            x, y: Position coordinates
            transformation: Type of transformation to apply
            job_id: Job identifier for tracking
        """
        try:
            # Convert PIL Image to bytes and encode as base64
            buffered = io.BytesIO()
            tile_image.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            
            # Create message
            message = {
                'tile_id': tile_id,
                'x': x,
                'y': y,
                'image_data': img_base64,
                'transformation': transformation,
                'job_id': job_id
            }
            
            # Publish to Kafka
            self.producer.produce(
                KAFKA_TASKS_TOPIC,
                key=str(tile_id),
                value=json.dumps(message),
                callback=self.delivery_report
            )
            
            self.producer.flush(timeout=1)
            print(f'Tile {tile_id} published to Kafka')
            
        except Exception as e:
            print(f'Error publishing tile {tile_id}: {str(e)}')
            raise
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer = None

