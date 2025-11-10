# worker/services/kafka_consumer.py

from confluent_kafka import Consumer, KafkaError
from worker.config import (
    KAFKA_BROKER, KAFKA_TASKS_TOPIC, 
    CONSUMER_GROUP, POLL_TIMEOUT
)
import json

class TasksConsumer:
    """Consumes image tiles from master"""
    
    def __init__(self, worker_id):
        """Initialize consumer"""
        self.worker_id = worker_id
        
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000
        })
        
        self.consumer.subscribe([KAFKA_TASKS_TOPIC])
        print(f"[{self.worker_id}] Subscribed to {KAFKA_TASKS_TOPIC}")
    
    def consume_task(self):
        """
        Consume a single task (image tile)
        
        Returns:
            Task dictionary or None if no message
        """
        msg = self.consumer.poll(timeout=POLL_TIMEOUT)
        
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                return None
            else:
                print(f"[{self.worker_id}] Consumer error: {msg.error()}")
                return None
        
        try:
            task = json.loads(msg.value().decode('utf-8'))
            print(f"\n[{self.worker_id}] Received task:")
            print(f"  - Tile ID: {task['tile_id']}")
            print(f"  - Position: ({task['x']}, {task['y']})")
            print(f"  - Transformation: {task['transformation']}")
            print(f"  - Job ID: {task['job_id']}")
            
            return task
            
        except Exception as e:
            print(f"[{self.worker_id}] Error decoding task: {str(e)}")
            return None
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()
        print(f"[{self.worker_id}] Consumer closed")

