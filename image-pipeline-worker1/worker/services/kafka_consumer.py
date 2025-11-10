from confluent_kafka import Consumer, KafkaError
from worker.config import (
    KAFKA_BROKER, KAFKA_TASKS_TOPIC, 
    CONSUMER_GROUP, POLL_TIMEOUT
)
import json

class TasksConsumer:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        
        self.consumer = Consumer({
	    'bootstrap.servers': KAFKA_BROKER,
	    'group.id': CONSUMER_GROUP,
	    'auto.offset.reset': 'earliest',
	    'enable.auto.commit': True,
	    'session.timeout.ms': 30000,
	    'heartbeat.interval.ms': 10000,
 	    'request.timeout.ms': 120000,
	    'socket.timeout.ms': 120000,
	    'max.partition.fetch.bytes': 10485760
	})
        
        self.consumer.subscribe([KAFKA_TASKS_TOPIC])
        print(f"[{self.worker_id}] Subscribed to {KAFKA_TASKS_TOPIC}")
    
    def consume_task(self):
        msg = self.consumer.poll(timeout=POLL_TIMEOUT)
        
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
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
        self.consumer.close()
        print(f"[{self.worker_id}] Consumer closed")

