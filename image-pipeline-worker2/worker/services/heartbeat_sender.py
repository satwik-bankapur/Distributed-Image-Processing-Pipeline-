# worker/services/heartbeat_sender.py

from confluent_kafka import Producer
from worker.config import (
    KAFKA_BROKER, KAFKA_HEARTBEATS_TOPIC, 
    HEARTBEAT_INTERVAL
)
import json
import threading
import time
from datetime import datetime

class HeartbeatSender:
    """Sends periodic heartbeats to master"""
    
    def __init__(self, worker_id):
        """Initialize heartbeat sender"""
        self.worker_id = worker_id
        self.running = False
        
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': f'{worker_id}-heartbeat'
        })
    
    def start(self):
        """Start heartbeat thread"""
        self.running = True
        thread = threading.Thread(target=self._send_loop, daemon=True)
        thread.start()
        print(f"[{self.worker_id}] Heartbeat sender started")
    
    def _send_loop(self):
        """Send heartbeats periodically"""
        while self.running:
            try:
                heartbeat = {
                    'worker_id': self.worker_id,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'active'
                }
                
                self.producer.produce(
                    KAFKA_HEARTBEATS_TOPIC,
                    key=self.worker_id,
                    value=json.dumps(heartbeat)
                )
                
                self.producer.flush(timeout=1)
                print(f"[{self.worker_id}] ❤️  Heartbeat sent")
                
                time.sleep(HEARTBEAT_INTERVAL)
                
            except Exception as e:
                print(f"[{self.worker_id}] Heartbeat error: {str(e)}")
    
    def stop(self):
        """Stop heartbeat thread"""
        self.running = False
        self.producer.flush()
        print(f"[{self.worker_id}] Heartbeat sender stopped")

