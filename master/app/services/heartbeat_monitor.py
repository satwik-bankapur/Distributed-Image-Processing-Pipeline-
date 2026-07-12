# app/services/heartbeat_monitor.py

from confluent_kafka import Consumer, KafkaError
from app.config import KAFKA_BROKER, KAFKA_HEARTBEATS_TOPIC
import json
import time
import threading
from datetime import datetime

class HeartbeatMonitor:
    """Monitors worker heartbeats"""
    
    def __init__(self):
        """Initialize heartbeat consumer"""
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'heartbeat-monitor',
            'auto.offset.reset': 'latest'  # Only new heartbeats
        })
        
        self.consumer.subscribe([KAFKA_HEARTBEATS_TOPIC])
        self.workers = {}  # {worker_id: last_heartbeat_time}
        self.lock = threading.Lock()
        self.monitoring = False
    
    def start_monitoring(self):
        """Start heartbeat monitoring in background thread"""
        self.monitoring = True
        thread = threading.Thread(target=self._monitor_loop, daemon=True)
        thread.start()
        print('Heartbeat monitoring started')
    
    def _monitor_loop(self):
        """Background thread to consume heartbeats"""
        while self.monitoring:
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                continue
            
            try:
                heartbeat = json.loads(msg.value().decode('utf-8'))
                worker_id = heartbeat.get('worker_id')
                
                with self.lock:
                    self.workers[worker_id] = time.time()
                
                print(f'Heartbeat from {worker_id}')
                
            except Exception as e:
                print(f'Error processing heartbeat: {str(e)}')
    
    def get_active_workers(self, timeout_sec=30):
        """Get list of active workers (heartbeat within timeout)"""
        current_time = time.time()
        
        with self.lock:
            active = []
            for worker_id, last_hb in self.workers.items():
                if current_time - last_hb < timeout_sec:
                    active.append({
                        'worker_id': worker_id,
                        'last_heartbeat': datetime.fromtimestamp(last_hb).isoformat(),
                        'age_seconds': int(current_time - last_hb)
                    })
        
        return active
    
    def stop_monitoring(self):
        """Stop heartbeat monitoring"""
        self.monitoring = False
        self.consumer.close()

