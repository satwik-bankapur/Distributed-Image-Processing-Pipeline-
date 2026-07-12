from confluent_kafka import Producer
from worker.config import KAFKA_BROKER, KAFKA_RESULTS_TOPIC
import json

class ResultsProducer:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'acks': 'all',
            'client.id': f'{worker_id}-producer'
        })
    
    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[{self.worker_id}] Delivery failed: {err}")
        else:
            print(f"[{self.worker_id}] Result delivered to partition {msg.partition()}")
    
    def publish_result(self, tile_id, processed_image_base64, x, y, job_id):
        try:
            result = {
                'tile_id': tile_id,
                'processed_data': processed_image_base64,
                'x': x,
                'y': y,
                'job_id': job_id,
                'worker_id': self.worker_id
            }
            
            self.producer.produce(
                KAFKA_RESULTS_TOPIC,
                key=str(tile_id),
                value=json.dumps(result),
                callback=self.delivery_report
            )
            
            self.producer.flush(timeout=5)
            print(f"[{self.worker_id}] Result for tile {tile_id} published")
            
        except Exception as e:
            print(f"[{self.worker_id}] Error publishing result: {str(e)}")
            raise
    
    def close(self):
        self.producer.flush()
        print(f"[{self.worker_id}] Producer closed")

