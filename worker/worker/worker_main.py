from worker.config import WORKER_ID
from worker.services.kafka_consumer import TasksConsumer
from worker.services.kafka_producer import ResultsProducer
from worker.services.image_processor import ImageProcessor
from worker.services.heartbeat_sender import HeartbeatSender
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler(f'worker/logs/{WORKER_ID}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(WORKER_ID)

class Worker:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tasks_consumed = 0
        self.tiles_processed = 0
        
        self.tasks_consumer = TasksConsumer(worker_id)
        self.results_producer = ResultsProducer(worker_id)
        self.heartbeat_sender = HeartbeatSender(worker_id)
        
        self.heartbeat_sender.start()
        
        logger.info(f"Worker initialized: {worker_id}")
    
    def process_task(self, task):
        try:
            tile_id = task['tile_id']
            x = task['x']
            y = task['y']
            transformation = task['transformation']
            job_id = task['job_id']
            image_data = task['image_data']
            
            logger.info(f"Processing tile {tile_id}...")
            
            processed_image = ImageProcessor.process_tile(image_data, transformation)
            
            self.results_producer.publish_result(
                tile_id=tile_id,
                processed_image_base64=processed_image,
                x=x,
                y=y,
                job_id=job_id
            )
            
            self.tiles_processed += 1
            logger.info(f"✅ Tile {tile_id} processed and result published")
            
        except Exception as e:
            logger.error(f"Error processing task: {str(e)}")
    
    def run(self):
        logger.info(f"Starting worker loop...")
        logger.info(f"Worker ID: {self.worker_id}")
        logger.info(f"Waiting for tasks from Kafka...\n")
        
        try:
            while True:
                task = self.tasks_consumer.consume_task()
                
                if task is not None:
                    self.tasks_consumed += 1
                    self.process_task(task)
                else:
                    time.sleep(0.5)
                
        except KeyboardInterrupt:
            logger.info("\nShutdown signal received")
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        logger.info("\nShutting down worker...")
        
        self.heartbeat_sender.stop()
        self.tasks_consumer.close()
        self.results_producer.close()
        
        logger.info(f"\n=== Final Statistics ===")
        logger.info(f"Tasks consumed: {self.tasks_consumed}")
        logger.info(f"Tiles processed: {self.tiles_processed}")
        logger.info(f"Worker shutdown complete\n")

def main():
    worker = Worker(WORKER_ID)
    worker.run()

if __name__ == "__main__":
    main()

