# app/routes/upload.py

from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.image_processor import ImageSplitter
from app.services.kafka_producer import KafkaImageProducer
from app.services.kafka_consumer import KafkaResultsConsumer
from app.config import UPLOAD_DIR, DEFAULT_TRANSFORMATION, RESULTS_DIR
import os
import uuid
from pathlib import Path

router = APIRouter()

# Global instances
producer = KafkaImageProducer()
consumer = KafkaResultsConsumer()

@router.post("/upload")
async def upload_image(
    file: UploadFile = File(...),
    transformation: str = DEFAULT_TRANSFORMATION
):
    """
    Upload image for processing
    
    1. Save uploaded image
    2. Split into tiles
    3. Publish tiles to Kafka
    4. Wait for results, reconstruct and save final image
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save uploaded file
        file_path = UPLOAD_DIR / f"{job_id}_{file.filename}"
        contents = await file.read()
        with open(file_path, 'wb') as f:
            f.write(contents)
        
        print(f'\nJob {job_id} started')
        print(f'File saved: {file_path}')
        
        # Validate and split image
        splitter = ImageSplitter()
        tiles, img_width, img_height = splitter.split_image(str(file_path), job_id)
        
        print(f'Image: {img_width}x{img_height}, Transformation: {transformation}')
        
        # Publish tiles to Kafka
        for tile in tiles:
            producer.publish_tile(
                tile_id=tile['tile_id'],
                tile_image=tile['image'],
                x=tile['x'],
                y=tile['y'],
                transformation=transformation,
                job_id=job_id
            )

        # Wait for processed tiles to arrive on the results topic
        print(f'Waiting for {len(tiles)} processed tiles for job {job_id}...')
        collected_tiles = consumer.consume_results(job_id=job_id, num_tiles=len(tiles), timeout_sec=300)

        # Check if all tiles were received
        if len(collected_tiles) < len(tiles):
            msg = f'Incomplete processing: received {len(collected_tiles)}/{len(tiles)} tiles for job {job_id}'
            print(msg)
            raise HTTPException(status_code=500, detail=msg)

        # Reconstruct and save final image (save here to avoid touching image_processor.py)
        reconstructed_img = ImageSplitter.reconstruct_image(collected_tiles, img_width, img_height, job_id)
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        result_path = RESULTS_DIR / f"{job_id}_result.png"
        reconstructed_img.save(result_path, format="PNG")
        print(f'Result image saved: {result_path}')

        return {
            'status': 'success',
            'job_id': job_id,
            'message': f'Image processed and reconstructed',
            'tiles_count': len(tiles),
            'transformation': transformation,
            'result_path': f'/static/results/{job_id}_result.png'
        }
        
    except HTTPException:
        # re-raise HTTPExceptions unchanged
        raise
    except Exception as e:
        print(f'Error: {str(e)}')
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/result/{job_id}")
async def get_result(job_id: str):
    """Retrieve processed image result"""
    result_path = Path(f"app/static/results/{job_id}_result.png")
    
    if not result_path.exists():
        raise HTTPException(status_code=404, detail="Result not found")
    
    return {
        'status': 'success',
        'result_path': f"/static/results/{job_id}_result.png",
        'job_id': job_id
    }

@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Get job processing status"""
    result_path = Path(f"app/static/results/{job_id}_result.png")
    
    if result_path.exists():
        return {'status': 'completed', 'job_id': job_id}
    else:
        return {'status': 'processing', 'job_id': job_id}

