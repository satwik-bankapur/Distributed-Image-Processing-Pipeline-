# app/routes/upload.py

import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException

from app.services.image_processor import ImageSplitter
from app.services.kafka_producer import KafkaImageProducer
from app.services.kafka_consumer import KafkaResultsConsumer
from app.config import (
    UPLOAD_DIR,
    RESULTS_DIR,
    DEFAULT_TRANSFORMATION,
    TRANSFORMATIONS,
    RESULT_TIMEOUT_SEC,
)

logger = logging.getLogger("master")
router = APIRouter()

# One producer/consumer per process, reused across requests.
producer = KafkaImageProducer()
consumer = KafkaResultsConsumer()


@router.post("/upload")
async def upload_image(
    file: UploadFile = File(...),
    transformation: str = DEFAULT_TRANSFORMATION,
):
    """Upload an image, fan its tiles out to Kafka, and return the reconstructed result.

    The request blocks until every tile comes back (or RESULT_TIMEOUT_SEC elapses).
    This keeps the client contract simple at the cost of a long-lived request; see
    the README "Trade-offs" section for the async alternative.
    """
    # Validate the transformation at the trust boundary — an unknown value would
    # otherwise pass straight through to the worker and silently no-op.
    if transformation not in TRANSFORMATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown transformation '{transformation}'. Allowed: {TRANSFORMATIONS}",
        )

    try:
        job_id = str(uuid.uuid4())

        file_path = UPLOAD_DIR / f"{job_id}_{file.filename}"
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
        logger.info("Job %s started (file=%s)", job_id, file_path)

        tiles, img_width, img_height = ImageSplitter.split_image(str(file_path), job_id)
        logger.info("Image %dx%d split into %d tiles, transformation=%s",
                    img_width, img_height, len(tiles), transformation)

        for tile in tiles:
            producer.publish_tile(
                tile_id=tile["tile_id"],
                tile_image=tile["image"],
                x=tile["x"],
                y=tile["y"],
                transformation=transformation,
                job_id=job_id,
            )

        collected_tiles = consumer.consume_results(
            job_id=job_id, num_tiles=len(tiles), timeout_sec=RESULT_TIMEOUT_SEC
        )

        if len(collected_tiles) < len(tiles):
            msg = (f"Incomplete processing: received {len(collected_tiles)}/{len(tiles)} "
                   f"tiles for job {job_id}")
            logger.warning(msg)
            raise HTTPException(status_code=504, detail=msg)

        reconstructed = ImageSplitter.reconstruct_image(collected_tiles, img_width, img_height, job_id)
        result_path = RESULTS_DIR / f"{job_id}_result.png"
        reconstructed.save(result_path, format="PNG")
        logger.info("Result saved: %s", result_path)

        return {
            "status": "success",
            "job_id": job_id,
            "message": "Image processed and reconstructed",
            "tiles_count": len(tiles),
            "transformation": transformation,
            "result_path": f"/static/results/{job_id}_result.png",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Upload failed")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/result/{job_id}")
async def get_result(job_id: str):
    """Return the result path for a finished job, or 404 if it isn't ready."""
    result_path = RESULTS_DIR / f"{job_id}_result.png"
    if not result_path.exists():
        raise HTTPException(status_code=404, detail="Result not found")
    return {
        "status": "success",
        "result_path": f"/static/results/{job_id}_result.png",
        "job_id": job_id,
    }


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Report whether a job's result image exists yet."""
    result_path = RESULTS_DIR / f"{job_id}_result.png"
    status = "completed" if result_path.exists() else "processing"
    return {"status": status, "job_id": job_id}
