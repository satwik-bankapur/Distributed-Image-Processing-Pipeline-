# app/main.py

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from app.routes import upload
from app.services.heartbeat_monitor import HeartbeatMonitor
from app.config import STATIC_DIR

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [master] %(message)s")
logger = logging.getLogger("master")

heartbeat_monitor = HeartbeatMonitor()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start/stop the background heartbeat monitor with the app."""
    heartbeat_monitor.start_monitoring()
    logger.info("Server started - heartbeat monitoring active")
    yield
    heartbeat_monitor.stop_monitoring()
    logger.info("Server stopped")


app = FastAPI(title="Distributed Image Processing Pipeline", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.include_router(upload.router)


@app.get("/")
async def root():
    """Serve the web UI."""
    with open(STATIC_DIR / "index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/workers")
async def get_active_workers():
    """List workers seen via heartbeat within the liveness window."""
    workers = heartbeat_monitor.get_active_workers()
    return {"active_workers": len(workers), "workers": workers}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "image-pipeline-master"}
