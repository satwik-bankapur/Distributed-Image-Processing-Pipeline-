# app/main.py

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from app.routes import upload
from app.services.heartbeat_monitor import HeartbeatMonitor
import os

app = FastAPI(title="Distributed Image Processing Pipeline")

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(upload.router)

# Heartbeat monitor
heartbeat_monitor = HeartbeatMonitor()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    heartbeat_monitor.start_monitoring()
    print("Server started - Heartbeat monitoring active")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    heartbeat_monitor.stop_monitoring()
    print("Server stopped")

@app.get("/")
async def root():
    """Serve HTML UI"""
    with open("app/static/index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/workers")
async def get_active_workers():
    """Get list of active workers"""
    workers = heartbeat_monitor.get_active_workers()
    return {
        'active_workers': len(workers),
        'workers': workers
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {'status': 'healthy', 'service': 'image-pipeline-master'}

