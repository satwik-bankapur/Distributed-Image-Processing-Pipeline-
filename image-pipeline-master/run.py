# run.py

import uvicorn
import sys

if __name__ == "__main__":
    # Run FastAPI server
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

