import time
import json
import logging
from typing import Annotated
from sqlmodel import Session
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware

from app.dependencies import get_settings, get_session
from app.routers import sources, models, recipients, destinations, transfers, internal

settings = get_settings()

SessionDep = Annotated[Session, Depends(get_session)]


# JSON log formatter
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "level": record.levelname,
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "message": json.loads(record.getMessage()),
            "name": record.name,
        }
        return json.dumps(log_data)


# Configure the root logger
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.setLevel(logging.INFO)
logger.addHandler(handler)



app = FastAPI()
app.include_router(sources.router)
app.include_router(models.router)
app.include_router(recipients.router)
app.include_router(destinations.router)
app.include_router(transfers.router)
app.include_router(internal.router)

origins = [
    settings.allow_origin,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_http(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    
    log_event = {
        "method": request.method,
        "url": str(request.url),
        "status_code": response.status_code,
        "client_ip": request.client.host,
        "process_time": f"{process_time:.4f}",
    }
    
    logger.info(json.dumps(log_event))

    return response
