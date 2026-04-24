from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import health, me_echo
from app.core.logging import configure_logging
from app.core.settings import settings

configure_logging()

app = FastAPI(
    title="DATEV Buchungstool",
    version="0.1.0",
    docs_url="/docs" if settings.environment != "production" else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(me_echo.router)
