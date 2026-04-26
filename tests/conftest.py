"""Pytest wiring.

Sets dummy values for the settings the app expects at import time so
tests can import app modules without a real .env file.
"""

import os

# Must run *before* any ``from app.*`` import picks up settings.
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATEV_CLIENT_ID", "test-client-id")
os.environ.setdefault("DATEV_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("DATEV_DEFAULT_CLIENT_ID_PATH", "1694291-99999")
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("LOG_FORMAT", "plain")
