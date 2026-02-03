import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-unsafe-change-me")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///local.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Bootstrap first admin user on first run (only if DB has no users)
    BOOTSTRAP_ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")
    BOOTSTRAP_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
    BOOTSTRAP_ADMIN_NAME = os.getenv("ADMIN_NAME", "Admin")

    APP_NAME = os.getenv("APP_NAME", "mini muse")

    # Dangerous: allows the in-app DB patch executor (/admin/db-patch).
    # Default OFF. Only enable intentionally via env var ALLOW_DB_PATCH=1.
    ALLOW_DB_PATCH = os.getenv("ALLOW_DB_PATCH", "0")

