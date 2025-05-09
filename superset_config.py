# superset_config.py
import os

# Database configuration
# SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset.db'
SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:postgres@postgres:5432/postgres'


# Flask App Builder configuration
SECRET_KEY = 'secret_key'

# Superset specific configuration
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# PostgreSQL database connection
SQLALCHEMY_BINDS = {
    'postgres': 'postgresql://postgres:postgres@postgres:5432/postgres'
}

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24  # 1 day default cache timeout
}

# Feature flags
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'DRILL_TO_DETAIL': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Enable CORS
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Set visualization options
VIZ_TYPE_BLACKLIST = []