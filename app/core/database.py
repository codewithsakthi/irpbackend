import logging
import os
import ssl
from typing import Optional
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, validator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    # Use a dummy default that is easily detectable but won't cause immediate resolution errors if not used
    DATABASE_URL: Optional[str] = Field(default=None, env="DATABASE_URL")
    SECRET_KEY: str = "maybedemo"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30  # Max JWT lifetime; inactivity logout (7 days) is enforced client-side
    DB_POOL_SIZE: int = 3
    DB_MAX_OVERFLOW: int = 7
    # CORS_ORIGINS: comma-separated list of allowed frontend origins
    # e.g. "http://localhost:5173,https://my-app.vercel.app"
    CORS_ORIGINS: str = ""
    SENTRY_DSN: Optional[str] = Field(default=None, env="SENTRY_DSN")
    SENTRY_ENV: str = Field(default="production", env="SENTRY_ENV")

    # ── AI / Intelligent Analytics settings ────────────────────────────────────
    # Defaulting to DeepSeek-V3 via NVIDIA Integra API.
    # To use local Ollama, set AI_API_URL to http://localhost:11434/v1
    AI_API_URL: Optional[str] = Field(default="https://integrate.api.nvidia.com/v1", env="AI_API_URL")
    AI_ASR_URL: str = Field(default="grpc.nvcf.nvidia.com:443", env="AI_ASR_URL")
    AI_ASR_FUNCTION_ID: str = Field(default="b702f636-f60c-4a3d-a6f4-f3568c13bd7d", env="AI_ASR_FUNCTION_ID")
    AI_ASR_FUNCTION_HEADER: str = Field(default="function-id", env="AI_ASR_FUNCTION_HEADER")
    AI_ASR_LANGUAGE_CODE: str = Field(default="en", env="AI_ASR_LANGUAGE_CODE")
    AI_ASR_TIMEOUT_SECONDS: float = Field(default=30.0, env="AI_ASR_TIMEOUT_SECONDS")
    AI_API_KEY: Optional[str] = Field(default=None, env="AI_API_KEY")
    AI_MODEL: str = Field(default="stepfun-ai/step-3.5-flash", env="AI_MODEL")
    AI_STREAM_ENABLED: str = Field(default="true", env="AI_STREAM_ENABLED")
    OPENAI_API_KEY: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    OPENAI_ASR_BASE_URL: str = Field(default="https://api.openai.com/v1", env="OPENAI_ASR_BASE_URL")
    OPENAI_ASR_MODEL: str = Field(default="whisper-1", env="OPENAI_ASR_MODEL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @validator("DATABASE_URL", pre=True)
    def validate_database_url(cls, v):
        if not v:
            # Try to get from OS environment directly if pydantic didn't pick it up
            v = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PRIVATE_URL")
        
        if not v:
            return None
            
        # Support common URL prefixes and ensure asyncpg driver
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql+asyncpg://", 1)
        elif v.startswith("postgresql://") and "+asyncpg" not in v:
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
            
        return v

settings = Settings()

# Enhanced Diagnostic Logging
db_related_vars = [k for k in os.environ.keys() if "DATABASE" in k or "POSTGRES" in k or "PG" in k]
logger.info(f"DB DIAGNOSTICS: Found {len(db_related_vars)} potentially relevant env vars: {db_related_vars}")

# Critical check for DATABASE_URL
if not settings.DATABASE_URL:
    error_msg = (
        "CRITICAL ERROR: DATABASE_URL is not set. "
        "Please go to your Railway Service -> Variables and ensure 'DATABASE_URL' is added. "
        "If you have a Postgres service, you can use '${{Postgres.DATABASE_URL}}' as the value."
    )
    logger.critical(error_msg)
    raise RuntimeError(error_msg)

# Diagnostic Logging (Safe masked URL)
try:
    masked_url = settings.DATABASE_URL.split("@")[-1] if "@" in settings.DATABASE_URL else "INVALID_URL"
    logger.info(f"Database settings initialized. Target host: {masked_url.split(':')[0].split('/')[0]}")
except Exception as e:
    logger.error(f"Error parsing DATABASE_URL for logging: {e}")

# Determine if SSL is required
use_ssl = "sslmode=require" in settings.DATABASE_URL
db_url = settings.DATABASE_URL.replace("?sslmode=require", "").replace("&sslmode=require", "")

# Create SSL context only if needed
connect_args = {}
if use_ssl:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    connect_args["ssl"] = ssl_context
    logger.info("Database connection: SSL enabled (verification disabled)")
else:
    logger.info("Database connection: SSL disabled")

# Create async engine
engine = create_async_engine(
    db_url,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_pre_ping=True,
    echo=False,
    connect_args=connect_args
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
