import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.routing import APIRoute
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from .core.limiter import limiter  # must be imported before routers
from .api.endpoints import auth, students, admin, staff, ai, websocket
from .core.database import engine, Base
from .core.database import settings
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sqlalchemy import text
from dotenv import load_dotenv
load_dotenv()
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiter is defined in core/limiter.py to avoid circular imports


def custom_generate_unique_id(route: APIRoute):
    tag = route.tags[0] if route.tags else "api"
    return f"{tag}-{route.name}"


app = FastAPI(
    title="SPARK Production API",
    description="Scalable Production-Grade Analytics for Academic Records & Knowledge",
    version="2.0.0",
    generate_unique_id_function=custom_generate_unique_id,
    servers=[
        {"url": "https://spark-backend-n5s2.onrender.com", "description": "Production server"},
        {"url": "/", "description": "Local development server"},
    ],
)

# Sentry initialization
if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENV,
        integrations=[FastApiIntegration()],
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
    )
    logger.info("Sentry monitoring initialized.")

# ---------------------------------------------------------------------------
# Middleware — order matters: SlowAPI must be registered first so rate-limit
# errors are handled before CORS adds headers to the 429 response.
# ---------------------------------------------------------------------------
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# CORS Configuration
allowed_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://192.168.1.4:5173",
]
if settings.CORS_ORIGINS:
    allowed_origins.extend([origin.strip() for origin in settings.CORS_ORIGINS.split(",") if origin.strip()])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",  # covers all Vercel preview + prod URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers (v1)
# ---------------------------------------------------------------------------
app.include_router(auth.router, prefix="/api/v1/auth")
app.include_router(students.router, prefix="/api/v1/students")
app.include_router(admin.router, prefix="/api/v1/admin")
app.include_router(staff.router, prefix="/api/v1/staff")
app.include_router(ai.router, prefix="/api/v1/ai")
app.include_router(websocket.router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Global Exception Handlers (Ensures CORS headers on errors)
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    response = JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error": str(exc)},
    )
    # Manually add CORS headers since middleware might be bypassed on crash
    origin = request.headers.get("origin")
    if origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    response = JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
    origin = request.headers.get("origin")
    if origin and origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global unhandled error: {exc}", exc_info=True)
    response = JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error": str(exc)},
    )
    origin = request.headers.get("origin")
    if origin and origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

# Legacy redirects (compatibility for transition)
@app.api_route("/api/auth/{path:path}", include_in_schema=False, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def redirect_auth_v1(request: Request, path: str):
    return RedirectResponse(url=f"/api/v1/auth/{path}", status_code=307)

@app.api_route("/api/students/{path:path}", include_in_schema=False, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def redirect_students_v1(request: Request, path: str):
    return RedirectResponse(url=f"/api/v1/students/{path}", status_code=307)

@app.api_route("/api/admin/{path:path}", include_in_schema=False, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def redirect_admin_v1(request: Request, path: str):
    return RedirectResponse(url=f"/api/v1/admin/{path}", status_code=307)


# Legacy redirects (keep for backward compat)
@app.get("/api/admin/subject-bottlenecks", include_in_schema=False)
def redirect_bottlenecks():
    return RedirectResponse("/api/admin/bottlenecks", status_code=301)


@app.get("/api/admin/faculty-impact", include_in_schema=False)
def redirect_faculty_impact():
    return RedirectResponse("/api/admin/impact-matrix", status_code=301)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    db_host = settings.DATABASE_URL.split("@")[-1].split(":")[0].split("/")[0]
    logger.info(f"Application starting up... Connection target: {db_host}")
    try:
        async with engine.begin() as conn:
            pass
        logger.info("Database connection verified successfully.")
    except Exception as e:
        logger.warning(f"Startup DB ping failed (non-fatal, will retry on first request): {e}")


@app.get("/")
async def root():
    return {"message": "SPARK API is running", "version": "2.0.0", "docs": "/docs"}

@app.get("/health")
async def health_check():
    health_status = {"status": "healthy", "version": "2.0.0", "database": "disconnected"}
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        logger.error(f"Health check DB ping failed: {e}")
        health_status["status"] = "degraded"
        
    if health_status["status"] == "degraded":
        return JSONResponse(content=health_status, status_code=503)
    return health_status

@app.get("/api/v1/debug-sentry", include_in_schema=False)
async def trigger_error():
    if settings.SENTRY_DSN:
        division_by_zero = 1 / 0
    return {"message": "Sentry DSN not configured, skipping crash."}


