from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.api.v1.router import api_router
from app.auth.middleware import AccessControlMiddleware
from app.auth.routes import router as auth_router
from app.core.logging import configure_logging
from app.core.metrics import PrometheusMetricsMiddleware, metrics_response
from app.core.settings import settings
from app.db.session import initialize_database
from app.middleware.security import LocalRateLimitMiddleware, SecurityHeadersMiddleware
from app.services.search_client import InvalidSearchQuery, SearchUnavailable


def create_application() -> FastAPI:
    configure_logging(settings.log_level)

    application = FastAPI(
        title=settings.project_name,
        version=settings.app_version,
        summary="Local AI-powered SOC/XDR streaming analytics API",
        description=(
            "Production-style local FastAPI service for telemetry analytics, ML predictions, "
            "SOC alerting, incident intelligence, threat intelligence, monitoring, and dashboard APIs."
        ),
        docs_url="/docs",
        swagger_ui_oauth2_redirect_url=None,
        redoc_url="/redoc",
        openapi_url=f"{settings.api_v1_prefix}/openapi.json",
        contact={"name": "Streaming Analytics Platform Engineering"},
        license_info={"name": "MIT"},
        openapi_tags=[
            {"name": "health", "description": "Platform liveness and readiness checks."},
            {"name": "analytics", "description": "Stored telemetry, predictions, risk, and search APIs."},
            {"name": "dashboard", "description": "Real-time dashboard APIs and WebSocket streams."},
            {"name": "alerts", "description": "SOC alerts, incidents, status updates, and live alert streams."},
            {"name": "monitoring", "description": "Prometheus-backed monitoring and service health summaries."},
            {"name": "threat-intelligence", "description": "IOC enrichment, MITRE mapping, detection rules, entity intelligence, and attack timelines."},
        ],
    )

    application.add_middleware(GZipMiddleware, minimum_size=1024)  # list responses are repetitive JSON
    application.add_middleware(AccessControlMiddleware)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH"],
        allow_headers=["Content-Type", "X-CSRF-Token", "X-Requested-With"],
    )
    application.add_middleware(SecurityHeadersMiddleware)
    application.add_middleware(LocalRateLimitMiddleware)
    application.add_middleware(PrometheusMetricsMiddleware)

    application.add_api_route("/metrics", metrics_response, methods=["GET"], include_in_schema=False)

    application.include_router(api_router, prefix=settings.api_v1_prefix)
    application.include_router(auth_router, prefix=settings.api_v1_prefix)

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError):
        # Pydantic's default response echoes input, including password fields.
        return JSONResponse(status_code=422, content={"detail": "Invalid request"},
                            headers={"Cache-Control": "no-store"})

    @application.exception_handler(SearchUnavailable)
    async def search_unavailable_handler(request: Request, exc: SearchUnavailable) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Search is temporarily unavailable"},
                            headers={"Retry-After": "30", "Cache-Control": "no-store"})

    @application.exception_handler(InvalidSearchQuery)
    async def invalid_search_handler(request: Request, exc: InvalidSearchQuery) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": "The search query could not be parsed"},
                            headers={"Cache-Control": "no-store"})

    @application.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "path": request.url.path},
        )

    @application.on_event("startup")
    def initialize_local_database() -> None:
        initialize_database()

    return application


app = create_application()
