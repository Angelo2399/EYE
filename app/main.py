from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.api.routes_asset_updates import router as asset_updates_router
from app.api.routes_briefings import router as briefings_router
from app.api.routes_chart import router as chart_router
from app.api.routes_health import router as health_router
from app.api.routes_price import router as price_router
from app.api.routes_signals import router as signals_router
from app.core.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    current_settings = settings or get_settings()

    app = FastAPI(
        title=current_settings.app_name,
        version=current_settings.app_version,
        debug=current_settings.debug,
        docs_url=None if current_settings.is_production else "/docs",
        redoc_url=None if current_settings.is_production else "/redoc",
        openapi_url=None if current_settings.is_production else "/openapi.json",
    )

    app.include_router(health_router, prefix=current_settings.api_v1_prefix)
    app.include_router(signals_router, prefix=current_settings.api_v1_prefix)
    app.include_router(chart_router, prefix=current_settings.api_v1_prefix)
    app.include_router(price_router, prefix=current_settings.api_v1_prefix)
    app.include_router(asset_updates_router, prefix=current_settings.api_v1_prefix)
    app.include_router(briefings_router, prefix=current_settings.api_v1_prefix)

    dashboard_path = Path(__file__).resolve().parent / "ui" / "dashboard.html"

    @app.get("/", tags=["root"])
    def read_root() -> dict[str, str]:
        return {
            "message": f"{current_settings.app_name} is running",
            "environment": current_settings.environment.value,
            "healthcheck": f"{current_settings.api_v1_prefix}/health",
            "dashboard": "/dashboard",
        }

    @app.get("/dashboard", response_class=HTMLResponse, tags=["dashboard"])
    def read_dashboard() -> HTMLResponse:
        html = dashboard_path.read_text(encoding="utf-8")
        return HTMLResponse(content=html)

    return app


app = create_app()
