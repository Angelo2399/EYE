from __future__ import annotations

from fastapi import APIRouter, Header, status
from pydantic import BaseModel, Field

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.services.asset_update_runner_service import AssetUpdateRunnerService

router = APIRouter(prefix="/asset-updates", tags=["asset-updates"])

asset_update_runner_service = AssetUpdateRunnerService()


class AssetUpdateRunRequest(BaseModel):
    assets: list[MarketSymbol] | None = None
    timeframe: MarketTimeframe = Field(default=MarketTimeframe.h1)


@router.post(
    "/run",
    status_code=status.HTTP_200_OK,
    summary="Run asset update cycle manually",
)
def run_asset_updates_route(
    payload: AssetUpdateRunRequest,
    x_eye_timezone: str | None = Header(default=None),
) -> dict[str, object]:
    runner_result = asset_update_runner_service.run_asset_updates(
        assets=payload.assets,
        timeframe=payload.timeframe,
        timezone_name=x_eye_timezone,
    )

    results = list(runner_result.get("results") or [])
    errors = list(runner_result.get("errors") or [])

    return {
        "results": results,
        "errors": errors,
        "processed_count": len(results) + len(errors),
        "error_count": len(errors),
    }
