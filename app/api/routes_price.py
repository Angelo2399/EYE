from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from app.schemas.market import MarketSymbol
from app.services.latest_price_service import LatestPriceService

router = APIRouter(prefix="/price", tags=["price"])

_latest_price_service = LatestPriceService()


@router.get("/latest")
def get_latest_price(
    symbol: MarketSymbol = Query(...),
) -> dict:
    try:
        return _latest_price_service.get_latest_price(symbol)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
