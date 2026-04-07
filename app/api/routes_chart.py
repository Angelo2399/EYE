from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.services.market_data_service import MarketDataService

router = APIRouter(prefix="/chart", tags=["chart"])

_market_data_service = MarketDataService()

DISPLAY_NAMES = {
    MarketSymbol.ndx: "Nasdaq 100 (NDX)",
    MarketSymbol.spx: "S&P 500 (SPX)",
    MarketSymbol.btc: "Bitcoin (BTC)",
    MarketSymbol.wti: "WTI Crude Oil (WTI)",
}


@router.get("/ohlcv")
def get_chart_ohlcv(
    symbol: MarketSymbol = Query(...),
    timeframe: MarketTimeframe = Query(...),
    limit: int = Query(default=300, ge=1, le=2000),
) -> dict:
    try:
        frame = _market_data_service.get_ohlcv(symbol, timeframe).tail(limit).copy()

        candles = [
            {
                "time": row["timestamp"].isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]) if row["volume"] == row["volume"] else 0.0,
            }
            for _, row in frame.iterrows()
        ]

        return {
            "asset": DISPLAY_NAMES.get(symbol, str(symbol.value)),
            "symbol": symbol.value,
            "timeframe": timeframe.value,
            "count": len(candles),
            "candles": candles,
            "supported_symbols": ["NDX", "SPX", "BTC", "WTI"],
            "supported_timeframes": ["1m", "1h", "4h", "1d"],
        }

    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
