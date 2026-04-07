from __future__ import annotations

from app.schemas.market import MarketSymbol


ASSET_DISPLAY_NAMES: dict[MarketSymbol, str] = {
    MarketSymbol.ndx: "Nasdaq 100",
    MarketSymbol.spx: "S&P 500",
}
