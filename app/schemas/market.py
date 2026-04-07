from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class MarketSymbol(str, Enum):
    ndx = "NDX"
    spx = "SPX"
    btc = "BTC"
    wti = "WTI"


class MarketTimeframe(str, Enum):
    m1 = "1m"
    h1 = "1h"
    h4 = "4h"
    d1 = "1d"


class MarketDataRequest(BaseModel):
    symbol: MarketSymbol = Field(..., description="Supported market symbol.")
    timeframe: MarketTimeframe = Field(
        default=MarketTimeframe.d1,
        description="Supported OHLCV timeframe.",
    )
