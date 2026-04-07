from fastapi import APIRouter, Header, HTTPException, Query, status

from app.repositories.signal_repository import SignalRepository
from app.schemas.signal import SignalRequest, SignalResponse, StoredSignalResponse
from app.services.signal_service import generate_signal

router = APIRouter(prefix="/signals", tags=["signals"])

signal_repository = SignalRepository()
last_briefing_state: dict[str, object] | None = None


@router.post(
    "/generate",
    response_model=SignalResponse,
    status_code=status.HTTP_200_OK,
    summary="Generate signal",
)
def generate_signal_route(
    payload: SignalRequest,
    x_eye_timezone: str | None = Header(default=None),
) -> SignalResponse:
    global last_briefing_state

    print("EYE timezone header:", x_eye_timezone)
    response = generate_signal(payload, timezone_name=x_eye_timezone)

    try:
        from app.services.signal_service import _signal_service

        if hasattr(_signal_service, "get_last_briefing_payload"):
            last_briefing_state = _signal_service.get_last_briefing_payload()
    except Exception:
        pass

    return response


@router.get(
    "/recent",
    response_model=list[StoredSignalResponse],
    status_code=status.HTTP_200_OK,
    summary="List recent signals",
)
def list_recent_signals_route(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[StoredSignalResponse]:
    rows = signal_repository.list_recent_signals(limit=limit)
    return [StoredSignalResponse(**row) for row in rows]


@router.get(
    "/briefing/last",
    status_code=status.HTTP_200_OK,
    summary="Get last generated briefing payload",
)
def get_last_briefing_route() -> dict[str, object]:
    if not isinstance(last_briefing_state, dict):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No briefing payload available yet.",
        )

    return dict(last_briefing_state)
