from __future__ import annotations

import json

from app.services.event_feedback_runner_service import EventFeedbackRunnerService


def main() -> None:
    service = EventFeedbackRunnerService()
    result = service.process_pending_events(limit=50)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
