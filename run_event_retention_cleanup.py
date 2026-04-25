from __future__ import annotations

import json

from app.repositories.market_event_repository import MarketEventRepository


def main() -> None:
    repo = MarketEventRepository()

    deleted_events = repo.delete_events_older_than(days=90)
    deleted_feedback = repo.delete_feedback_older_than(days=180)

    print(
        json.dumps(
            {
                "deleted_events": deleted_events,
                "deleted_feedback": deleted_feedback,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
