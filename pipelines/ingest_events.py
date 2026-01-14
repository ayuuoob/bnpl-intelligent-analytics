import json
from pathlib import Path
from datetime import datetime, timezone

BRONZE_PATH = Path("data/bronze/bnpl_events.json")

def append_event(event: dict):
    print(f"Writing event to {BRONZE_PATH.resolve()}")

    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    event["ts"] = event["ts"].isoformat()
    print(f"Writing event to {BRONZE_PATH.resolve()}")


    with open(BRONZE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    sample_event = {
        "event_id": "evt_001",
        "event_type": "SIGNUP",
        "ts": datetime.now(timezone.utc),
        "user_id": "user_123",
        "merchant_id": None,
        "order_id": None,
        "device_id": "device_x",
        "city": "Casablanca",
        "payload_json": {"signup_channel": "mobile"}
    }

    # 🔴 THIS LINE MUST EXIST
    append_event(sample_event)
