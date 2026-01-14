import json
import pandas as pd
from pathlib import Path

# --------------------
# PATHS
# --------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "bnpl_events.json"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
ORDERS_PATH = SILVER_PATH / "orders.csv"


def load_bronze_events():
    events = []
    with open(BRONZE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    df = pd.DataFrame(events)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def build_orders(df: pd.DataFrame) -> pd.DataFrame:
    orders = df[df["event_type"].isin(["ORDER_OK", "ORDER_REJ"])].copy()

    orders["status"] = orders["event_type"].apply(
        lambda x: "approved" if x == "ORDER_OK" else "rejected"
    )

    orders["amount"] = orders["payload_json"].apply(lambda x: x.get("amount"))
    orders["currency"] = orders["payload_json"].apply(lambda x: x.get("currency"))
    orders["installments_count"] = orders["payload_json"].apply(
        lambda x: x.get("installments_count")
    )

    orders = orders[[
        "order_id",
        "user_id",
        "merchant_id",
        "ts",
        "status",
        "amount",
        "currency",
        "installments_count"
    ]]

    orders = orders.rename(columns={"ts": "order_date"})
    orders = orders.drop_duplicates(subset=["order_id"])

    return orders


def main():
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_events = load_bronze_events()
    orders = build_orders(df_events)

    orders.to_csv(ORDERS_PATH, index=False)
    print(f"✅ Silver orders table created: {ORDERS_PATH}")


if __name__ == "__main__":
    main()
