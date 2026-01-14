import json
import pandas as pd
from pathlib import Path

# --------------------
# PATHS
# --------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "bnpl_events.json"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
INSTALLMENTS_PATH = SILVER_PATH / "installments.csv"


def load_bronze_events():
    events = []
    with open(BRONZE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    df = pd.DataFrame(events)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def build_installments(df: pd.DataFrame) -> pd.DataFrame:
    # ---------- DUE ----------
    due = df[df["event_type"] == "INST_DUE"].copy()
    due["installment_id"] = due["payload_json"].apply(lambda x: x["installment_id"])
    due["due_date"] = pd.to_datetime(due["payload_json"].apply(lambda x: x["due_date"]))
    due["installment_amount"] = due["payload_json"].apply(lambda x: x["installment_amount"])

    due = due[[
        "installment_id",
        "order_id",
        "user_id",
        "merchant_id",
        "due_date",
        "installment_amount"
    ]]

    # ---------- PAID ----------
    paid = df[df["event_type"] == "INST_PAID"].copy()
    paid["installment_id"] = paid["payload_json"].apply(lambda x: x["installment_id"])
    paid["paid_date"] = pd.to_datetime(paid["payload_json"].apply(lambda x: x["paid_date"]))

    paid = paid[[
        "installment_id",
        "paid_date"
    ]]

    # ---------- LATE ----------
    late = df[df["event_type"] == "INST_LATE"].copy()
    late["installment_id"] = late["payload_json"].apply(lambda x: x["installment_id"])
    late["late_days"] = late["payload_json"].apply(lambda x: x["late_days"])
    late["paid_date"] = late["ts"]

    late = late[[
        "installment_id",
        "paid_date",
        "late_days"
    ]]

    # ---------- MERGE ----------
    installments = due.merge(paid, on="installment_id", how="left")
    installments = installments.merge(late, on="installment_id", how="left", suffixes=("", "_late"))

    # ---------- STATUS ----------
    installments["status"] = "due"
    installments.loc[installments["paid_date"].notna() & installments["late_days"].isna(), "status"] = "paid"
    installments.loc[installments["late_days"].notna(), "status"] = "late"

    installments["late_days"] = installments["late_days"].fillna(0).astype(int)

    return installments


def main():
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_events = load_bronze_events()
    installments = build_installments(df_events)

    installments.to_csv(INSTALLMENTS_PATH, index=False)
    print(f"✅ Silver installments table created: {INSTALLMENTS_PATH}")


if __name__ == "__main__":
    main()
