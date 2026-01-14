import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "bnpl_events.json"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
USERS_PATH = SILVER_PATH / "users.csv"


def load_bronze_events():
    df = pd.read_json(BRONZE_PATH, lines=True)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def build_users(df_events: pd.DataFrame) -> pd.DataFrame:
    signup = df_events[df_events["event_type"] == "SIGNUP"].copy()
    signup["signup_date"] = signup["ts"].dt.date
    signup = signup[["user_id", "signup_date", "city"]]

    kyc = df_events[df_events["event_type"] == "KYC_OK"].copy()
    kyc["kyc_level"] = kyc["payload_json"].apply(lambda x: x.get("kyc_level"))
    kyc = kyc[["user_id", "kyc_level"]]

    users = signup.merge(kyc, on="user_id", how="left")
    users = users.drop_duplicates(subset=["user_id"])

    return users


def main():
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_events = load_bronze_events()
    print("Loaded columns:", df_events.columns.tolist())

    users = build_users(df_events)
    users.to_csv(USERS_PATH, index=False)

    print(f"✅ Silver users table created: {USERS_PATH}")


if __name__ == "__main__":
    main()
