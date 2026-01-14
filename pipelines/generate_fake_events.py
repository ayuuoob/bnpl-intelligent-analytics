import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from ingest_events import append_event

# =========================
# CONFIG
# =========================
NUM_USERS = 150
NUM_MERCHANTS = 20
INSTALLMENTS_PER_ORDER = 3

START_DATE = datetime.now(timezone.utc) - timedelta(days=120)
GRACE_DAYS = 2

USER_PERSONAS = {
    "good": 0.5,
    "average": 0.3,
    "risky": 0.2
}

MERCHANT_PERSONAS = {
    "good": 0.7,
    "risky": 0.3
}

CITIES = ["Casablanca", "Rabat", "Marrakech"]
MERCHANT_CATEGORIES = ["electronics", "fashion", "travel", "home"]

# =========================
# HELPERS
# =========================
def uid(prefix):
    return f"{prefix}_{uuid4().hex[:8]}"

def weighted_choice(dist):
    r = random.random()
    s = 0
    for k, v in dist.items():
        s += v
        if r <= s:
            return k
    return k

def rand_date(start, max_days):
    return start + timedelta(days=random.randint(0, max_days))

# =========================
# CREATE MERCHANTS
# =========================
merchants = []
for _ in range(NUM_MERCHANTS):
    merchants.append({
        "merchant_id": uid("merchant"),
        "persona": weighted_choice(MERCHANT_PERSONAS),
        "category": random.choice(MERCHANT_CATEGORIES)
    })

# =========================
# GENERATE USERS & EVENTS
# =========================
for _ in range(NUM_USERS):
    user_id = uid("user")
    user_persona = weighted_choice(USER_PERSONAS)
    signup_date = rand_date(START_DATE, 20)

    # ---------- SIGNUP ----------
    append_event({
        "event_id": uid("evt"),
        "event_type": "SIGNUP",
        "ts": signup_date,
        "user_id": user_id,
        "merchant_id": None,
        "order_id": None,
        "device_id": uid("dev"),
        "city": random.choice(CITIES),
        "payload_json": {"signup_channel": "mobile"}
    })

    # ---------- KYC ----------
    kyc_level = {
        "good": "full",
        "average": random.choice(["basic", "full"]),
        "risky": "basic"
    }[user_persona]

    append_event({
        "event_id": uid("evt"),
        "event_type": "KYC_OK",
        "ts": signup_date + timedelta(days=1),
        "user_id": user_id,
        "merchant_id": None,
        "order_id": None,
        "device_id": None,
        "city": None,
        "payload_json": {"kyc_level": kyc_level}
    })

    # ---------- ORDERS ----------
    orders_count = {
        "good": random.randint(2, 4),
        "average": random.randint(1, 3),
        "risky": random.randint(2, 5)
    }[user_persona]

    for _ in range(orders_count):
        merchant = random.choice(merchants)
        order_id = uid("order")
        order_date = rand_date(signup_date, 80)

        amount = {
            "good": random.randint(300, 900),
            "average": random.randint(600, 1800),
            "risky": random.randint(1500, 4000)
        }[user_persona]

        # ---------- APPROVAL LOGIC ----------
        reject_prob = 0.0

        if user_persona == "risky":
            reject_prob += 0.25
        if kyc_level == "basic":
            reject_prob += 0.25
        if amount > 2500:
            reject_prob += 0.2
        if merchant["persona"] == "risky":
            reject_prob += 0.1

        is_rejected = random.random() < reject_prob
        event_type = "ORDER_REJ" if is_rejected else "ORDER_OK"

        append_event({
            "event_id": uid("evt"),
            "event_type": event_type,
            "ts": order_date,
            "user_id": user_id,
            "merchant_id": merchant["merchant_id"],
            "order_id": order_id,
            "device_id": None,
            "city": None,
            "payload_json": {
                "amount": amount,
                "currency": "MAD",
                "installments_count": INSTALLMENTS_PER_ORDER,
                "merchant_category": merchant["category"]
            }
        })

        # ❌ STOP HERE if rejected
        if is_rejected:
            continue

        # ---------- INSTALLMENTS ----------
        inst_amount = amount / INSTALLMENTS_PER_ORDER

        for i in range(INSTALLMENTS_PER_ORDER):
            inst_id = uid("inst")
            due_date = order_date + timedelta(days=30 * (i + 1))

            append_event({
                "event_id": uid("evt"),
                "event_type": "INST_DUE",
                "ts": due_date,
                "user_id": user_id,
                "merchant_id": merchant["merchant_id"],
                "order_id": order_id,
                "device_id": None,
                "city": None,
                "payload_json": {
                    "installment_id": inst_id,
                    "due_date": due_date.date().isoformat(),
                    "installment_amount": inst_amount
                }
            })

            late_prob = {
                "good": 0.03,
                "average": 0.15,
                "risky": 0.45
            }[user_persona]

            if merchant["persona"] == "risky":
                late_prob += 0.15
            if kyc_level == "basic":
                late_prob += 0.1

            if random.random() < late_prob:
                paid_date = due_date + timedelta(days=random.randint(3, 12))
                append_event({
                    "event_id": uid("evt"),
                    "event_type": "INST_LATE",
                    "ts": paid_date,
                    "user_id": user_id,
                    "merchant_id": merchant["merchant_id"],
                    "order_id": order_id,
                    "device_id": None,
                    "city": None,
                    "payload_json": {
                        "installment_id": inst_id,
                        "late_days": (paid_date - due_date).days
                    }
                })
            else:
                paid_date = due_date - timedelta(days=random.randint(0, GRACE_DAYS))
                append_event({
                    "event_id": uid("evt"),
                    "event_type": "INST_PAID",
                    "ts": paid_date,
                    "user_id": user_id,
                    "merchant_id": merchant["merchant_id"],
                    "order_id": order_id,
                    "device_id": None,
                    "city": None,
                    "payload_json": {
                        "installment_id": inst_id,
                        "paid_date": paid_date.date().isoformat(),
                        "installment_amount": inst_amount,
                        "payment_channel": random.choice(["card", "wallet"])
                    }
                })

        # ---------- DISPUTES ----------
        if (
            user_persona == "risky"
            and merchant["persona"] == "risky"
            and random.random() < 0.25
        ):
            append_event({
                "event_id": uid("evt"),
                "event_type": "DISPUTE",
                "ts": order_date + timedelta(days=15),
                "user_id": user_id,
                "merchant_id": merchant["merchant_id"],
                "order_id": order_id,
                "device_id": None,
                "city": None,
                "payload_json": {
                    "dispute_reason": "refund",
                    "dispute_amount": amount
                }
            })

print("✅ Strong, realistic BNPL fake data generated")
