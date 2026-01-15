# scripts/generate_gold_orders_analytics.py

import pandas as pd

# ------------------
# LOAD SILVER TABLES
# ------------------
users = pd.read_csv("data/silver/users.csv")
merchants = pd.read_csv("data/silver/merchants.csv")
orders = pd.read_csv("data/silver/orders.csv")
installments = pd.read_csv("data/silver/installments.csv")
payments = pd.read_csv("data/silver/payments.csv")
disputes = pd.read_csv("data/silver/disputes.csv")
refunds = pd.read_csv("data/silver/refunds.csv")

# ------------------
# INSTALLMENTS AGGREGATION (per order)
# ------------------
inst_agg = installments.groupby("order_id").agg(
    installments_count=("installment_id", "count"),
    paid_installments=("status", lambda x: (x == "paid").sum()),
    late_installments=("status", lambda x: (x == "late").sum()),
    unpaid_installments=("status", lambda x: (x == "unpaid").sum()),
    max_late_days=("late_days", "max")
).reset_index()

# ------------------
# PAYMENTS AGGREGATION (per order)
# ------------------
pay_agg = payments.groupby("order_id").agg(
    successful_payments=("status", lambda x: (x == "success").sum()),
    failed_payments=("status", lambda x: (x == "failed").sum())
).reset_index()

# ------------------
# DISPUTES / REFUNDS
# ------------------
disp_flag = disputes.groupby("order_id").size().reset_index(name="has_dispute")
disp_flag["has_dispute"] = 1

refund_agg = refunds.groupby("order_id").agg(
    refund_amount=("amount", "sum")
).reset_index()

# ------------------
# BUILD GOLD TABLE
# ------------------
gold = (
    orders
    .merge(users, on="user_id", how="left", suffixes=("", "_user"))
    .merge(merchants, on="merchant_id", how="left", suffixes=("", "_merchant"))
    .merge(inst_agg, on="order_id", how="left")
    .merge(pay_agg, on="order_id", how="left")
    .merge(disp_flag, on="order_id", how="left")
    .merge(refund_agg, on="order_id", how="left")
)

# ------------------
# CLEAN NULLS
# ------------------
gold["has_dispute"] = gold["has_dispute"].fillna(0).astype(int)
gold["refund_amount"] = gold["refund_amount"].fillna(0)

for col in [
    "paid_installments",
    "late_installments",
    "unpaid_installments",
    "successful_payments",
    "failed_payments"
]:
    gold[col] = gold[col].fillna(0).astype(int)

gold["max_late_days"] = gold["max_late_days"].fillna(0)

# ------------------
# SAVE GOLD
# ------------------
gold.to_csv("data/gold/gold_orders_analytics.csv", index=False)

print("✅ gold_orders_analytics generated")
print("Rows:", len(gold))
