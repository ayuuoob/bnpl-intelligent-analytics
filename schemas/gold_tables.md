# Gold Layer – Analytics & Feature Tables

Gold tables are optimized for analytics, ML, and AI agents.
They are derived from Silver tables and updated daily.

---

## user_features_daily

One row per user per day.

- user_id (string)
- date (date)

### Behavioral & Risk Features
- on_time_rate_30d (float)
- on_time_rate_90d (float)
- late_days_sum_30d (int)
- late_days_sum_90d (int)
- installment_count_90d (int)
- avg_installment_amount_30d (float)

### Stability Signals
- device_change_count_30d (int)
- dispute_rate_90d (float)
- account_age_days (int)

---

## merchant_features_daily

One row per merchant per day.

- merchant_id (string)
- date (date)

### Performance Metrics
- approval_rate_30d (float)
- late_rate_30d (float)
- dispute_rate_30d (float)
- refund_rate_30d (float)

### Volume
- order_count_30d (int)
- gmv_30d (float)

---

## kpi_daily

One row per day (global KPIs).

- date (date)
- gmv (float)
- approval_rate (float)
- late_rate (float)
- dispute_rate (float)
- net_margin (float)

---

## cohorts_signup_week

User cohort analysis.

- signup_week (date)
- user_count (int)
- late_rate_30d (float)
- avg_order_amount (float)
