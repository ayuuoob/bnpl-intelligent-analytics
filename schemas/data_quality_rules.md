# Data Quality Rules – BNPL Platform

This document defines mandatory data quality checks applied to Silver and Gold tables.
These checks ensure trust, explainability, and safe decision-making.

---

## Global Rules (All Tables)

- No duplicate primary keys
- No nulls in mandatory fields
- Data freshness: updated daily
- All timestamps must be UTC

---

## users

- user_id must be unique
- signup_date must not be in the future
- kyc_level must be present for approved users

---

## merchants

- merchant_id must be unique
- category must not be null
- city must be populated

---

## orders

- order_id must be unique
- amount > 0
- status ∈ {approved, rejected}
- approved orders must have installments

---

## installments

- installment_id must be unique
- due_date must exist
- paid_date ≥ due_date if status = paid
- late_days ≥ 0
- status ∈ {paid, late, due}

---

## payments

- payment_id must be unique
- amount > 0
- paid_at must exist

---

## disputes_returns

- case_id must be unique
- amount ≥ 0
- outcome ∈ {open, resolved, refunded}

---

## Gold / KPIs

- approval_rate ∈ [0,1]
- late_rate ∈ [0,1]
- GMV ≥ 0
- sudden spikes (>3x day-over-day) must be flagged

---

## Data Quality Output

A daily table `data_quality_daily` is produced with:

- date
- table_name
- check_name
- status (pass / fail)
- metric_value
