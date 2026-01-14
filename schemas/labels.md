# Machine Learning Labels & Targets – BNPL Risk

This document defines the labels used for training and evaluating ML models.
These definitions are shared across data engineering, ML, and analytics.

---

## Primary Target: Late Payment Risk

### Objective
Predict whether a user will have a late installment in the near future.

---

## Label Definition: late_next_30d

A user is labeled as `late_next_30d = 1` if:

- At least one installment
- becomes due within the next 30 days
- AND is paid after the due date + grace period

Otherwise:
- `late_next_30d = 0`

---

## Grace Period
- grace_period_days = 2

Late is defined as:

paid_date > due_date + 2 days

---

## Prediction Time
- The label is computed at a daily level
- Using only information available **before** the prediction date

This avoids data leakage.

---

## Negative Examples
Users with:
- no late installments
- within the prediction window
are labeled as `0`.

---

## Secondary / Optional Labels

### late_next_installment
- Predict lateness on the very next installment

### fraud_flag (optional)
- Based on anomaly detection (IsolationForest)

### credit_limit_recommendation (future)
- Regression or rule-based target

---

## Important Constraints

- Labels must not use future information
- Definitions must remain stable across experiments
- Any change must be versioned
