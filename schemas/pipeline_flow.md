# BNPL Daily Data Pipeline Flow

This document defines the execution order of data pipelines.
All jobs run once per day (T+1) using data up to end of day T.

---

## Step 1 – Bronze Ingestion
- Append raw BNPL events to `bnpl_events.json`
- No transformation
- No deduplication

Output:
- Raw event log (source of truth)

---

## Step 2 – Silver Transformations
- Read Bronze events
- Deduplicate by event_id
- Normalize timestamps (UTC)
- Build clean operational tables:
  - users
  - merchants
  - orders
  - installments
  - payments
  - disputes_returns

Output:
- Trusted operational data

---

## Step 3 – Data Quality Checks
- Validate schema and constraints
- Check freshness and volume
- Flag anomalies (spikes, drops)

Output:
- `data_quality_daily` table

---

## Step 4 – Gold Aggregations & Features
- Compute user_features_daily
- Compute merchant_features_daily
- Compute kpi_daily
- Compute cohorts_signup_week

Output:
- Analytics-ready tables

---

## Step 5 – Consumption
- Dashboards read Gold tables
- ML models train on Gold snapshots
- AI agents query Gold tables only

---

## Failure Handling
- If Silver fails → Gold does not run
- If Quality fails → alerts raised
- Bronze is never modified

---

## Scheduling Notes
- All steps are idempotent
- Pipelines can be re-run safely
- Historical recomputation is possible
