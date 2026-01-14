# Data Consumption Contracts – BI & AI Agents

This document defines which tables and fields can be consumed
by dashboards and AI agents. No other tables should be queried directly.

---

## BI Dashboards (Read-only)

### Allowed Tables
- kpi_daily
- cohorts_signup_week
- merchant_features_daily

### KPI Definitions
- GMV = sum(order amount for approved orders)
- Approval Rate = approved_orders / total_orders
- Late Rate = late_installments / total_installments
- Dispute Rate = disputes / total_orders

### Refresh Frequency
- Daily (T+1)

---

## AI Agents (Copilot)

### Allowed Tables
- user_features_daily
- merchant_features_daily
- kpi_daily
- data_quality_daily

### Query Rules
- No direct access to Bronze or Silver
- Aggregations only on Gold tables
- Time filters must be explicit

---

## Output Requirements (Agents)

Every agent response must include:
1. Short summary (2–3 lines)
2. Evidence (table or metric)
3. Suggested action
4. Data source used (table name)

---

## Security & Governance

- PII fields are masked for non-authorized roles
- Role-based filtering enforced at query level
- All queries must be logged
