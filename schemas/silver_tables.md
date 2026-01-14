# Silver Layer – Clean Operational Tables

Silver tables are cleaned, normalized representations of BNPL business entities.
They are derived from Bronze events and are trusted across BI, ML, and AI agents.

---

## users

- user_id (string)
- signup_date (date)
- kyc_level (string)
- city (string)
- device_fingerprint (string)
- account_status (active / blocked)

---

## merchants

- merchant_id (string)
- merchant_name (string)
- category (string)
- city (string)
- risk_tier (low / medium / high)

---

## orders

- order_id (string)
- user_id (string)
- merchant_id (string)
- amount (float)
- currency (string)
- status (approved / rejected)
- created_at (timestamp)

---

## installments

- installment_id (string)
- order_id (string)
- due_date (date)
- paid_date (date, nullable)
- status (paid / late / due)
- late_days (int)

---

## payments

- payment_id (string)
- installment_id (string)
- amount (float)
- payment_channel (card / wallet)
- paid_at (timestamp)

---

## disputes_returns

- case_id (string)
- order_id (string)
- reason (string)
- amount (float)
- outcome (open / resolved / refunded)
