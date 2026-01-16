import joblib
import pandas as pd

# =====================================================
# 1️⃣ Chargement du modèle (UNE FOIS)
# =====================================================

ARTIFACT_PATH = "rf_bnpl_baseline_v1.joblib"

artifact = joblib.load(ARTIFACT_PATH)

rf_model = artifact["model"]
FEATURES = artifact["features"]
TARGET = artifact["target"]

# =====================================================
# 2️⃣ Paramètres business (ajustables sans réentraîner)
# =====================================================

DECISION_RULES = {
    "approve": 70,
    "limit": 40
}

EXPLANATION_THRESHOLDS = {
    "late_rate_high": 0.3,
    "ontime_low": 0.7,
    "checkout_high": 0.4,
    "active_plans_high": 3,
    "account_young": 60
}

# =====================================================
# 3️⃣ Trust Score + Décision
# =====================================================

def score_and_decide(X_row: pd.DataFrame):
    """
    X_row : DataFrame avec UNE ligne, colonnes = FEATURES
    """

    risk_proba = rf_model.predict_proba(X_row)[0][1]
    trust_score = round(100 * (1 - risk_proba))

    if trust_score >= DECISION_RULES["approve"]:
        decision = "APPROVED_3X"
    elif trust_score >= DECISION_RULES["limit"]:
        decision = "APPROVED_WITH_LIMIT"
    else:
        decision = "REJECTED_3X"

    return risk_proba, trust_score, decision

# =====================================================
# 4️⃣ Explicabilité métier (phrases claires)
# =====================================================

def explain_score(row: pd.Series):
    reasons = []

    if row["late_rate_90d"] > EXPLANATION_THRESHOLDS["late_rate_high"]:
        reasons.append("Retards de paiement observés")

    if row["ontime_rate_90d"] < EXPLANATION_THRESHOLDS["ontime_low"]:
        reasons.append("Paiements rarement à temps")

    if row["active_plans"] >= EXPLANATION_THRESHOLDS["active_plans_high"]:
        reasons.append("Plusieurs paiements en cours")

    if row["checkout_abandon_rate_30d"] > EXPLANATION_THRESHOLDS["checkout_high"]:
        reasons.append("Abandons fréquents au paiement")

    if row["account_age_days"] < EXPLANATION_THRESHOLDS["account_young"]:
        reasons.append("Compte récent")

    if len(reasons) == 0:
        return "Comportement stable et fiable"

    return " | ".join(reasons[:3])

# =====================================================
# 5️⃣ FONCTION PRINCIPALE (celle que l’agent appelle)
# =====================================================

def predict_client(client_features: dict):
    """
    client_features : dict avec TOUTES les FEATURES attendues
    """

    # Mise en DataFrame avec ordre strict
    X = pd.DataFrame([client_features])[FEATURES]

    # Score + décision
    risk_proba, trust_score, decision = score_and_decide(X)

    # Explication
    explanation = explain_score(X.iloc[0])

    return {
        "risk_probability": round(risk_proba, 3),
        "trust_score": trust_score,
        "decision": decision,
        "explanation": explanation
    }
