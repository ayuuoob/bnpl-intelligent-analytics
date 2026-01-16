"""
ML Prediction Tool - Integration with trained BNPL risk models.

Provides access to:
- UC1: Late Payment Risk Model (bnpl_pay_late_risk_model.pkl)
- UC2: Trust Score Model (rf_bnpl_v1.joblib) with risk_engine logic
"""

import os
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool
import pandas as pd

# Model paths relative to the agent directory
ML_DIR = Path(__file__).resolve().parents[3] / "ML" / "to_test_agent"
UC1_MODEL_PATH = ML_DIR / "uc1_Late_Pay_Risk" / "model" / "bnpl_pay_late_risk_model.pkl"
UC2_MODEL_PATH = ML_DIR / "uc2_Risk_Score" / "model" / "rf_bnpl_v1.joblib"

# Cached models (loaded once)
_uc1_model = None
_uc2_artifact = None


def _load_uc1_model():
    """Load UC1 Late Payment Risk model (pickle)."""
    global _uc1_model
    if _uc1_model is None and UC1_MODEL_PATH.exists():
        import pickle
        with open(UC1_MODEL_PATH, "rb") as f:
            _uc1_model = pickle.load(f)
    return _uc1_model


def _load_uc2_model():
    """Load UC2 Risk Score model (joblib)."""
    global _uc2_artifact
    if _uc2_artifact is None and UC2_MODEL_PATH.exists():
        import joblib
        _uc2_artifact = joblib.load(UC2_MODEL_PATH)
    return _uc2_artifact


# =====================================================
# UC2 Risk Engine Logic (adapted from risk_engine.py)
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


def _score_and_decide(model, X_row: pd.DataFrame) -> tuple:
    """Calculate trust score and decision from model prediction."""
    risk_proba = model.predict_proba(X_row)[0][1]
    trust_score = round(100 * (1 - risk_proba))
    
    if trust_score >= DECISION_RULES["approve"]:
        decision = "APPROVED_3X"
    elif trust_score >= DECISION_RULES["limit"]:
        decision = "APPROVED_WITH_LIMIT"
    else:
        decision = "REJECTED_3X"
    
    return risk_proba, trust_score, decision


def _explain_score(row: pd.Series) -> str:
    """Generate human-readable explanation for the score."""
    reasons = []
    
    if row.get("late_rate_90d", 0) > EXPLANATION_THRESHOLDS["late_rate_high"]:
        reasons.append("Late payment history observed")
    
    if row.get("ontime_rate_90d", 1) < EXPLANATION_THRESHOLDS["ontime_low"]:
        reasons.append("Payments rarely on time")
    
    if row.get("active_plans", 0) >= EXPLANATION_THRESHOLDS["active_plans_high"]:
        reasons.append("Multiple active payment plans")
    
    if row.get("checkout_abandon_rate_30d", 0) > EXPLANATION_THRESHOLDS["checkout_high"]:
        reasons.append("Frequent checkout abandonment")
    
    if row.get("account_age_days", 999) < EXPLANATION_THRESHOLDS["account_young"]:
        reasons.append("New account")
    
    if len(reasons) == 0:
        return "Stable and reliable behavior"
    
    return " | ".join(reasons[:3])


class MLPredictionTool(BaseTool):
    """
    LangChain tool for ML-based risk predictions.
    
    Integrates with trained BNPL models for:
    - UC1: Late payment risk prediction
    - UC2: Trust score calculation with business logic
    
    Usage:
        tool = MLPredictionTool()
        
        # Late payment prediction
        result = tool.invoke({
            "prediction_type": "late_payment",
            "features": {"late_payment_rate_90d": 0.1, ...}
        })
        
        # Trust score
        result = tool.invoke({
            "prediction_type": "trust_score", 
            "features": {"account_age_days": 120, ...}
        })
    """
    
    name: str = "ml_predict"
    description: str = """Get ML predictions for BNPL risk assessment.
    
    Parameters:
    - prediction_type (required): "late_payment" or "trust_score"
    - features (required): Dictionary of client/transaction features
    
    For late_payment (UC1), key features include:
    - late_payment_rate_90d, avg_late_days_90d, on_time_payment_rate_90d
    - num_active_plans, account_age_days, kyc_level_num
    - dispute_count_90d, refund_count_90d
    
    For trust_score (UC2), key features include:
    - account_age_days, kyc_level_num, account_status_num
    - late_rate_90d, ontime_rate_90d, active_plans
    - orders_30d, amount_30d, disputes_90d, refunds_90d
    - checkout_abandon_rate_30d
    
    Returns structured prediction with confidence and explanation.
    """
    
    def _run(
        self,
        prediction_type: Literal["late_payment", "trust_score"],
        features: Dict[str, Any],
    ) -> str:
        """Synchronous prediction."""
        return asyncio.run(self._arun(prediction_type, features))
    
    async def _arun(
        self,
        prediction_type: Literal["late_payment", "trust_score"],
        features: Dict[str, Any],
    ) -> str:
        """Run ML prediction based on type."""
        if prediction_type == "late_payment":
            return self._predict_late_payment(features)
        elif prediction_type == "trust_score":
            return self._predict_trust_score(features)
        else:
            return f"Error: Unknown prediction_type '{prediction_type}'. Use 'late_payment' or 'trust_score'."
    
    def _predict_late_payment(self, features: Dict[str, Any]) -> str:
        """UC1: Predict late payment risk."""
        model = _load_uc1_model()
        
        if model is None:
            return self._format_error("UC1 model not found", UC1_MODEL_PATH)
        
        try:
            # Prepare features DataFrame
            X = pd.DataFrame([features])
            
            # Get prediction
            prediction = model.predict(X)[0]
            probability = model.predict_proba(X)[0][1] if hasattr(model, 'predict_proba') else None
            
            return self._format_late_payment_result(prediction, probability, features)
        except Exception as e:
            return f"Error in late payment prediction: {str(e)}"
    
    def _predict_trust_score(self, features: Dict[str, Any]) -> str:
        """UC2: Calculate trust score with business logic."""
        artifact = _load_uc2_model()
        
        if artifact is None:
            return self._format_error("UC2 model not found", UC2_MODEL_PATH)
        
        try:
            model = artifact["model"]
            expected_features = artifact["features"]
            
            # Prepare features DataFrame with expected order
            X = pd.DataFrame([features])
            
            # Ensure all required features exist
            for feat in expected_features:
                if feat not in X.columns:
                    X[feat] = 0  # Default missing features
            
            X = X[expected_features]
            
            # Score and decide
            risk_proba, trust_score, decision = _score_and_decide(model, X)
            
            # Generate explanation
            explanation = _explain_score(X.iloc[0])
            
            return self._format_trust_score_result(
                risk_proba, trust_score, decision, explanation
            )
        except Exception as e:
            return f"Error in trust score prediction: {str(e)}"
    
    def _format_late_payment_result(
        self, 
        prediction: int, 
        probability: Optional[float],
        features: Dict[str, Any]
    ) -> str:
        """Format UC1 late payment prediction as readable text."""
        is_late = bool(prediction)
        risk_level = "HIGH" if is_late else "LOW"
        emoji = "🔴" if is_late else "🟢"
        
        lines = [
            f"# Late Payment Risk: {emoji} {risk_level}",
            "",
            f"**Prediction**: {'LIKELY TO BE LATE' if is_late else 'LIKELY ON TIME'}",
        ]
        
        if probability is not None:
            lines.append(f"**Risk Probability**: {probability:.1%}")
        
        lines.extend([
            "",
            "_Model: UC1 Late Payment Risk (v1.0)_"
        ])
        
        return "\n".join(lines)
    
    def _format_trust_score_result(
        self,
        risk_proba: float,
        trust_score: int,
        decision: str,
        explanation: str
    ) -> str:
        """Format UC2 trust score as readable text."""
        # Color-code decision
        if decision == "APPROVED_3X":
            emoji = "🟢"
            decision_text = "APPROVED (3X)"
        elif decision == "APPROVED_WITH_LIMIT":
            emoji = "🟡"
            decision_text = "APPROVED WITH LIMIT"
        else:
            emoji = "🔴"
            decision_text = "REJECTED"
        
        lines = [
            f"# Trust Score: {trust_score}/100 {emoji}",
            "",
            f"**Decision**: {decision_text}",
            f"**Risk Probability**: {risk_proba:.1%}",
            "",
            f"**Assessment**: {explanation}",
            "",
            "_Model: UC2 Risk Score (rf_bnpl_v1)_"
        ]
        
        return "\n".join(lines)
    
    def _format_error(self, message: str, path: Path) -> str:
        """Format error message."""
        return f"⚠️ **Error**: {message}\n\nExpected model at: `{path}`\n\nPlease ensure the ML models are in the correct location."


# Convenience function for direct import
def get_ml_tool() -> MLPredictionTool:
    """Get an instance of the ML prediction tool."""
    return MLPredictionTool()
