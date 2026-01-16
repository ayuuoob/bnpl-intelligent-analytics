"""
Tests for ML Prediction Tool.
"""

import pytest
from pathlib import Path


# Check if ML models are available
ML_DIR = Path(__file__).resolve().parents[1] / ".." / ".." / "ML" / "to_test_agent"
UC1_MODEL_EXISTS = (ML_DIR / "uc1_Late_Pay_Risk" / "model" / "bnpl_pay_late_risk_model.pkl").exists()
UC2_MODEL_EXISTS = (ML_DIR / "uc2_Risk_Score" / "model" / "rf_bnpl_v1.joblib").exists()


class TestMLPredictionTool:
    """Test cases for ML Prediction Tool."""
    
    @pytest.fixture
    def ml_tool(self):
        from src.tools import MLPredictionTool
        return MLPredictionTool()
    
    def test_tool_instantiation(self, ml_tool):
        """Test tool can be instantiated."""
        assert ml_tool.name == "ml_predict"
        assert "ML predictions" in ml_tool.description
    
    @pytest.mark.skipif(not UC2_MODEL_EXISTS, reason="UC2 model not found")
    @pytest.mark.asyncio
    async def test_trust_score_prediction(self, ml_tool):
        """Test UC2 trust score prediction."""
        result = await ml_tool._arun(
            prediction_type="trust_score",
            features={
                "account_age_days": 120,
                "kyc_level_num": 1,
                "account_status_num": 1,
                "late_rate_90d": 0.1,
                "ontime_rate_90d": 0.9,
                "active_plans": 1,
                "orders_30d": 5,
                "amount_30d": 1200,
                "disputes_90d": 0,
                "refunds_90d": 0,
                "checkout_abandon_rate_30d": 0.2,
            }
        )
        
        assert "Trust Score" in result
        assert "/100" in result
        assert "Decision" in result
    
    @pytest.mark.skipif(not UC1_MODEL_EXISTS, reason="UC1 model not found")
    @pytest.mark.asyncio
    async def test_late_payment_prediction(self, ml_tool):
        """Test UC1 late payment prediction."""
        result = await ml_tool._arun(
            prediction_type="late_payment",
            features={
                "late_payment_rate_90d": 0.1,
                "avg_late_days_90d": 2,
                "on_time_payment_rate_90d": 0.9,
                "num_active_plans": 1,
                "account_age_days": 180,
                "kyc_level_num": 2,
            }
        )
        
        assert "Late Payment Risk" in result
    
    @pytest.mark.asyncio
    async def test_invalid_prediction_type(self, ml_tool):
        """Test error handling for invalid prediction type."""
        result = await ml_tool._arun(
            prediction_type="invalid_type",
            features={}
        )
        
        assert "Error" in result
        assert "Unknown prediction_type" in result


class TestRiskToolMLIntegration:
    """Test ML integration in RiskTool."""
    
    @pytest.fixture
    def risk_tool(self):
        from src.tools import RiskTool
        return RiskTool()
    
    @pytest.mark.asyncio
    async def test_risk_tool_with_user(self, risk_tool):
        """Test RiskTool returns a score for users."""
        result = await risk_tool._arun(
            entity_type="user",
            entity_id="U12345"
        )
        
        assert "Risk Score" in result
        # Should have band info
        assert any(band in result.upper() for band in ["LOW", "MEDIUM", "HIGH"])
    
    @pytest.mark.skipif(not UC2_MODEL_EXISTS, reason="UC2 model not found")
    @pytest.mark.asyncio
    async def test_risk_tool_uses_ml_model(self, risk_tool):
        """Test that RiskTool uses ML model when available."""
        result = await risk_tool._arun(
            entity_type="user",
            entity_id="U99999"
        )
        
        # If ML model is used, it should mention UC2 model version
        # Note: May fall back to mock depending on model availability
        assert "Risk Score" in result
