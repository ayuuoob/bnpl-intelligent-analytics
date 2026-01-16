"""
Risk Tool - MCP wrapper for risk scoring (optional).

Provides access to ML risk scores for users and orders.
This is an optional tool that integrates with the ML layer.
"""

import asyncio
from typing import Optional, Literal
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool

from .mcp_client import get_mcp_client, MCPResponse


class RiskScore(BaseModel):
    """Risk score result."""
    
    entity_type: str
    entity_id: str
    score: float = Field(ge=0, le=1)
    band: Literal["low", "medium", "high", "very_high"]
    reasons: list[str] = Field(default_factory=list)
    model_version: Optional[str] = None


class RiskTool(BaseTool):
    """
    LangChain tool for fetching ML risk scores via MCP.
    
    This is an OPTIONAL tool - only available if ML scoring
    service is configured. Returns risk score, band, and
    contributing factors for users or orders.
    
    Usage:
        tool = RiskTool()
        result = tool.invoke({
            "entity_type": "user",
            "entity_id": "U12345"
        })
    """
    
    name: str = "risk_score"
    description: str = """Get ML risk score for a user or order.
    
    Parameters:
    - entity_type (required): "user" or "order"
    - entity_id (required): The ID of the entity to score
    
    Returns:
    - score: 0.0 to 1.0 (higher = more risky)
    - band: "low", "medium", "high", or "very_high"
    - reasons: List of top contributing factors
    
    Use this tool when:
    - Analyzing individual user risk profiles
    - Investigating specific risky orders
    - Understanding why a segment is high-risk
    
    Note: This tool requires ML scoring service to be available.
    """
    
    def _run(
        self,
        entity_type: Literal["user", "order"],
        entity_id: str,
    ) -> str:
        """Synchronous risk scoring."""
        return asyncio.run(self._arun(entity_type, entity_id))
    
    async def _arun(
        self,
        entity_type: Literal["user", "order"],
        entity_id: str,
    ) -> str:
        """
        Fetch risk score from MCP server or ML model.
        
        Priority: MCP -> ML Model -> Mock score
        """
        if entity_type not in ["user", "order"]:
            return f"Error: entity_type must be 'user' or 'order', got '{entity_type}'"
        
        if not entity_id:
            return "Error: entity_id is required"
        
        # Try MCP first
        client = get_mcp_client()
        params = {
            "entity": entity_type,
            "id": entity_id,
        }
        
        response: MCPResponse = await client.call("risk.score", params)
        
        if response.success and response.data:
            return self._format_result(response.data)
        
        # Try ML model for user scoring
        if entity_type == "user":
            ml_result = self._get_ml_score(entity_id)
            if ml_result:
                return self._format_result(ml_result)
        
        # Fallback to mock score
        mock_result = self._get_mock_score(entity_type, entity_id)
        return self._format_result(mock_result)
    
    def _format_result(self, data: dict) -> str:
        """Format risk score as readable text."""
        score = data.get("score", 0)
        band = data.get("band", "unknown")
        reasons = data.get("reasons", [])
        
        # Color-code band
        band_emoji = {
            "low": "🟢",
            "medium": "🟡", 
            "high": "🟠",
            "very_high": "🔴",
        }
        
        lines = [
            f"# Risk Score: {score:.2f} {band_emoji.get(band, '')} ({band.upper()})",
            "",
        ]
        
        if reasons:
            lines.append("## Contributing Factors:")
            for i, reason in enumerate(reasons, 1):
                lines.append(f"{i}. {reason}")
        else:
            lines.append("No specific risk factors identified.")
        
        model_version = data.get("model_version")
        if model_version:
            lines.append(f"\n_Model version: {model_version}_")
        
        return "\n".join(lines)
    
    def _get_mock_score(
        self,
        entity_type: str,
        entity_id: str,
    ) -> dict:
        """Generate mock risk score for demo/testing."""
        import random
        import hashlib
        
        # Use entity_id hash for consistent mock scores
        hash_val = int(hashlib.md5(entity_id.encode()).hexdigest()[:8], 16)
        random.seed(hash_val)
        
        score = random.uniform(0, 1)
        
        if score < 0.3:
            band = "low"
        elif score < 0.5:
            band = "medium"
        elif score < 0.75:
            band = "high"
        else:
            band = "very_high"
        
        # Generate mock reasons based on score
        all_reasons = [
            "High late payment rate in last 90 days",
            "Multiple device changes detected",
            "New account (less than 30 days)",
            "Previous dispute history",
            "High-risk merchant category",
            "Large order amount relative to history",
            "Unusual transaction pattern",
            "Low on-time payment rate",
        ]
        
        num_reasons = min(int(score * 5) + 1, len(all_reasons))
        reasons = random.sample(all_reasons, num_reasons)
        
        return {
            "score": score,
            "band": band,
            "reasons": reasons,
            "model_version": "mock-v1.0",
        }
    
    def _get_ml_score(self, user_id: str) -> Optional[dict]:
        """
        Get ML-based risk score using UC2 model.
        
        Returns standardized risk score dict or None if model unavailable.
        """
        try:
            from .ml_tool import _load_uc2_model, _score_and_decide, _explain_score
            import pandas as pd
            
            artifact = _load_uc2_model()
            if artifact is None:
                return None
            
            model = artifact["model"]
            features = artifact["features"]
            
            # Generate synthetic features for this user
            # In production, these would come from the database
            import hashlib
            hash_val = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
            
            # Create reasonable feature values based on user hash
            seed_val = hash_val % 1000 / 1000
            
            client_features = {
                "account_age_days": int(30 + seed_val * 400),
                "kyc_level_num": 1 if seed_val < 0.3 else 2,
                "account_status_num": 1,
                "late_rate_90d": seed_val * 0.5,
                "ontime_rate_90d": 1 - seed_val * 0.5,
                "active_plans": int(seed_val * 4),
                "orders_30d": int(1 + seed_val * 10),
                "amount_30d": 500 + seed_val * 3000,
                "disputes_90d": int(seed_val * 3),
                "refunds_90d": int(seed_val * 2),
                "checkout_abandon_rate_30d": seed_val * 0.6,
            }
            
            # Prepare features DataFrame
            X = pd.DataFrame([client_features])
            
            # Ensure all required features exist
            for feat in features:
                if feat not in X.columns:
                    X[feat] = 0
            
            X = X[features]
            
            # Get prediction
            risk_proba, trust_score, decision = _score_and_decide(model, X)
            explanation = _explain_score(X.iloc[0])
            
            # Convert to standardized format
            if trust_score >= 70:
                band = "low"
            elif trust_score >= 40:
                band = "medium"
            elif trust_score >= 20:
                band = "high"
            else:
                band = "very_high"
            
            return {
                "score": risk_proba,
                "band": band,
                "reasons": explanation.split(" | ") if explanation else [],
                "model_version": "UC2-rf_bnpl_v1",
                "trust_score": trust_score,
                "decision": decision,
            }
            
        except Exception as e:
            # Silently fall back to mock if ML fails
            return None
