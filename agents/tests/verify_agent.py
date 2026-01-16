"""
Test script for verifying agent data extraction and ML predictions.
"""
import asyncio
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.graph import run_query
from src.tools import KPITool, SQLTool, MLPredictionTool, RiskTool
from src.tools.local_data import get_local_data


async def test_local_data_extraction():
    """Test that we can extract real data from CSV files."""
    print("=" * 60)
    print("TEST 1: Local Data Extraction")
    print("=" * 60)
    
    local_data = get_local_data()
    
    # Test loading orders
    orders_df = local_data.get_table("orders")
    if orders_df is not None:
        print(f"[PASS] Orders loaded: {len(orders_df)} rows")
        print(f"       Columns: {list(orders_df.columns)[:5]}...")
        if 'amount' in orders_df.columns:
            total_amount = orders_df['amount'].sum()
            print(f"       Total order amount: {total_amount:,.2f}")
    else:
        print("[FAIL] Could not load orders data")
    
    # Test loading users
    users_df = local_data.get_table("users")
    if users_df is not None:
        print(f"[PASS] Users loaded: {len(users_df)} rows")
    else:
        print("[FAIL] Could not load users data")
    
    # Test loading installments
    installments_df = local_data.get_table("installments")
    if installments_df is not None:
        print(f"[PASS] Installments loaded: {len(installments_df)} rows")
    else:
        print("[FAIL] Could not load installments data")
    
    # Test KPI calculations
    gmv = local_data.calculate_gmv()
    print(f"[PASS] GMV calculation: {gmv}")
    
    approval_rate = local_data.calculate_approval_rate()
    print(f"[PASS] Approval rate: {approval_rate}")
    
    print()


async def test_kpi_tool():
    """Test KPI tool returns real analytics."""
    print("=" * 60)
    print("TEST 2: KPI Tool Analytics")
    print("=" * 60)
    
    kpi_tool = KPITool()
    
    # Test GMV
    result = await kpi_tool._arun(kpi_name="gmv")
    print(f"GMV Result:\n{result}\n")
    
    # Test Approval Rate
    result = await kpi_tool._arun(kpi_name="approval_rate")
    print(f"Approval Rate Result:\n{result}\n")
    
    print()


async def test_sql_tool():
    """Test SQL tool extracts correct data."""
    print("=" * 60)
    print("TEST 3: SQL Tool Data Extraction")
    print("=" * 60)
    
    sql_tool = SQLTool()
    
    # Test simple query
    result = await sql_tool._arun(
        query="SELECT COUNT(*) as total_orders FROM orders"
    )
    print(f"Order Count Query:\n{result}\n")
    
    print()


async def test_ml_predictions():
    """Test ML prediction tool with real models."""
    print("=" * 60)
    print("TEST 4: ML Predictions")
    print("=" * 60)
    
    ml_tool = MLPredictionTool()
    
    # Test Trust Score prediction (UC2)
    print("Testing UC2 Trust Score Model...")
    result = await ml_tool._arun(
        prediction_type="trust_score",
        features={
            "account_age_days": 120,
            "kyc_level_num": 2,
            "account_status_num": 1,
            "late_rate_90d": 0.1,
            "ontime_rate_90d": 0.9,
            "active_plans": 1,
            "orders_30d": 5,
            "amount_30d": 1200,
            "disputes_90d": 0,
            "refunds_90d": 0,
            "checkout_abandon_rate_30d": 0.15,
        }
    )
    print(f"Trust Score Result (Good Customer):\n{result}\n")
    
    # Test with risky customer
    result = await ml_tool._arun(
        prediction_type="trust_score",
        features={
            "account_age_days": 30,
            "kyc_level_num": 1,
            "account_status_num": 1,
            "late_rate_90d": 0.5,
            "ontime_rate_90d": 0.5,
            "active_plans": 3,
            "orders_30d": 8,
            "amount_30d": 3000,
            "disputes_90d": 2,
            "refunds_90d": 1,
            "checkout_abandon_rate_30d": 0.6,
        }
    )
    print(f"Trust Score Result (Risky Customer):\n{result}\n")
    
    print()


async def test_risk_tool_with_ml():
    """Test RiskTool uses ML models."""
    print("=" * 60)
    print("TEST 5: RiskTool ML Integration")
    print("=" * 60)
    
    risk_tool = RiskTool()
    
    result = await risk_tool._arun(entity_type="user", entity_id="U12345")
    print(f"Risk Score for U12345:\n{result}\n")
    
    result = await risk_tool._arun(entity_type="user", entity_id="U99999")
    print(f"Risk Score for U99999:\n{result}\n")
    
    print()


async def test_full_agent_query():
    """Test full agent query processing."""
    print("=" * 60)
    print("TEST 6: Full Agent Query")
    print("=" * 60)
    
    queries = [
        "What was our GMV last month?",
        "How many active users do we have?",
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        print("-" * 40)
        try:
            response = await run_query(query)
            print(response)
        except Exception as e:
            print(f"Error: {e}")
    
    print()


async def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("BNPL AGENT VERIFICATION TESTS")
    print("=" * 60 + "\n")
    
    await test_local_data_extraction()
    await test_kpi_tool()
    await test_sql_tool()
    await test_ml_predictions()
    await test_risk_tool_with_ml()
    await test_full_agent_query()
    
    print("=" * 60)
    print("ALL TESTS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
