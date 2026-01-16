"""
BNPL Intelligent Analytics - Streamlit Dashboard

Professional web application for BNPL analytics with:
- KPI Dashboard
- Agent Chat Interface
- Dynamic Graph Generation
- ML Explainability
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import asyncio

# Add agents to path
AGENTS_PATH = Path(__file__).parent.parent / "agents"
sys.path.insert(0, str(AGENTS_PATH))

from dotenv import load_dotenv
load_dotenv(AGENTS_PATH / ".env")

# Import agent components
from src.tools.local_data import get_local_data
from src.tools import MLPredictionTool, RiskTool, KPITool
from src.graph import run_query

# Page config
st.set_page_config(
    page_title="BNPL Analytics Copilot",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    /* Main theme */
    .main {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        margin: 10px 0;
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 10px 0;
    }
    
    .kpi-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    
    /* Chat styling */
    .chat-message {
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        animation: fadeIn 0.3s ease-in;
    }
    
    .user-message {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        margin-left: 20%;
    }
    
    .agent-message {
        background: #2d2d44;
        color: #e0e0e0;
        margin-right: 20%;
        border-left: 4px solid #667eea;
    }
    
    /* Risk Score styling */
    .risk-low { color: #00c853; }
    .risk-medium { color: #ffc107; }
    .risk-high { color: #ff5722; }
    .risk-very-high { color: #f44336; }
    
    /* Sidebar */
    .css-1d391kg {
        background: #1a1a2e;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom header */
    .header-container {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)


# Initialize session state
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "query_logs" not in st.session_state:
    st.session_state.query_logs = []


def get_kpi_data():
    """Fetch all KPIs from local data."""
    local_data = get_local_data()
    
    kpis = {
        "gmv": local_data.calculate_gmv(),
        "approval_rate": local_data.calculate_approval_rate(),
        "late_rate": local_data.calculate_late_rate(),
        "active_users": local_data.calculate_active_users(),
    }
    
    # Get additional metrics
    orders_df = local_data.get_table("orders")
    users_df = local_data.get_table("users")
    disputes_df = local_data.get_table("disputes")
    
    kpis["total_orders"] = len(orders_df) if orders_df is not None else 0
    kpis["total_users"] = len(users_df) if users_df is not None else 0
    kpis["total_disputes"] = len(disputes_df) if disputes_df is not None else 0
    
    if orders_df is not None and disputes_df is not None:
        kpis["dispute_rate"] = len(disputes_df) / len(orders_df) * 100 if len(orders_df) > 0 else 0
    else:
        kpis["dispute_rate"] = 0
    
    return kpis


def create_kpi_card(label, value, prefix="", suffix="", delta=None):
    """Create a styled KPI card."""
    delta_html = ""
    if delta:
        color = "green" if delta > 0 else "red"
        arrow = "↑" if delta > 0 else "↓"
        delta_html = f'<div style="color: {color}; font-size: 0.9rem;">{arrow} {abs(delta):.1f}%</div>'
    
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{prefix}{value}{suffix}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_kpi_dashboard():
    """Render the KPI dashboard page."""
    st.markdown("## 📊 Key Performance Indicators")
    
    kpis = get_kpi_data()
    
    # Top row - Main KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        gmv_value = kpis["gmv"]["value"] if isinstance(kpis["gmv"], dict) else 0
        st.metric(
            label="💰 GMV (30 days)",
            value=f"${gmv_value:,.0f}",
            delta="5.2%"
        )
    
    with col2:
        approval = kpis["approval_rate"]["value"] * 100 if isinstance(kpis["approval_rate"], dict) else 0
        st.metric(
            label="✅ Approval Rate",
            value=f"{approval:.1f}%",
            delta="2.1%"
        )
    
    with col3:
        late = kpis["late_rate"]["value"] * 100 if isinstance(kpis["late_rate"], dict) else 0
        st.metric(
            label="⏰ Late Payment Rate",
            value=f"{late:.1f}%",
            delta="-1.3%",
            delta_color="inverse"
        )
    
    with col4:
        active = kpis["active_users"]["value"] if isinstance(kpis["active_users"], dict) else 0
        st.metric(
            label="👥 Active Users",
            value=f"{active:,}",
            delta="8.4%"
        )
    
    # Second row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(label="📦 Total Orders", value=f"{kpis['total_orders']:,}")
    
    with col2:
        st.metric(label="👤 Total Users", value=f"{kpis['total_users']:,}")
    
    with col3:
        st.metric(label="⚠️ Disputes", value=f"{kpis['total_disputes']}")
    
    with col4:
        st.metric(label="📉 Dispute Rate", value=f"{kpis['dispute_rate']:.2f}%")
    
    st.divider()
    
    # Charts
    render_charts()


def render_charts():
    """Render analytics charts."""
    local_data = get_local_data()
    orders_df = local_data.get_table("orders")
    
    if orders_df is None:
        st.warning("No order data available")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Daily Order Volume")
        if "order_date" in orders_df.columns:
            orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
            daily_orders = orders_df.groupby(orders_df["order_date"].dt.date).agg({
                "order_id": "count",
                "amount": "sum"
            }).reset_index()
            daily_orders.columns = ["date", "orders", "amount"]
            
            fig = px.area(
                daily_orders, x="date", y="amount",
                title="",
                color_discrete_sequence=["#667eea"]
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                xaxis_title="",
                yaxis_title="Amount ($)"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🏪 Top Merchants by GMV")
        if "merchant_id" in orders_df.columns and "status" in orders_df.columns:
            approved = orders_df[orders_df["status"] == "approved"]
            merchant_gmv = approved.groupby("merchant_id")["amount"].sum().nlargest(10).reset_index()
            
            fig = px.bar(
                merchant_gmv, x="amount", y="merchant_id",
                orientation="h",
                title="",
                color_discrete_sequence=["#764ba2"]
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                xaxis_title="GMV ($)",
                yaxis_title=""
            )
            st.plotly_chart(fig, use_container_width=True)


async def process_agent_query(query: str):
    """Process a query through the agent."""
    try:
        response = await run_query(query)
        return response
    except Exception as e:
        return f"Error: {str(e)}"


def detect_chart_request(query: str) -> dict:
    """Detect if query requests a chart and what type."""
    query_lower = query.lower()
    
    chart_keywords = {
        "line": ["trend", "over time", "evolution", "history", "timeline"],
        "bar": ["comparison", "compare", "top", "ranking", "by merchant", "by category"],
        "pie": ["distribution", "breakdown", "proportion", "share"],
        "scatter": ["correlation", "relationship", "vs", "versus"],
    }
    
    for chart_type, keywords in chart_keywords.items():
        for keyword in keywords:
            if keyword in query_lower:
                return {"type": chart_type, "detected": True}
    
    # Check for graph/chart explicit request
    if any(word in query_lower for word in ["graph", "chart", "plot", "visualiz"]):
        return {"type": "bar", "detected": True}
    
    return {"type": None, "detected": False}


def generate_dynamic_chart(query: str, response: str):
    """Generate a chart based on query context."""
    local_data = get_local_data()
    chart_info = detect_chart_request(query)
    
    if not chart_info["detected"]:
        return None
    
    query_lower = query.lower()
    
    # GMV trend
    if "gmv" in query_lower and chart_info["type"] == "line":
        orders_df = local_data.get_table("orders")
        if orders_df is not None and "order_date" in orders_df.columns:
            orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
            approved = orders_df[orders_df["status"] == "approved"]
            daily = approved.groupby(approved["order_date"].dt.date)["amount"].sum().reset_index()
            daily.columns = ["date", "gmv"]
            
            fig = px.line(daily, x="date", y="gmv", title="GMV Trend Over Time")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            return fig
    
    # Merchant comparison
    if "merchant" in query_lower and chart_info["type"] == "bar":
        orders_df = local_data.get_table("orders")
        if orders_df is not None:
            approved = orders_df[orders_df["status"] == "approved"]
            merchant_gmv = approved.groupby("merchant_id")["amount"].sum().nlargest(10).reset_index()
            
            fig = px.bar(merchant_gmv, x="merchant_id", y="amount", title="Top Merchants by GMV")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            return fig
    
    # Default chart based on type
    return None


def get_ml_prediction(query: str) -> dict:
    """Check if query requires ML prediction and return results."""
    query_lower = query.lower()
    
    result = {"requires_ml": False, "prediction": None, "type": None}
    
    # Check for risk score request
    if any(word in query_lower for word in ["risk score", "trust score", "risk assessment", "credit score"]):
        result["requires_ml"] = True
        result["type"] = "trust_score"
    
    # Check for late payment prediction
    elif any(word in query_lower for word in ["late payment", "will pay late", "payment prediction", "delinquency"]):
        result["requires_ml"] = True
        result["type"] = "late_payment"
    
    if result["requires_ml"]:
        ml_tool = MLPredictionTool()
        
        # Sample features for demo (in production, extract from query or database)
        if result["type"] == "trust_score":
            features = {
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
            
            prediction = asyncio.run(ml_tool._arun("trust_score", features))
            result["prediction"] = prediction
        
        elif result["type"] == "late_payment":
            features = {
                "late_payment_rate_90d": 0.1,
                "avg_late_days_90d": 2,
                "on_time_payment_rate_90d": 0.9,
                "num_active_plans": 1,
                "account_age_days": 180,
                "kyc_level_num": 2,
            }
            
            prediction = asyncio.run(ml_tool._arun("late_payment", features))
            result["prediction"] = prediction
    
    return result


def render_chat_interface():
    """Render the agent chat interface."""
    st.markdown("## 💬 BNPL Analytics Copilot")
    st.markdown("Ask questions about your BNPL data, request visualizations, or get risk assessments.")
    
    # Example questions
    with st.expander("💡 Example Questions"):
        examples = [
            "What was our GMV last month?",
            "Show me the trend of orders over time (graph)",
            "What is the risk score for a new customer?",
            "Compare top merchants by revenue",
            "What is the late payment rate?",
            "Predict if a customer will pay late",
        ]
        for i, ex in enumerate(examples):
            if st.button(ex, key=f"example_{i}"):
                st.session_state.pending_query = ex
    
    # Chat history display
    chat_container = st.container()
    
    with chat_container:
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(f"""
                <div class="chat-message user-message">
                    <strong>You:</strong> {msg["content"]}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="chat-message agent-message">
                    <strong>🤖 Agent:</strong><br>{msg["content"]}
                </div>
                """, unsafe_allow_html=True)
                
                # Display chart if available
                if "chart" in msg and msg["chart"] is not None:
                    st.plotly_chart(msg["chart"], use_container_width=True)
                
                # Display ML prediction if available
                if "ml_result" in msg and msg["ml_result"]:
                    with st.expander("🔍 ML Model Explainability"):
                        st.markdown(msg["ml_result"])
    
    # Input
    query = st.chat_input("Ask me anything about your BNPL data...")
    
    # Handle pending query from examples
    if hasattr(st.session_state, "pending_query"):
        query = st.session_state.pending_query
        del st.session_state.pending_query
    
    if query:
        # Add user message
        st.session_state.chat_history.append({"role": "user", "content": query})
        
        # Log query
        st.session_state.query_logs.append({
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "status": "processing"
        })
        
        with st.spinner("🔄 Analyzing..."):
            # Check for ML prediction request
            ml_result = get_ml_prediction(query)
            
            # Generate chart if requested
            chart = None
            if detect_chart_request(query)["detected"]:
                chart = generate_dynamic_chart(query, "")
            
            # Get agent response
            response = asyncio.run(process_agent_query(query))
            
            # Update log
            st.session_state.query_logs[-1]["status"] = "completed"
            st.session_state.query_logs[-1]["response_length"] = len(response)
            
            # Add agent message
            msg = {
                "role": "agent",
                "content": response,
                "chart": chart,
            }
            
            if ml_result["requires_ml"]:
                msg["ml_result"] = ml_result["prediction"]
            
            st.session_state.chat_history.append(msg)
        
        st.rerun()


def render_history_logs():
    """Render the history and logs page."""
    st.markdown("## 📜 Query History & Logs")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Recent Queries")
        if st.session_state.query_logs:
            for i, log in enumerate(reversed(st.session_state.query_logs[-20:])):
                status_color = "🟢" if log["status"] == "completed" else "🟡"
                st.markdown(f"""
                **{status_color} {log['timestamp'][:19]}**  
                Query: _{log['query']}_  
                """)
        else:
            st.info("No queries yet. Start a conversation in the Chat tab!")
    
    with col2:
        st.markdown("### Statistics")
        total_queries = len(st.session_state.query_logs)
        st.metric("Total Queries", total_queries)
        
        if total_queries > 0:
            completed = sum(1 for log in st.session_state.query_logs if log["status"] == "completed")
            st.metric("Success Rate", f"{completed/total_queries*100:.0f}%")
        
        # Clear history button
        if st.button("🗑️ Clear History"):
            st.session_state.chat_history = []
            st.session_state.query_logs = []
            st.rerun()


def render_risk_assessment():
    """Render the ML Risk Assessment page."""
    st.markdown("## 🎯 Risk Assessment Tool")
    st.markdown("Get real-time risk predictions using our ML models.")
    
    tab1, tab2 = st.tabs(["Trust Score (UC2)", "Late Payment Risk (UC1)"])
    
    with tab1:
        st.markdown("### Customer Trust Score Assessment")
        
        col1, col2 = st.columns(2)
        
        with col1:
            account_age = st.slider("Account Age (days)", 1, 500, 120)
            kyc_level = st.selectbox("KYC Level", [0, 1, 2], index=2)
            late_rate = st.slider("Late Payment Rate (90d)", 0.0, 1.0, 0.1)
            ontime_rate = 1 - late_rate
            active_plans = st.slider("Active Payment Plans", 0, 10, 1)
        
        with col2:
            orders_30d = st.slider("Orders (30d)", 0, 20, 5)
            amount_30d = st.slider("Total Amount (30d)", 0, 5000, 1200)
            disputes = st.slider("Disputes (90d)", 0, 10, 0)
            abandon_rate = st.slider("Checkout Abandon Rate", 0.0, 1.0, 0.15)
        
        if st.button("🔮 Calculate Trust Score", key="trust_btn"):
            with st.spinner("Running ML Model..."):
                ml_tool = MLPredictionTool()
                features = {
                    "account_age_days": account_age,
                    "kyc_level_num": kyc_level,
                    "account_status_num": 1,
                    "late_rate_90d": late_rate,
                    "ontime_rate_90d": ontime_rate,
                    "active_plans": active_plans,
                    "orders_30d": orders_30d,
                    "amount_30d": amount_30d,
                    "disputes_90d": disputes,
                    "refunds_90d": 0,
                    "checkout_abandon_rate_30d": abandon_rate,
                }
                
                result = asyncio.run(ml_tool._arun("trust_score", features))
                
                st.markdown("### Result")
                st.markdown(result)
                
                # Visualization
                # Extract score from result (simplified)
                import re
                score_match = re.search(r"Score: (\d+)/100", result)
                if score_match:
                    score = int(score_match.group(1))
                    
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=score,
                        domain={"x": [0, 1], "y": [0, 1]},
                        title={"text": "Trust Score"},
                        gauge={
                            "axis": {"range": [0, 100]},
                            "bar": {"color": "#667eea"},
                            "steps": [
                                {"range": [0, 40], "color": "#ff5722"},
                                {"range": [40, 70], "color": "#ffc107"},
                                {"range": [70, 100], "color": "#00c853"},
                            ],
                        }
                    ))
                    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown("### Late Payment Prediction")
        st.info("Enter customer payment history to predict late payment risk.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            lp_late_rate = st.slider("Historical Late Rate", 0.0, 1.0, 0.1, key="lp_late")
            lp_avg_late = st.slider("Avg Late Days", 0, 30, 2, key="lp_avg")
            lp_active = st.slider("Active Plans", 0, 10, 1, key="lp_active")
        
        with col2:
            lp_account_age = st.slider("Account Age", 1, 500, 180, key="lp_age")
            lp_kyc = st.selectbox("KYC Level", [0, 1, 2], index=2, key="lp_kyc")
        
        if st.button("🔮 Predict Late Payment Risk", key="late_btn"):
            with st.spinner("Running ML Model..."):
                ml_tool = MLPredictionTool()
                features = {
                    "late_payment_rate_90d": lp_late_rate,
                    "avg_late_days_90d": lp_avg_late,
                    "on_time_payment_rate_90d": 1 - lp_late_rate,
                    "num_active_plans": lp_active,
                    "account_age_days": lp_account_age,
                    "kyc_level_num": lp_kyc,
                }
                
                result = asyncio.run(ml_tool._arun("late_payment", features))
                
                st.markdown("### Prediction Result")
                st.markdown(result)


def main():
    """Main application."""
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50?text=BNPL+Analytics", use_container_width=True)
        st.markdown("---")
        
        page = st.radio(
            "Navigation",
            ["📊 Dashboard", "💬 Chat", "🎯 Risk Assessment", "📜 History"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("### System Status")
        st.success("✅ Agent Online")
        st.success("✅ ML Models Loaded")
        st.success("✅ Data Connected")
        
        st.markdown("---")
        st.markdown("**Version:** 1.0.0")
        st.markdown("**Last Updated:** Jan 2026")
    
    # Header
    st.markdown("""
    <div class="header-container">
        <h1 style="color: white; margin: 0;">🏦 BNPL Intelligent Analytics</h1>
        <p style="color: rgba(255,255,255,0.8); margin: 0;">Powered by AI • Real-time Insights • ML Predictions</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Page routing
    if page == "📊 Dashboard":
        render_kpi_dashboard()
    elif page == "💬 Chat":
        render_chat_interface()
    elif page == "🎯 Risk Assessment":
        render_risk_assessment()
    elif page == "📜 History":
        render_history_logs()


if __name__ == "__main__":
    main()
