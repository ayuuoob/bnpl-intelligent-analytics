# BNPL Intelligent Analytics - Web Dashboard

A professional Streamlit dashboard for BNPL analytics with AI-powered insights.

## Features

- **📊 KPI Dashboard**: Real-time GMV, Approval Rate, Late Payment Rate, Active Users
- **💬 Agent Chat**: Natural language interface to query your data
- **🎯 Risk Assessment**: ML-powered Trust Score and Late Payment prediction
- **📈 Dynamic Charts**: Auto-generated visualizations based on queries
- **📜 History & Logs**: Track all queries and agent interactions

## Quick Start

```bash
# Navigate to webapp
cd webapp

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

The app will open at http://localhost:8501

## Pages

### Dashboard
View key performance indicators with real-time data:
- GMV (Gross Merchandise Value)
- Approval Rate
- Late Payment Rate
- Active Users
- Order volume charts
- Top merchants

### Chat
Ask questions in natural language:
- "What was our GMV last month?"
- "Show me order trends over time" (generates chart)
- "What is the risk score for a customer?"
- "Compare top merchants by revenue"

### Risk Assessment
Interactive ML prediction tools:
- **Trust Score (UC2)**: Adjust customer features and get instant predictions
- **Late Payment (UC1)**: Predict payment behavior

### History
View query history and session logs.

## Environment

Create a `.env` file in the `agents/` folder:

```
GOOGLE_API_KEY=your_gemini_api_key
```
