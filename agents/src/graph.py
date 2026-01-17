"""
BNPL Analytics Agent - LangGraph State Machine

This is the main entry point for the agent. It assembles the
graph nodes and provides the interface for processing queries.

Graph Flow:
User Query → Router → Planner → Executor → Validator → Narrator → Response
                                    ↑           │
                                    └───────────┘ (retry if needed)
"""

import os
from typing import Optional, Literal, Union
from langgraph.graph import StateGraph, END

from .state import AgentState
from .nodes import (
    RouterNode,
    PlannerNode,
    ExecutorNode,
    ValidatorNode,
    NarratorNode,
)

# Backup API keys for rotation when rate limited
BACKUP_API_KEYS = [
    "AIzaSyD1TgM4t5OzhEjQX8JOwq7rXfQJYTFcHRQ",
    "AIzaSyBrK8Oby0jULyMdf6rPyf4v3UuhQH_6kek",
    "AIzaSyCFSlT8bDXXrs4a4niOOeaFLyRFIv1Rh4Y",
]

# Track current API key index
_current_key_index = 0
_llm_instance = None


def get_next_api_key() -> str:
    """Get the next API key in rotation."""
    global _current_key_index
    
    # First try environment variable
    env_key = os.getenv("GOOGLE_API_KEY")
    if env_key and _current_key_index == 0:
        return env_key
    
    # Use backup keys
    backup_index = _current_key_index - 1 if env_key else _current_key_index
    if 0 <= backup_index < len(BACKUP_API_KEYS):
        return BACKUP_API_KEYS[backup_index]
    
    # Cycle back to first backup key
    _current_key_index = 1 if env_key else 0
    return BACKUP_API_KEYS[0] if BACKUP_API_KEYS else None


def rotate_api_key():
    """Rotate to the next API key (call when rate limited)."""
    global _current_key_index, _llm_instance
    
    env_key = os.getenv("GOOGLE_API_KEY")
    max_keys = len(BACKUP_API_KEYS) + (1 if env_key else 0)
    
    _current_key_index = (_current_key_index + 1) % max_keys
    _llm_instance = None  # Force re-creation of LLM
    
    print(f"Rotated to API key {_current_key_index + 1}/{max_keys}")


def get_llm() -> Optional[Union["ChatGoogleGenerativeAI", "ChatOpenAI"]]:
    """
    Initialize LLM with rotating API keys.
    Supports multiple Gemini API keys with automatic rotation on rate limit.
    Returns None if no API key is configured (agent still works with rule-based logic).
    """
    global _llm_instance
    
    if _llm_instance is not None:
        return _llm_instance
    
    # Try Gemini with rotating keys
    api_key = get_next_api_key()
    if api_key:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            _llm_instance = ChatGoogleGenerativeAI(
                model="gemini-1.5-flash",  # Fast and free tier friendly
                google_api_key=api_key,
                temperature=0,
            )
            return _llm_instance
        except ImportError:
            print("Warning: langchain-google-genai not installed. Run: pip install langchain-google-genai")
    
    # Fallback to OpenAI
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        try:
            from langchain_openai import ChatOpenAI
            _llm_instance = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0,
                api_key=openai_key,
            )
            return _llm_instance
        except ImportError:
            print("Warning: langchain-openai not installed. Run: pip install langchain-openai")
    
    # No LLM configured - agent will use rule-based logic only
    print("Note: No LLM API key configured. Agent will use rule-based classification only.")
    return None


def create_agent_graph():
    """
    Create the BNPL Analytics Agent graph.
    
    Returns a compiled LangGraph that can process queries.
    """
    # Initialize LLM (optional - agent works without it)
    llm = get_llm()
    
    # Initialize nodes
    router = RouterNode(llm=llm)
    planner = PlannerNode(llm=llm)
    executor = ExecutorNode(llm=llm)
    validator = ValidatorNode(llm=llm)
    narrator = NarratorNode(llm=llm)
    
    # Build the graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("router", router)
    workflow.add_node("planner", planner)
    workflow.add_node("executor", executor)
    workflow.add_node("validator", validator)
    workflow.add_node("narrator", narrator)
    
    # Define edges
    workflow.set_entry_point("router")
    workflow.add_edge("router", "planner")
    workflow.add_edge("planner", "executor")
    workflow.add_edge("executor", "validator")
    
    # Conditional edge from validator
    def should_retry(state: AgentState) -> Literal["executor", "narrator"]:
        """Determine if we should retry or proceed to narrator."""
        if state.validation and state.validation.retry_needed:
            if state.retry_count < state.max_retries:
                return "executor"
        return "narrator"
    
    workflow.add_conditional_edges(
        "validator",
        should_retry,
        {
            "executor": "executor",
            "narrator": "narrator",
        }
    )
    
    workflow.add_edge("narrator", END)
    
    # Compile and return
    return workflow.compile()


# Create a singleton instance
_agent_graph = None


def get_agent():
    """Get the singleton agent graph instance."""
    global _agent_graph
    if _agent_graph is None:
        _agent_graph = create_agent_graph()
    return _agent_graph


async def run_query(query: str, session_id: Optional[str] = None) -> str:
    """
    Run a query through the agent with automatic API key rotation on rate limit.
    
    Args:
        query: Natural language question
        session_id: Optional session identifier for tracing
        
    Returns:
        Structured response string
    """
    global _agent_graph
    max_retries = len(BACKUP_API_KEYS) + 1  # Try all available keys
    
    for attempt in range(max_retries):
        try:
            agent = get_agent()
            
            # Create initial state
            initial_state = AgentState(
                user_query=query,
                session_id=session_id,
            )
            
            # Run the graph
            final_state = await agent.ainvoke(initial_state)
            
            # Return the response
            if isinstance(final_state, dict):
                return final_state.get("final_response", "No response generated")
            return final_state.final_response or "No response generated"
            
        except Exception as e:
            error_msg = str(e).lower()
            # Check if rate limited or quota exceeded
            if any(term in error_msg for term in ["rate limit", "quota", "429", "resource exhausted"]):
                print(f"API rate limited (attempt {attempt + 1}/{max_retries}). Rotating key...")
                rotate_api_key()
                _agent_graph = None  # Force recreation of agent with new key
            else:
                # Not a rate limit error, re-raise
                raise e
    
    return "Error: All API keys exhausted. Please try again later."


def run_query_sync(query: str, session_id: Optional[str] = None) -> str:
    """Synchronous version of run_query."""
    import asyncio
    return asyncio.run(run_query(query, session_id))


# Demo function
async def demo():
    """Run demo queries."""
    demo_queries = [
        "What was our GMV last month?",
        "Which merchants have the highest dispute rates?",
        "What is our late payment rate by cohort?",
        "How many active users do we have?",
        "What is the checkout conversion rate?",
    ]
    
    print("=" * 60)
    print("BNPL Analytics Agent - Demo")
    print("=" * 60)
    
    for query in demo_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print("=" * 60)
        
        response = await run_query(query)
        print(response)
        print()


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo())
