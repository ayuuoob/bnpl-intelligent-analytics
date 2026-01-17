"""
SQL Tool - MCP wrapper for executing read-only SQL queries.

Provides safe SQL execution with mandatory guardrails:
- Read-only (SELECT only)
- Time-bounded queries  
- Row limits
- Table allowlist enforcement
"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional, List, Set
import os
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool

from .mcp_client import get_mcp_client, MCPResponse
from .schema_tool import SchemaTool


class SQLGuardrails(BaseModel):
    """Configuration for SQL execution guardrails."""
    
    max_rows: int = Field(default=1000, description="Maximum rows to return")
    default_time_window_days: int = Field(default=30, description="Default time filter")
    require_time_filter: bool = Field(default=True, description="Require date filter")
    
    # Blocked patterns (case-insensitive)
    blocked_patterns: List[str] = Field(
        default=[
            r"\bINSERT\b",
            r"\bUPDATE\b",
            r"\bDELETE\b",
            r"\bDROP\b",
            r"\bCREATE\b",
            r"\bALTER\b",
            r"\bTRUNCATE\b",
            r"\bGRANT\b",
            r"\bREVOKE\b",
            r"\bEXEC\b",
            r"\bEXECUTE\b",
            r"--",  # SQL comments (potential injection)
            r"/\*",  # Block comments
        ]
    )


class SQLValidationResult(BaseModel):
    """Result of SQL validation."""
    
    is_valid: bool
    query: str  # Potentially modified query
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class SQLTool(BaseTool):
    """
    LangChain tool for executing read-only SQL queries via MCP.
    
    IMPORTANT: This tool enforces strict guardrails:
    1. SELECT queries only (no INSERT/UPDATE/DELETE/DDL)
    2. Must include time filter (or default applied)
    3. Row limit enforced (default 1000)
    4. Only allowed tables can be queried
    
    Usage:
        tool = SQLTool()
        result = tool.invoke({
            "query": "SELECT merchant_id, SUM(amount) as gmv FROM orders WHERE created_at >= '2025-12-01' GROUP BY merchant_id",
        })
    """
    
    name: str = "sql_run"
    description: str = """Execute a read-only SQL query against the BNPL data warehouse.
    
    GUARDRAILS (automatically enforced):
    - SELECT queries ONLY (no INSERT/UPDATE/DELETE/DDL)
    - Time filter REQUIRED (default: last 30 days)
    - Row limit: 1000 (configurable)
    - Only allowed tables can be queried
    
    Allowed Tables:
    GOLD (preferred): kpi_daily, user_features_daily, merchant_features_daily, cohorts_signup_week
    SILVER (drill-down): orders, installments, payments, users, merchants, disputes_returns
    
    Parameters:
    - query (required): SQL SELECT query
    - limit (optional): Override row limit (max 5000)
    
    IMPORTANT: Before running SQL, use schema_get tool to discover tables and columns.
    
    Returns: Query results as formatted table or error message.
    """
    
    guardrails: SQLGuardrails = Field(default_factory=SQLGuardrails)
    _schema_tool: Optional[SchemaTool] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._schema_tool = SchemaTool()
        
        # Load config from env
        max_rows = os.getenv("MAX_SQL_ROWS")
        if max_rows:
            self.guardrails.max_rows = int(max_rows)
        
        time_window = os.getenv("DEFAULT_TIME_WINDOW_DAYS")
        if time_window:
            self.guardrails.default_time_window_days = int(time_window)
    
    def _run(self, query: str, limit: Optional[int] = None) -> str:
        """Synchronous SQL execution."""
        return asyncio.run(self._arun(query, limit))
    
    async def _arun(self, query: str, limit: Optional[int] = None) -> str:
        """
        Execute SQL query with guardrails.
        
        1. Validate query structure
        2. Check for blocked patterns
        3. Verify table allowlist
        4. Apply row limit
        5. Execute via local data or MCP
        """
        # Validate query
        validation = self._validate_query(query, limit)
        
        if not validation.is_valid:
            return "SQL Validation Failed:\n" + "\n".join(f"- {e}" for e in validation.errors)
        
        # Add warnings to output
        warnings_text = ""
        if validation.warnings:
            warnings_text = "Warnings:\n" + "\n".join(f"- {w}" for w in validation.warnings) + "\n\n"
        
        # Check if using local data mode
        use_local = os.getenv("USE_LOCAL_DATA", "false").lower() == "true"
        
        if use_local:
            # Execute against local CSV data
            result = self._execute_local_sql(validation.query)
            return warnings_text + result
        
        # Execute via MCP
        client = get_mcp_client()
        response: MCPResponse = await client.call("sql.run", {"query": validation.query})
        
        if response.success and response.data:
            return warnings_text + self._format_results(response.data)
        
        if response.error:
            return warnings_text + f"SQL Execution Error: {response.error}"
        
        # Fallback to mock execution for demo
        mock_result = self._mock_execute(validation.query)
        return warnings_text + mock_result
    
    def _execute_local_sql(self, query: str) -> str:
        """Execute SQL-like query against local CSV data using pandas."""
        try:
            from .local_data import get_local_data
            import pandas as pd
            
            # Get all local data
            data = get_local_data()
            
            # Parse query to find what's needed
            query_lower = query.lower()
            
            # Determine which data to use based on query
            if "kpi_daily" in query_lower or "kpi" in query_lower:
                # Aggregate KPI metrics
                return self._execute_kpi_query(data, query)
            elif "order" in query_lower:
                df = data.get("orders", pd.DataFrame())
                return self._execute_pandas_query(df, query, "orders")
            elif "user" in query_lower:
                df = data.get("users", pd.DataFrame())
                return self._execute_pandas_query(df, query, "users")
            elif "merchant" in query_lower:
                df = data.get("merchants", pd.DataFrame())
                return self._execute_pandas_query(df, query, "merchants")
            elif "installment" in query_lower or "payment" in query_lower:
                df = data.get("installments", pd.DataFrame())
                return self._execute_pandas_query(df, query, "installments")
            elif "dispute" in query_lower or "return" in query_lower:
                df = data.get("disputes_returns", pd.DataFrame())
                return self._execute_pandas_query(df, query, "disputes_returns")
            else:
                # Default to orders
                df = data.get("orders", pd.DataFrame())
                return self._execute_pandas_query(df, query, "orders")
                
        except Exception as e:
            return f"Local SQL Error: {str(e)}"
    
    def _execute_kpi_query(self, data: dict, query: str) -> str:
        """Execute KPI-style query from local data."""
        import pandas as pd
        
        orders_df = data.get("orders", pd.DataFrame())
        users_df = data.get("users", pd.DataFrame())
        installments_df = data.get("installments", pd.DataFrame())
        
        if orders_df.empty:
            return "No order data available."
        
        # Basic KPI aggregation
        results = []
        
        # Calculate daily metrics
        if "date" in orders_df.columns or "created_at" in orders_df.columns:
            date_col = "date" if "date" in orders_df.columns else "created_at"
            orders_df[date_col] = pd.to_datetime(orders_df[date_col])
            
            daily = orders_df.groupby(orders_df[date_col].dt.date).agg({
                "amount": "sum",
                "order_id": "count" if "order_id" in orders_df.columns else "size"
            }).reset_index()
            daily.columns = ["date", "gmv", "orders"]
            
            # Get last 10 days
            daily = daily.tail(10)
            
            result_data = {
                "columns": ["date", "gmv", "orders"],
                "rows": daily.to_dict("records")
            }
            return "_Using local CSV data_\n\n" + self._format_results(result_data)
        
        return "Date column not found in orders data."
    
    def _execute_pandas_query(self, df: pd.DataFrame, query: str, table_name: str) -> str:
        """Execute a simple query against a pandas DataFrame."""
        import pandas as pd
        
        if df.empty:
            return f"No {table_name} data available."
        
        # Apply limit from query
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        limit = int(limit_match.group(1)) if limit_match else 50
        
        # Get sample of data
        result_df = df.head(limit)
        
        result_data = {
            "columns": list(result_df.columns),
            "rows": result_df.to_dict("records")
        }
        
        return f"_Using local CSV data ({len(df)} total rows)_\n\n" + self._format_results(result_data)
    
    def _validate_query(self, query: str, limit: Optional[int] = None) -> SQLValidationResult:
        """Validate SQL query against guardrails."""
        errors = []
        warnings = []
        modified_query = query.strip()
        
        # 1. Check it's a SELECT query
        if not modified_query.upper().startswith("SELECT"):
            errors.append("Only SELECT queries are allowed. Query must start with SELECT.")
            return SQLValidationResult(is_valid=False, query=query, errors=errors)
        
        # 2. Check for blocked patterns
        for pattern in self.guardrails.blocked_patterns:
            if re.search(pattern, modified_query, re.IGNORECASE):
                errors.append(f"Blocked SQL pattern detected: {pattern}")
        
        if errors:
            return SQLValidationResult(is_valid=False, query=query, errors=errors)
        
        # 3. Validate tables against allowlist
        table_errors = self._validate_tables(modified_query)
        errors.extend(table_errors)
        
        if errors:
            return SQLValidationResult(is_valid=False, query=query, errors=errors)
        
        # 4. Check/add time filter
        if self.guardrails.require_time_filter:
            has_time_filter = self._has_time_filter(modified_query)
            if not has_time_filter:
                # Add default time filter
                default_start = (datetime.now() - timedelta(days=self.guardrails.default_time_window_days)).strftime("%Y-%m-%d")
                warnings.append(f"No time filter detected. Adding default: last {self.guardrails.default_time_window_days} days (since {default_start})")
                modified_query = self._add_time_filter(modified_query, default_start)
        
        # 5. Add/modify LIMIT
        effective_limit = min(limit or self.guardrails.max_rows, 5000)
        modified_query = self._ensure_limit(modified_query, effective_limit)
        
        return SQLValidationResult(
            is_valid=True,
            query=modified_query,
            errors=errors,
            warnings=warnings,
        )
    
    def _validate_tables(self, query: str) -> List[str]:
        """Extract and validate tables from query."""
        errors = []
        allowed_tables = set(self._schema_tool.get_allowed_tables())
        
        # Simple table extraction (FROM and JOIN clauses)
        # This is a simplified approach - production would use SQL parser
        from_pattern = r'\bFROM\s+(\w+)'
        join_pattern = r'\bJOIN\s+(\w+)'
        
        tables_in_query: Set[str] = set()
        
        for match in re.finditer(from_pattern, query, re.IGNORECASE):
            tables_in_query.add(match.group(1).lower())
        
        for match in re.finditer(join_pattern, query, re.IGNORECASE):
            tables_in_query.add(match.group(1).lower())
        
        # Check against allowlist
        for table in tables_in_query:
            if table not in allowed_tables:
                errors.append(f"Table '{table}' is not in the allowlist. Allowed: {', '.join(sorted(allowed_tables))}")
        
        return errors
    
    def _has_time_filter(self, query: str) -> bool:
        """Check if query has a time/date filter."""
        time_patterns = [
            r"WHERE.*\b(date|created_at|paid_at|due_date|signup_date)\b.*[>=<]",
            r"WHERE.*\b(date|created_at)\b.*BETWEEN",
            r"WHERE.*\b\d{4}-\d{2}-\d{2}\b",  # ISO date format
        ]
        
        for pattern in time_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return True
        return False
    
    def _add_time_filter(self, query: str, start_date: str) -> str:
        """Add time filter to query."""
        # Find WHERE clause or add one
        if re.search(r'\bWHERE\b', query, re.IGNORECASE):
            # Add to existing WHERE
            query = re.sub(
                r'(\bWHERE\b)',
                f"WHERE created_at >= '{start_date}' AND",
                query,
                count=1,
                flags=re.IGNORECASE,
            )
        else:
            # Find position to insert WHERE (before GROUP BY, ORDER BY, or LIMIT)
            insert_match = re.search(r'\b(GROUP BY|ORDER BY|LIMIT)\b', query, re.IGNORECASE)
            if insert_match:
                insert_pos = insert_match.start()
                query = query[:insert_pos] + f" WHERE created_at >= '{start_date}' " + query[insert_pos:]
            else:
                # Append at end
                query = query + f" WHERE created_at >= '{start_date}'"
        
        return query
    
    def _ensure_limit(self, query: str, limit: int) -> str:
        """Ensure query has a LIMIT clause."""
        # Check for existing LIMIT
        limit_match = re.search(r'\bLIMIT\s+(\d+)', query, re.IGNORECASE)
        
        if limit_match:
            existing_limit = int(limit_match.group(1))
            if existing_limit > limit:
                # Replace with lower limit
                query = re.sub(
                    r'\bLIMIT\s+\d+',
                    f"LIMIT {limit}",
                    query,
                    flags=re.IGNORECASE,
                )
        else:
            # Add LIMIT
            query = query.rstrip(";") + f" LIMIT {limit}"
        
        return query
    
    def _format_results(self, data: dict) -> str:
        """Format SQL results as readable table."""
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        
        if not rows:
            return "Query returned 0 rows."
        
        lines = [f"Query returned {len(rows)} rows.\n"]
        
        # Build markdown table
        if columns:
            lines.append("| " + " | ".join(str(c) for c in columns) + " |")
            lines.append("| " + " | ".join("---" for _ in columns) + " |")
        
        for row in rows[:50]:  # Limit display to 50 rows
            if isinstance(row, dict):
                values = [str(row.get(c, "")) for c in columns]
            else:
                values = [str(v) for v in row]
            lines.append("| " + " | ".join(values) + " |")
        
        if len(rows) > 50:
            lines.append(f"\n... and {len(rows) - 50} more rows (truncated for display)")
        
        return "\n".join(lines)
    
    def _mock_execute(self, query: str) -> str:
        """Generate mock results for demo/testing."""
        import random
        
        # Detect query type and generate appropriate mock data
        if "merchant" in query.lower():
            mock_data = {
                "columns": ["merchant_id", "merchant_name", "value"],
                "rows": [
                    {"merchant_id": f"M{i:03d}", "merchant_name": f"Merchant {i}", "value": random.uniform(10000, 500000)}
                    for i in range(1, 11)
                ]
            }
        elif "user" in query.lower():
            mock_data = {
                "columns": ["user_id", "metric", "value"],
                "rows": [
                    {"user_id": f"U{i:05d}", "metric": "orders", "value": random.randint(1, 20)}
                    for i in range(1, 11)
                ]
            }
        else:
            mock_data = {
                "columns": ["date", "metric", "value"],
                "rows": [
                    {"date": f"2025-12-{i:02d}", "metric": "daily_value", "value": random.uniform(50000, 150000)}
                    for i in range(1, 11)
                ]
            }
        
        return "_Note: Using mock data (MCP server not connected)_\n\n" + self._format_results(mock_data)
