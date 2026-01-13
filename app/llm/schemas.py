from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Union


# 1. Define the filters strictly to prevent SQL Injection
class FilterCondition(BaseModel):
    column: str = Field(..., description="The database column name, e.g., 'trading_volume'")
    operator: Literal['=', '>', '<', '>=', '<=', 'LIKE', 'IN'] = Field(..., description="SQL operator")
    value: Union[str, int, float, List[str]] = Field(..., description="The value to filter by")

# 2. The Core "Query Plan" - The AI fills this out
class DataQueryPlan(BaseModel):
    intent: Literal['query_data', 'general_chat', 'explanation'] = Field(
        ..., description="Did the user ask for data or just saying hello?"
    )
    
    # Metadata for the UI to show "I am searching for..."
    reasoning: str = Field(..., description="Short explanation of what the AI understands")
    
    # SQL Construction Blocks
    target_table: Optional[str] = Field(None, description="The specific ADS table name to query")
    metrics: List[str] = Field(default_factory=list, description="Columns to select/aggregate, e.g., 'SUM(volume)'")
    dimensions: List[str] = Field(default_factory=list, description="Columns to GROUP BY, e.g., 'trading_pair'")
    filters: List[FilterCondition] = Field(default_factory=list, description="WHERE clause conditions")
    time_range_start: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    time_range_end: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")
    limit: int = Field(default=100, description="Max rows to return")
    
    # Visualization Recommendation
    suggested_chart: Literal['table', 'line', 'bar', 'pie', 'kpi_card'] = Field(
        'table', description="Best way to visualize this data"
    )

class SQLQueryPlan(BaseModel):
    """
    The strict structure we force the AI to return.
    This prevents it from writing 'paragraphs' of text when we need code.
    """
    thought_process: str = Field(
        ..., 
        description="Explain briefly why you chose this SQL. e.g. 'Querying daily volume table filtering by BTC-USDT'"
    )
    sql_query: str = Field(
        ..., 
        description="The valid PostgreSQL query. Must start with SELECT."
    )
    visualization_type: Literal['table', 'line_chart', 'bar_chart', 'number_card'] = Field(
        ..., 
        description="The best way to show this result to the user."
    )
    is_safe: bool = Field(
        True,
        description="Set to False if the user asks for something outside the provided schema."
    )