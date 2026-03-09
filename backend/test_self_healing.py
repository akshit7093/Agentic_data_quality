
import asyncio
import logging
from typing import Dict, Any, List
from app.agents.column_analysis_agent import ColumnAnalysisAgent
from app.validation.engine import ValidationEngine

# Set up logging to see the healing attempts
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def mock_engine_executor(query: str, q_type: str) -> Dict[str, Any]:
    """Mock executor that uses the real ValidationEngine."""
    engine = ValidationEngine()
    # Simple mock data source info
    class DataSource:
        target_path = "test_table"
    
    sample_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    
    return await engine.execute_agent_query(
        query, q_type, DataSource(), sample_data
    )

async def test_column_agent_healing():
    logger.info("Starting test_column_agent_healing...")
    
    # Create an agent for the 'age' column
    agent = ColumnAnalysisAgent(
        column_name="age",
        dtype="INTEGER",
        samples=[30, 25, 35],
        mode="ai_recommended",
        engine_executor=mock_engine_executor
    )
    
    # Manually inject a BROKEN rule (syntax error: missing quote or invalid keyword)
    # We'll use a query that will definitely fail in SQLite
    agent.rules_to_run = [
        {
            "rule_name": "age_broken_rule",
            "severity": "critical",
            "query": "SELECT COUNT(*) AS cnt FROM test_table WHERE age IS INVALID_KEYWORD"
        }
    ]
    
    logger.info("Executing rules (expecting healing attempt)...")
    await agent._execute_rules(table_name="test_table")
    
    # Check results
    result = agent.execution_results[0]
    logger.info(f"Final Result Status: {result['result']['status']}")
    logger.info(f"Final Query Used: {result['query']}")
    
    if result['result']['status'] == "success":
        print("\n✅ SUCCESS: Agent successfully healed the broken query!")
    else:
        print(f"\n❌ FAILURE: Agent failed to heal. Error: {result['result'].get('error')}")

if __name__ == "__main__":
    asyncio.run(test_column_agent_healing())
