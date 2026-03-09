
import asyncio
import sys
import os

# Mocking parts of the app to test tool_based_agent
sys.path.append('d:/projects/Agentic_qc/ai-data-quality-agent/backend')

from app.agents.tool_based_agent import ValidationToolExecutor, UNIVERSAL_TOOLS, TABLE_TOOLS

class MockConnector:
    async def get_schema(self, table):
        return {"columns": {"col 1": {"type": "string"}, "col 2": {"type": "integer"}}}
    async def execute_raw_query(self, query):
        print(f"DEBUG: Executing Query -> {query}")
        return {"status": "success", "row_count": 0, "sample_rows": []}

async def test_quoting():
    # Test column name with space
    connector = MockConnector()
    executor = ValidationToolExecutor(connector, "my-data-file.csv")
    
    print("--- Testing table_null_scan with space in column ---")
    # table_null_scan uses {table} and {column}
    result = await executor.execute_tool("table_null_scan", column="Reporting starts")
    
    # Check if identifiers are quoted in the executed command
    cmd = result.command_executed
    print(f"Generated Command: {cmd}")
    
    assert '"my-data-file.csv"' in cmd
    assert '"Reporting starts"' in cmd
    print("SUCCESS: Identifiers are correctly quoted.")

    print("\n--- Testing table_column_scan with {all_columns} ---")
    # table_column_scan uses {all_columns}
    result = await executor.execute_tool("table_column_scan")
    cmd = result.command_executed
    print(f"Generated Command: {cmd}")
    assert '"col 1"' in cmd
    assert '"col 2"' in cmd
    print("SUCCESS: All columns are correctly quoted.")

if __name__ == "__main__":
    asyncio.run(test_quoting())
