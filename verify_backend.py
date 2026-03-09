
import asyncio
import json
import uuid
import logging
from app.agents.state import DataSourceInfo, AgentState, AgentStatus, ValidationMode
from app.agents.data_quality_agent import DataQualityAgent
from app.api.routes import _validation_store, run_validation, ValidationRequest, BUILTIN_SOURCES

logging.basicConfig(level=logging.INFO)

async def test_backend_fixes():
    # 1. Test connector NameError fix
    print("Testing connector NameError fix...")
    agent = DataQualityAgent()
    state = {
        "messages": [{"role": "assistant", "content": '{"action": "execute_query", "query": "SELECT 1"}'}],
        "data_source_info": DataSourceInfo(
            source_type="sqlite",
            connection_config=BUILTIN_SOURCES["local-test"]["connection_config"],
            target_path="customers"
        ),
        "exploration_steps": 0
    }
    # This should not raise NameError: name 'connector' is not defined
    try:
        result = await agent._execute_tool(state)
        print("Connector NameError fix verified (or at least initialized).")
    except NameError as e:
        print(f"FAILED: NameError still occurs: {e}")
    except Exception as e:
        print(f"Caught expected/other exception during tool execution: {e}")

    # 2. Test analysis scope optimization
    print("\nTesting analysis scope optimization...")
    # Mock an applied session
    session_id = str(uuid.uuid4())
    selected_columns = ["customer_id", "email"]
    
    from app.agents.template_routes import set_applied_session
    set_applied_session(session_id, {
        "columns": selected_columns,
        "source_id": "local-test",
        "resource_path": "customers"
    })
    
    request = ValidationRequest(
        data_source_id="local-test",
        target_path="customers",
        session_id=session_id
    )
    
    validation_id = "test-val-id"
    _validation_store[validation_id] = {"status": "pending"}
    
    await run_validation(validation_id, request)
    
    result = _validation_store[validation_id].get("result")
    if result:
        data_source_info = result.get("data_source_info")
        if data_source_info and data_source_info.selected_columns == selected_columns:
            print(f"SUCCESS: selected_columns correctly populated: {data_source_info.selected_columns}")
            
            # Check if schema was filtered
            schema = data_source_info.schema
            if schema and list(schema.get("columns", {}).keys()) == selected_columns:
                print("SUCCESS: Schema filtered to selected columns.")
            else:
                print(f"FAILED: Schema not filtered correctly. Found: {list(schema.get('columns', {}).keys())}")
        else:
            print(f"FAILED: selected_columns not populated correctly. Found: {getattr(data_source_info, 'selected_columns', 'None')}")
    else:
        print(f"FAILED: No result found in validation store for {validation_id}")

if __name__ == "__main__":
    asyncio.run(test_backend_fixes())
