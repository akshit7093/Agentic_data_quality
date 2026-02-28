import asyncio
import json
from datetime import datetime

from app.core.config import get_settings
from app.agents.state import DataSourceInfo, ValidationMode
from app.agents.data_quality_agent import get_data_quality_agent

async def main():
    agent = get_data_quality_agent()
    
    import os
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "test_data", "test_database.db"))
    ds_info = DataSourceInfo(
        source_type="sqlite",
        connection_config={"connection_string": "sqlite:///" + db_path.replace("\\", "/")},
        target_path="sales_transactions",
        full_scan_requested=True
    )
    
    # We will invoke the graph up to profile_data
    print("Running _connect_data_source directly...")
    state = {
        "validation_id": "test_script_run",
        "validation_mode": ValidationMode.HYBRID,
        "data_source_info": ds_info,
        "custom_rules": [],
        "execution_config": {"full_scan": True, "sample_size": 1000},
        "messages": [],
    }
    
    result = await agent._connect_data_source(state)
    print("Connect Result Status:", result.get("status"))
    print("Connect Messages:", result.get("messages"))
    
    # Update the state based on the connect result
    # (simulate the graph passing it to the next step)
    state.update(result)
    
    print("Running _profile_data directly...")
    result2 = await agent._profile_data(state)
    print("Profile Result Status:", result2.get("status"))
    if result2.get("error_message"):
        print("Profile Error Message:", result2.get("error_message"))

if __name__ == "__main__":
    asyncio.run(main())
