
import asyncio
import os
import pandas as pd
from pathlib import Path
from app.connectors.local_file import LocalFileConnector
from app.agents.state import DataSourceType

async def test_schema():
    uploads_dir = Path(r"d:\projects\Agentic_qc\ai-data-quality-agent\uploads")
    csv_file = "BlueTea---Prepaid-Ad-sets-1-Jan-2026-5-Jan-2026.csv"
    
    connector = LocalFileConnector({
        "base_path": str(uploads_dir)
    }, source_type=DataSourceType.LOCAL_FILE)
    
    print(f"Testing schema for {csv_file}")
    try:
        schema = await connector.get_schema(csv_file)
        print("Schema retrieved:")
        import json
        print(json.dumps(schema, indent=2))
        
        # Test direct read
        df = await connector._read_multiple_files([str(uploads_dir / csv_file)], limit=10)
        print(f"\nSample data (first 5 rows, first 5 cols):")
        print(df.iloc[:5, :5])
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import os
    import sys
    # Add backend to path
    sys.path.append(os.path.join(os.getcwd(), "backend"))
    asyncio.run(test_schema())
