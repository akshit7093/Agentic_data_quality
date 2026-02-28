import asyncio
import httpx
import time
import json

async def test_full_scan():
    print("Fetching data sources...")
    async with httpx.AsyncClient(timeout=120.0) as client:
        res = await client.get("http://localhost:8000/api/v1/datasources")
        sources = res.json()
        
        # Find SQLite test_db
        source_id = None
        for s in sources:
            if s.get("source_type") == "sqlite" or s.get("name") == "Test Database":
                source_id = s.get("id")
                break
                
        if not source_id:
            print("Test Database not found. Printing available sources:")
            print(json.dumps(sources, indent=2))
            return
            
        print(f"Found test database with ID: {source_id}")
        
        print("\nSubmitting full-scan validation for 'sales_transactions' table...")
        payload = {
            "data_source_id": source_id,
            "target_path": "sales_transactions",
            "validation_mode": "hybrid",
            "sample_size": 1000,
            "full_scan": True
        }
        res = await client.post("http://localhost:8000/api/v1/validate", json=payload)
        
        if res.status_code != 200:
            print("Failed to start validation:", res.text)
            return
            
        validation_id = res.json().get("validation_id")
        print(f"Started validation: {validation_id}")
        
        print("\nPolling status...")
        while True:
            await asyncio.sleep(2)
            res = await client.get(f"http://localhost:8000/api/v1/validate/{validation_id}")
            status_data = res.json()
            status = status_data.get("status")
            print(f"Status: {status}")
            
            if status in ["completed", "failed"]:
                print(f"\nFinal Score: {status_data.get('quality_score')}%")
                print(f"Passed/Failed Rules: {status_data.get('passed_rules')} / {status_data.get('failed_rules')}")
                
                # Try to get detailed results
                res = await client.get(f"http://localhost:8000/api/v1/validate/{validation_id}/results")
                results_data = res.json()
                
                print("\n=== EXECUTION LOGS ===")
                # The execution logs are tucked under state['messages'] which may be exposed in the results under 'result'
                raw_result = status_data.get("result", {})
                if not raw_result:
                    raw_result = results_data.get("raw_state", {})
                print("RAW STATE MESSAGES:", json.dumps(raw_result, indent=2))
                
                messages = raw_result.get("messages", [])
                
                for msg in messages:
                    if msg.get("type") == "ai" and msg.get("name") == "DataQualityAgent":
                        content = msg.get("content")
                        if isinstance(content, str) and ("[Engine]" in content or "[SQL Pushdown]" in content):
                            print(f"> {content}")
                        elif isinstance(content, list):
                            for c in content:
                                if isinstance(c, dict) and "text" in c:
                                    t = c["text"]
                                    if "[Engine]" in t or "[SQL Pushdown]" in t:
                                        print(f"> {t}")
                                elif isinstance(c, str):
                                    if "[Engine]" in c or "[SQL Pushdown]" in c:
                                        print(f"> {c}")
                break

if __name__ == "__main__":
    asyncio.run(test_full_scan())
