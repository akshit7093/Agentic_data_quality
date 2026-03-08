import re
import json

raw_tests = [
    """[
  {
    "rule_name": "test",
    "query": "SELECT * 
FROM table"
  }
]""",
    """[
  {
    "rule_name": "test2",
    "severity": "critical",
    "query": \"\"\"SELECT *
FROM table\"\"\"
  }
]"""
]

for raw in raw_tests:
    print("--- Original ---")
    print(raw)
    
    # Fix regex
    fixed = re.sub(r'("(?:[^"\\]|\\.)*")', lambda m: m.group(1).replace('\n', '\\n'), raw)
    
    print("--- Fixed ---")
    print(fixed)
    
    try:
        rules = json.loads(fixed)
        print("Success!", len(rules), "rules parsed.")
    except Exception as e:
        print("FAILED:", e)
