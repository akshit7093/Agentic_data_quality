import logging
import json
from datetime import datetime
from app.agents.llm_service import LLMService

logger = logging.getLogger(__name__)

class TicketingAgent:
    """Agent responsible for creating Jira/GitHub style markdown tickets for manual data errors."""
    
    def __init__(self):
        self.llm_service = LLMService()
        
    async def generate_ticket(self, rule_name: str, rule_details: dict, schema: dict, failure_examples: list) -> str:
        """Generates a structured markdown ticket containing all context needed for a human to fix the data."""
        
        system_prompt = """You are a Data Quality Engineering Assistant.
Your task is to draft a comprehensive, well-structured Jira/GitHub issue ticket in Markdown format.
The ticket is for a data steward or engineer to manually investigate and resolve a data quality anomaly that cannot be auto-fixed by SQL updates.

Please structure the ticket EXACTLY as follows:

# [Data Quality Anomaly] {Rule Name} Failure

**Priority:** High
**Created:** {Current Date}
**Target Table/Collection:** {Table Name}

## Description
A concise explanation of the anomaly based on the provided constraint/rule and the schema context.

## Schema Context
Briefly explain what the columns involved mean based on the provided schema.

## Affected Data (Sample)
Show a concise markdown table or code block of the failure examples provided.

## Recommended Investigation Steps
Provide 3-4 actionable steps a human should take to figure out WHY this data is anomalous (e.g., check upstream system X, verify physically, reach out to team Y).
"""

        user_prompt = f"""
Rule Name: {rule_name}
Detailed Rule Info: {json.dumps(rule_details, indent=2)}
Schema: {json.dumps(schema, indent=2)}
Failure Examples (up to 5): {json.dumps(failure_examples, indent=2)}
"""

        logger.info(f"TicketingAgent generating ticket for rule: {rule_name}")
        
        try:
            # Add dynamic timestamp replacement in the prompt
            system_prompt = system_prompt.replace("{Current Date}", datetime.now().strftime("%Y-%m-%d"))
            
            # Use LLM Service to generate the ticket
            response = await self.llm_service.generate_completion(system_prompt, user_prompt)
            return response
            
        except Exception as e:
            logger.error(f"Failed to generate ticket via LLM: {str(e)}")
            return f"""# [Data Quality Anomaly] {rule_name} Failure

**Priority:** High
**Target:** {schema.get('name', 'Unknown')}

## Description
Failed to generate AI reasoning for this ticket due to an LLM error: {str(e)}

## Raw Details
Rule Info: 
```json
{json.dumps(rule_details, indent=2)}
```

Examples:
```json
{json.dumps(failure_examples, indent=2)}
```
"""
