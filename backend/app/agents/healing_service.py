
import logging
import json
from typing import Dict, Any, List, Optional
from app.agents.llm_service import get_llm_service
from app.agents.llm_sanitizer import sanitize_llm_response

logger = logging.getLogger(__name__)

class HealingService:
    """Service to handle self-healing of LLM-generated queries."""
    
    def __init__(self):
        self.llm_service = get_llm_service()

    async def get_correction(
        self, 
        error_msg: str, 
        original_query: str, 
        query_type: str,
        column_name: str,
        dtype: str,
        samples: List[Any],
        table_name: str,
        context: Optional[str] = None
    ) -> Optional[str]:
        """Ask the LLM to correct a failing query based on the error message."""
        
        system_prompt = f"""You are a Data Quality Recovery AI. 
Your task is to fix a failing {query_type.upper()} query that was intended to validate the column "{column_name}".

CRITICAL RULES:
1. Output ONLY the corrected {query_type.upper()} query string.
2. No reasoning, no markdown fences, no prose.
3. For SQL: Must be SQLite-compatible. Use double quotes for identifiers: "{column_name}".
4. For SQL: Must use COUNT(*) AS cnt.
5. For Pandas: Must evaluate to an integer (count of failing rows).
"""

        user_prompt = f"""
FAILING QUERY: {original_query}
ERROR MESSAGE: {error_msg}

CONTEXT:
- Table: {table_name}
- Column: {column_name}
- Data Type: {dtype}
- Sample Values: {samples[:10]}
{f"- Additional Context: {context}" if context else ""}

Please provide the CORRECTED {query_type.upper()} query that bypasses this error while achieving the same validation goal.
"""

        try:
            logger.info(f"HealingService: Requesting correction for error: {error_msg[:100]}")
            response = await self.llm_service.generate(
                prompt=user_prompt,
                system_prompt=system_prompt
            )
            corrected_query = sanitize_llm_response(response).strip()
            
            # Basic validation of the corrected query
            if not corrected_query or corrected_query.lower() == original_query.lower():
                logger.warning("HealingService: LLM returned empty or identical query.")
                return None
                
            logger.info(f"HealingService: Received corrected query: {corrected_query[:100]}")
            return corrected_query

        except Exception as e:
            logger.error(f"HealingService: Correction request failed: {e}")
            return None

    def format_healing_message(self, attempt: int, error: str) -> str:
        """Format a user-facing or log message for healing attempts."""
        return f"⚠️ HEALING ATTEMPT {attempt}: Previous query failed with error: {error}. Attempting auto-correction..."
