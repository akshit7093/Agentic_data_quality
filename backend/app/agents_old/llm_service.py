"""LLM service supporting Ollama, LM Studio, and cloud providers."""
import json
import logging
import re
from typing import List, Dict, Any, Optional, AsyncGenerator
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class LLMService:
    """Service for interacting with LLMs."""
    
    def __init__(self):
        self.settings = get_settings()
        self._llm: Optional[BaseChatModel] = None
        self._embedding_model = None

    @property
    def llm(self) -> BaseChatModel:
        """Get or create LLM instance."""
        if self._llm is None:
            self._llm = self._create_llm()
        return self._llm

    def _create_llm(self) -> BaseChatModel:
        """Create LLM instance based on configuration."""
        provider = self.settings.LLM_PROVIDER.strip().lower()
        
        if provider == "ollama":
            logger.info(f"Initializing Ollama LLM with model: {self.settings.OLLAMA_MODEL}")
            return ChatOllama(
                model=self.settings.OLLAMA_MODEL.strip(),
                base_url=self.settings.OLLAMA_BASE_URL.strip(),
                temperature=0.1,
                num_ctx=8192,
            )
        
        elif provider == "lmstudio":
            logger.info(f"Initializing LM Studio LLM at: {self.settings.LMSTUDIO_BASE_URL}")
            # ✅ FIXED: Use 'base_url' instead of deprecated 'openai_api_base'
            return ChatOpenAI(
                model=self.settings.LMSTUDIO_MODEL.strip(),
                base_url=self.settings.LMSTUDIO_BASE_URL.strip(),
                api_key=self.settings.LMSTUDIO_API_KEY or "not-needed",
                temperature=0.1,
                max_tokens=4096,
            )
        
        elif provider == "openai":
            if not self.settings.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required for OpenAI provider")
            logger.info(f"Initializing OpenAI LLM with model: {self.settings.OPENAI_MODEL}")
            return ChatOpenAI(
                model=self.settings.OPENAI_MODEL.strip(),
                api_key=self.settings.OPENAI_API_KEY.strip(),
                temperature=0.1,
            )
        
        elif provider == "anthropic":
            if not self.settings.ANTHROPIC_API_KEY:
                raise ValueError("ANTHROPIC_API_KEY is required for Anthropic provider")
            logger.info(f"Initializing Anthropic LLM with model: {self.settings.ANTHROPIC_MODEL}")
            return ChatAnthropic(
                model=self.settings.ANTHROPIC_MODEL.strip(),
                api_key=self.settings.ANTHROPIC_API_KEY.strip(),
                temperature=0.1,
            )
        
        else:
            raise ValueError(f"Unsupported LLM provider: '{provider}' (available: ollama, lmstudio, openai, anthropic)")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
    ) -> str:
        """Generate text from LLM."""
        messages = []
        
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt.strip()))
        
        messages.append(HumanMessage(content=prompt.strip()))
        
        try:
            response = await self.llm.ainvoke(messages)
            return response.content.strip()
        except Exception as e:
            logger.error(f"LLM generation error: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def generate_structured(
        self,
        prompt: str,
        output_schema: Dict[str, Any],
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate structured JSON output from LLM with ROBUST parsing."""
        json_system_prompt = """You are a data quality expert AI. ALWAYS respond with VALID JSON ONLY.
- NO explanatory text before or after the JSON
- NO markdown code blocks (```json or ```)
- NO trailing commas
- STRICTLY follow the requested schema format
- If unsure, omit optional fields rather than guessing"""
        
        if system_prompt:
            json_system_prompt = f"{system_prompt.strip()}\n\n{json_system_prompt}"
        
        schema_prompt = f"""
Respond with JSON matching this schema EXACTLY:
{json.dumps(output_schema, indent=2)}

YOUR RESPONSE MUST BE VALID JSON ONLY:"""
        
        full_prompt = f"{prompt.strip()}\n\n{schema_prompt}"
        
        # Get raw LLM response
        response = await self.generate(full_prompt, json_system_prompt)
        logger.debug(f"Raw LLM response (first 500 chars): {response[:500]}...")
        
        # ✅ CRITICAL FIX: Robust multi-strategy JSON extraction
        parsed_json = self._extract_json(response)
        
        if parsed_json is None:
            error_msg = (
                f"❌ FAILED to extract valid JSON from LLM response. "
                f"Preview: {response[:300]}... "
                f"(Length: {len(response)} chars)"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        rule_count = len(parsed_json.get('rules', []))
        logger.info(f"✅ Successfully parsed {rule_count} validation rules from LLM response")
        return parsed_json

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON using 4 layered strategies:
        1. Direct parse (with basic cleanup)
        2. Outermost { } braces extraction (handles raw JSON with newlines)
        3. Code block extraction (```json ... ```)
        4. First { to last } fallback
        """
        # Strategy 1: Direct parse with cleanup
        try:
            cleaned = text.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:].strip()
            if cleaned.startswith("```"):
                cleaned = cleaned[3:].strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract outermost { ... } (handles raw JSON with \n newlines)
        match = re.search(r'\{(?:[^{}]|(?R))*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        # Strategy 3: Code block extraction
        code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL | re.IGNORECASE)
        if code_block_match:
            try:
                return json.loads(code_block_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Strategy 4: First { to last } fallback
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end+1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
        
        logger.error(f"❌ All JSON extraction strategies failed. Preview: {text[:200]}...")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def generate_stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """Stream text generation from LLM."""
        messages = []
        
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt.strip()))
        
        messages.append(HumanMessage(content=prompt.strip()))
        
        try:
            async for chunk in self.llm.astream(messages):
                if chunk.content:
                    yield chunk.content
        except Exception as e:
            logger.error(f"LLM streaming error: {str(e)}")
            raise

    async def check_health(self) -> Dict[str, Any]:
        """Check LLM service health."""
        try:
            response = await self.generate(
                prompt="Respond with exactly: healthy",
                max_tokens=10
            )
            return {
                "status": "healthy",
                "provider": self.settings.LLM_PROVIDER,
                "model": self._get_model_name(),
                "response": response.strip(),
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "provider": self.settings.LLM_PROVIDER,
                "error": str(e),
            }

    def _get_model_name(self) -> str:
        """Get current model name."""
        provider = self.settings.LLM_PROVIDER.strip().lower()
        if provider == "ollama":
            return self.settings.OLLAMA_MODEL.strip()
        elif provider == "lmstudio":
            return self.settings.LMSTUDIO_MODEL.strip()
        elif provider == "openai":
            return self.settings.OPENAI_MODEL.strip()
        elif provider == "anthropic":
            return self.settings.ANTHROPIC_MODEL.strip()
        return "unknown"


# Singleton instance
_llm_service: Optional[LLMService] = None

def get_llm_service() -> LLMService:
    """Get LLM service singleton."""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service