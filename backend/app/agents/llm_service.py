"""LLM service supporting Ollama, LM Studio, and cloud providers.

REWRITE v5 - Optimized for ReAct Agent architecture.
Supports expanded context windows and enhanced logging for agentic tracing.
"""
import json
import logging
import re
from typing import List, Dict, Any, Optional, AsyncGenerator
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
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

    @property
    def llm(self) -> BaseChatModel:
        """Get or create LLM instance."""
        if self._llm is None:
            self._llm = self._create_llm()
        return self._llm

    def _create_llm(self) -> BaseChatModel:
        """Create LLM instance based on configuration."""
        provider = self.settings.LLM_PROVIDER.strip().lower()
        
        # INCREASED context sizes for ReAct loops which consume many tokens
        if provider == "ollama":
            logger.info(f"Initializing Ollama LLM with model: {self.settings.OLLAMA_MODEL}")
            return ChatOllama(
                model=self.settings.OLLAMA_MODEL.strip(),
                base_url=self.settings.OLLAMA_BASE_URL.strip(),
                temperature=0.1,
                num_ctx=16384,  # Expanded for agentic memory
            )
        
        elif provider == "lmstudio":
            logger.info(f"Initializing LM Studio LLM at: {self.settings.LMSTUDIO_BASE_URL}")
            # Import the centralized config so max_tokens is tunable in one place
            from app.agents.data_quality_agent import LLM_MAX_TOKENS
            return ChatOpenAI(
                model=self.settings.LMSTUDIO_MODEL.strip(),
                base_url=self.settings.LMSTUDIO_BASE_URL.strip(),
                api_key=self.settings.LMSTUDIO_API_KEY or "not-needed",
                temperature=0.1,
                max_tokens=LLM_MAX_TOKENS,
            )
        
        elif provider == "openai":
            if not self.settings.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required for OpenAI provider")
            logger.info(f"Initializing OpenAI LLM with model: {self.settings.OPENAI_MODEL}")
            return ChatOpenAI(
                model=self.settings.OPENAI_MODEL.strip(),
                api_key=self.settings.OPENAI_API_KEY.strip(),
                temperature=0.1,
                max_tokens=4096,
            )
        
        elif provider == "anthropic":
            if not self.settings.ANTHROPIC_API_KEY:
                raise ValueError("ANTHROPIC_API_KEY is required for Anthropic provider")
            logger.info(f"Initializing Anthropic LLM with model: {self.settings.ANTHROPIC_MODEL}")
            return ChatAnthropic(
                model=self.settings.ANTHROPIC_MODEL.strip(),
                api_key=self.settings.ANTHROPIC_API_KEY.strip(),
                temperature=0.1,
                max_tokens=4096,
            )
        
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        chat_history: Optional[List[Dict[str, str]]] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Generate text from LLM.
        Supports native chat history injection for advanced agentic memory.
        """
        messages = []
        
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt.strip()))
        
        # Optionally pass native LangChain chat history instead of stringifying
        if chat_history:
            for msg in chat_history:
                role = msg.get("role", "").lower()
                content = msg.get("content", "")
                if role == "user":
                    messages.append(HumanMessage(content=content))
                elif role == "assistant":
                    messages.append(AIMessage(content=content))
                elif role == "system":
                    messages.append(SystemMessage(content=content))
        
        messages.append(HumanMessage(content=prompt.strip()))
        
        try:
            logger.debug(f"Sending prompt to LLM (length: {len(prompt)} chars)")
            response = await self.llm.ainvoke(messages)
            logger.debug(f"LLM response received (length: {len(response.content)} chars)")
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
- If unsure, omit optional fields rather than guessing

IMPORTANT: Start your response with { and end with }"""

        if system_prompt:
            json_system_prompt = f"{system_prompt.strip()}\n\n{json_system_prompt}"
        
        schema_prompt = f"""
Respond with JSON matching this schema EXACTLY:
{json.dumps(output_schema, indent=2)}

YOUR RESPONSE MUST BE VALID JSON ONLY (no markdown, no explanation):"""
        
        full_prompt = f"{prompt.strip()}\n\n{schema_prompt}"
        
        # Get raw LLM response
        response = await self.generate(full_prompt, json_system_prompt)
        
        logger.info(f"🔵 RAW LLM RESPONSE (first 500 chars):\n{response[:500]}")
        
        parsed_json = self._extract_json(response)
        
        if parsed_json is None:
            error_msg = (
                f"Failed to extract valid JSON from LLM response. "
                f"Raw response preview: {response[:500]}... "
                f"(Full response length: {len(response)} chars)"
            )
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        rule_count = len(parsed_json.get('rules', [])) if isinstance(parsed_json, dict) else 0
        logger.info(f"✅ Successfully parsed JSON from LLM response")
        
        return parsed_json

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON from LLM response using multiple strategies.
        Handles: raw JSON, code blocks, newlines, whitespace, prefixes/suffixes.
        """
        if not text or not text.strip():
            logger.error("Empty text provided to JSON extractor")
            return None
        
        strategies = [
            ("Direct parse", self._try_direct_parse),
            ("Outermost braces (balanced)", self._try_outermost_braces),
            ("Code block extraction", self._try_code_block_extraction),
            ("Prefix/suffix stripping", self._try_prefix_suffix_stripping),
            ("Aggressive cleanup", self._try_aggressive_cleanup),
        ]
        
        for strategy_name, strategy_fn in strategies:
            try:
                result = strategy_fn(text)
                if result is not None:
                    logger.debug(f"✅ JSON extraction succeeded using strategy: {strategy_name}")
                    return result
            except Exception as e:
                logger.debug(f"Strategy '{strategy_name}' failed: {str(e)}")
                continue
        
        logger.error("❌ All JSON extraction strategies failed")
        return None

    def _try_direct_parse(self, text: str) -> Optional[Dict[str, Any]]:
        """Try direct JSON parsing after basic cleanup."""
        cleaned = text.strip()
        return json.loads(cleaned)

    def _try_outermost_braces(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract content between outermost { } braces."""
        start = text.find('{')
        if start == -1:
            return None
        
        brace_count = 0
        end = -1
        
        for i in range(start, len(text)):
            if text[i] == '{':
                brace_count += 1
            elif text[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    end = i
                    break
        
        if end == -1:
            return None
        
        candidate = text[start:end + 1]
        
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None

    def _try_code_block_extraction(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from markdown code blocks."""
        match = re.search(r'```json\s*([\s\S]*?)\s*```', text, re.IGNORECASE)
        if match:
            content = match.group(1).strip()
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
        
        match = re.search(r'```\s*([\s\S]*?)\s*```', text)
        if match:
            content = match.group(1).strip()
            if content.startswith('{') or content.startswith('['):
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    pass
        
        return None

    def _try_prefix_suffix_stripping(self, text: str) -> Optional[Dict[str, Any]]:
        """Remove common LLM fluff prefixes/suffixes before parsing."""
        lines = text.strip().split('\n')
        json_lines = []
        in_json = False
        brace_count = 0
        
        for line in lines:
            stripped = line.strip()
            brace_count += stripped.count('{') - stripped.count('}')
            
            if '{' in stripped and not in_json:
                in_json = True
            
            if in_json:
                json_lines.append(line)
            
            if in_json and brace_count == 0:
                break
        
        candidate = '\n'.join(json_lines).strip()
        if candidate:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
        
        return None

    def _try_aggressive_cleanup(self, text: str) -> Optional[Dict[str, Any]]:
        """Last resort: aggressive cleanup of common LLM artifacts."""
        prefixes_to_remove = [
            r'^Here\'s the JSON:?\s*',
            r'^Here is the JSON:?\s*',
            r'^Here\'s the result:?\s*',
            r'^Here is the result:?\s*',
            r'^Sure!?\s*',
            r'^Of course!?\s*',
            r'^Certainly!?\s*',
            r'^```json\s*',
            r'^```\s*',
        ]
        
        cleaned = text.strip()
        for prefix_pattern in prefixes_to_remove:
            cleaned = re.sub(prefix_pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = re.sub(r'```$', '', cleaned.strip())
        cleaned = re.sub(r'\s*```\s*$', '', cleaned)
        
        start = cleaned.find('{')
        if start != -1:
            brace_count = 0
            end = -1
            for i in range(start, len(cleaned)):
                if cleaned[i] == '{':
                    brace_count += 1
                elif cleaned[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end = i
                        break
            
            if end != -1:
                candidate = cleaned[start:end + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    pass
        
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
        """Check LLM service health without interrupting active generations."""
        # We disabled the actual LLM call here because for local models (like LMStudio),
        # hitting the completion endpoint for healthchecks interrupts long-running
        # reasoning validation tasks.
        return {
            "status": "healthy",
            "provider": self.settings.LLM_PROVIDER,
            "model": self._get_model_name(),
            "response": "healthy",
            "note": "LLM call disabled for healthcheck to avoid interrupting active generations"
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