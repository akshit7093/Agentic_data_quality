"""
Universal LLM Response Sanitizer — Model-Agnostic Pipeline.

Handles output quirks from ANY LLM provider or model family:
  - Reasoning models (DeepSeek, Granite, QwQ): <think>...</think> blocks
  - Chat models (Llama, Mistral): hallucinated multi-turn continuations
  - Code models (CodeLlama, StarCoder): HTML tags, <code> blocks
  - All models: truncated JSON, excessive whitespace, prompt echo-back

Architecture: A pipeline of composable filter functions.
Each filter takes a string in and returns a string out.
Filters are applied in order — order matters for correctness.
"""

import re
import logging
from typing import List, Callable, Optional

logger = logging.getLogger(__name__)

# Type alias for a sanitizer filter function
SanitizerFilter = Callable[[str], str]


# ═══════════════════════════════════════════════════════════
# FILTER FUNCTIONS — each handles one class of LLM artifacts
# ═══════════════════════════════════════════════════════════

def strip_reasoning_blocks(raw: str) -> str:
    """Strip <think>...</think>, <reasoning>...</reasoning>, <scratchpad>...</scratchpad> blocks.
    
    Models: DeepSeek-R1, Granite, QwQ, any CoT/reasoning model.
    """
    patterns = [
        r'<think>[\s\S]*?</think>',
        r'<reasoning>[\s\S]*?</reasoning>',
        r'<scratchpad>[\s\S]*?</scratchpad>',
        r'<reflection>[\s\S]*?</reflection>',
        r'<inner_monologue>[\s\S]*?</inner_monologue>',
        r'<chain_of_thought>[\s\S]*?</chain_of_thought>',
        # Some models use |think| or [think] markers
        r'\|think\|[\s\S]*?\|/think\|',
        r'\[think\][\s\S]*?\[/think\]',
    ]
    for pattern in patterns:
        raw = re.sub(pattern, '', raw, flags=re.IGNORECASE)
    return raw.strip()


def strip_html_tags(raw: str) -> str:
    """Strip HTML formatting tags injected by code-trained or chat models.
    
    Models: CodeLlama, StarCoder, GPT-4 with markdown mode, Granite.
    Preserves content inside tags, only removes the tags themselves.
    """
    # Common HTML tags (NOT stripping our structural tags like <METADATA>, <REPORT>)
    raw = re.sub(
        r'</?(?:code|br|p|div|span|pre|em|strong|b|i|ul|li|ol|a|h[1-6]|table|tr|td|th|thead|tbody|blockquote|hr|img|input|button|form|label|select|option|textarea|section|article|nav|header|footer|main|aside|details|summary|figure|figcaption|mark|small|sub|sup|del|ins|abbr|cite|dfn|kbd|samp|var|wbr)(?:\s[^>]*)?\s*/?>',
        '', raw, flags=re.IGNORECASE
    )
    return raw


def strip_html_entities(raw: str) -> str:
    """Decode common HTML entities that LLMs sometimes output.
    
    Models: Any model trained on web data.
    """
    entity_map = {
        '&lt;': '<', '&gt;': '>', '&amp;': '&',
        '&quot;': '"', '&apos;': "'", '&#39;': "'",
        '&nbsp;': ' ', '&ndash;': '-', '&mdash;': '—',
        '&hellip;': '...', '&copy;': '©', '&reg;': '®',
    }
    for entity, replacement in entity_map.items():
        raw = raw.replace(entity, replacement)
    return raw


def strip_hallucinated_turns(raw: str) -> str:
    """Strip hallucinated multi-turn continuations.
    
    Models: Llama, Mistral, Phi — small models that hallucinate full conversations.
    Detects "ASSISTANT:", "USER:", "SYSTEM:", numbered turns, query separators.
    """
    hallucination_patterns = [
        r'\n\s*ASSISTANT\s*[:：]\s*',
        r'\n\s*USER\s*[:：]\s*',
        r'\n\s*SYSTEM\s*[:：]\s*',
        r'\n\s*(?:Turn|Iteration)\s*\d+\s*[:：]\s*',
        r'\n\s*---+\s*\n\s*Query\s*\d+',
        r'\n\s*###\s*(?:Response|Answer|Output)\s*\d+',
        # Chat template leakage
        r'\n\s*\[/?INST\]',
        r'\n\s*<\|(?:im_start|im_end|eot_id|start_header_id|end_header_id)\|>',
        # Llama-style role markers
        r'\n\s*<\|(?:user|assistant|system)\|>',
    ]

    earliest_cut = len(raw)
    for pattern in hallucination_patterns:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match and match.start() < earliest_cut:
            earliest_cut = match.start()

    if earliest_cut < len(raw):
        logger.debug(f"Sanitizer: stripped hallucinated content at pos {earliest_cut}")
        raw = raw[:earliest_cut].rstrip()

    return raw


def extract_structural_tag(raw: str) -> str:
    """If a complete <METADATA>...</METADATA> or <REPORT>...</REPORT> exists,
    strip trailing garbage after it.
    
    Purpose: Our agent protocol uses these tags as phase-completion signals.
    """
    for tag in ('METADATA', 'REPORT'):
        pattern = rf'(<{tag}>[\s\S]*?</{tag}>)'
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            end_pos = match.end()
            remaining = raw[end_pos:].strip()

            if remaining and len(remaining) > 20:
                has_another_action = re.search(r'"action"\s*:', remaining)
                has_another_tag = re.search(r'<(?:METADATA|REPORT)>', remaining, re.IGNORECASE)

                if has_another_action or has_another_tag:
                    logger.debug(f"Sanitizer: stripped {len(remaining)} chars after <{tag}> block")
                    raw = raw[:end_pos].rstrip()
            break
    return raw


def isolate_first_json_block(raw: str) -> str:
    """Keep only the first fenced JSON block, strip all trailing text.
    
    Purpose: LLMs often echo back prompt instructions after their JSON output.
    This prevents the echo-back from polluting conversation history.
    """
    # Match ```json ... ``` (with optional language tag)
    json_pattern = r'```(?:json|JSON)?\s*([\s\S]*?)\s*```'
    json_matches = list(re.finditer(json_pattern, raw, re.IGNORECASE))

    if json_matches:
        first_end = json_matches[0].end()
        text_before = raw[:json_matches[0].start()].rstrip()
        first_block = raw[json_matches[0].start():json_matches[0].end()]

        remaining = raw[first_end:].strip()
        has_structural_tag = re.search(r'<(?:METADATA|REPORT)>', remaining, re.IGNORECASE)

        if has_structural_tag:
            pass  # Keep both — routing logic will decide
        else:
            if remaining and len(remaining) > 10:
                logger.debug(f"Sanitizer: stripped {len(remaining)} chars of trailing text after JSON block")
            raw = (text_before + '\n' + first_block if text_before else first_block).rstrip()

    return raw


def detect_truncated_json(raw: str) -> str:
    """Detect truncated JSON blocks (finish_reason='length') and return empty to trigger retry.
    
    Purpose: If the LLM ran out of tokens mid-JSON, the response is useless.
    Returning empty signals the caller to request a retry.
    """
    if '```json' in raw.lower():
        after_open = raw.split('```json', 1)[1] if '```json' in raw else raw.split('```JSON', 1)[1]
        # Check if there's a closing ``` after the opening
        if '```' not in after_open:
            logger.warning("Sanitizer: detected truncated JSON block — returning empty for retry")
            return ""
    return raw


def normalize_whitespace(raw: str) -> str:
    """Clean up excessive whitespace, normalize newlines."""
    raw = re.sub(r'\n{3,}', '\n\n', raw)
    raw = re.sub(r'[ \t]+\n', '\n', raw)  # Trailing spaces on lines
    return raw.strip()


def strip_prompt_leakage(raw: str) -> str:
    """Strip cases where the LLM echoes back the system or user prompt.
    
    Models: Small local models that leak prompt context into their output.
    Detects common patterns like "You are a...", "Your task is to...", 
    "Output EXACTLY", "CRITICAL RULES:" etc.
    """
    leakage_patterns = [
        r'\n\s*(?:You are (?:a|an|the) )[A-Z][\s\S]{20,}',  # "You are a Data Quality..."
        r'\n\s*CRITICAL RULES:\s*\n',
        r'\n\s*Required JSON (?:Array )?Format:\s*\n',
        r'\n\s*(?:Output|Respond) (?:EXACTLY|ONLY|with)\b',
    ]
    
    earliest_cut = len(raw)
    for pattern in leakage_patterns:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match and match.start() < earliest_cut:
            # Only cut if we already have some meaningful content before
            content_before = raw[:match.start()].strip()
            if len(content_before) > 30:  # Must have real content first
                earliest_cut = match.start()
    
    if earliest_cut < len(raw):
        logger.debug(f"Sanitizer: stripped prompt leakage at pos {earliest_cut}")
        raw = raw[:earliest_cut].rstrip()
    
    return raw


# ═══════════════════════════════════════════════════════════
# THE PIPELINE — ordered list of filters applied sequentially
# ═══════════════════════════════════════════════════════════

DEFAULT_PIPELINE: List[SanitizerFilter] = [
    strip_reasoning_blocks,      # 1. Remove <think>, <reasoning>, etc.
    strip_html_tags,             # 2. Remove <code>, <br>, etc.
    strip_html_entities,         # 3. Decode &lt;, &gt;, etc.
    strip_hallucinated_turns,    # 4. Cut at ASSISTANT:, USER:, etc.
    strip_prompt_leakage,        # 5. Cut at echoed prompt instructions
    extract_structural_tag,      # 6. Isolate <METADATA>/<REPORT> blocks
    isolate_first_json_block,    # 7. Keep only first JSON fence
    detect_truncated_json,       # 8. Empty if JSON is truncated
    normalize_whitespace,        # 9. Clean up whitespace
]


class LLMResponseSanitizer:
    """
    Universal, model-agnostic LLM response sanitizer.
    
    Uses a pipeline of composable filter functions applied in order.
    Each filter handles one class of LLM output artifact.
    
    Usage:
        sanitizer = LLMResponseSanitizer()
        clean = sanitizer.sanitize(raw_llm_output)
    
    Customization:
        # Add a custom filter
        sanitizer = LLMResponseSanitizer()
        sanitizer.add_filter(my_custom_filter, position=3)
        
        # Remove a filter
        sanitizer.remove_filter("strip_html_tags")
    """
    
    def __init__(self, pipeline: Optional[List[SanitizerFilter]] = None):
        self.pipeline = list(pipeline or DEFAULT_PIPELINE)
    
    def sanitize(self, raw: str) -> str:
        """Run the full sanitization pipeline on raw LLM output."""
        if not raw:
            return raw
        
        original_len = len(raw)
        result = raw
        
        for filter_fn in self.pipeline:
            result = filter_fn(result)
            if not result:  # A filter returned empty (e.g., truncated JSON)
                logger.info(f"Sanitizer: filter '{filter_fn.__name__}' returned empty — aborting pipeline")
                return ""
        
        if len(result) < original_len:
            logger.info(f"Sanitized LLM response: {original_len} -> {len(result)} chars")
        
        return result
    
    def add_filter(self, filter_fn: SanitizerFilter, position: int = -1) -> None:
        """Add a custom filter to the pipeline at the given position (-1 = end)."""
        if position < 0:
            self.pipeline.append(filter_fn)
        else:
            self.pipeline.insert(position, filter_fn)
    
    def remove_filter(self, filter_name: str) -> bool:
        """Remove a filter by function name. Returns True if found and removed."""
        for i, fn in enumerate(self.pipeline):
            if fn.__name__ == filter_name:
                self.pipeline.pop(i)
                return True
        return False


# Singleton instance
_sanitizer: Optional[LLMResponseSanitizer] = None


def get_sanitizer() -> LLMResponseSanitizer:
    """Get or create the global sanitizer singleton."""
    global _sanitizer
    if _sanitizer is None:
        _sanitizer = LLMResponseSanitizer()
    return _sanitizer


def sanitize_llm_response(raw: str) -> str:
    """Convenience function — sanitize using global singleton."""
    return get_sanitizer().sanitize(raw)
