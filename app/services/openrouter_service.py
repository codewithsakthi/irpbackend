"""
openrouter_service.py
---------------------
Service to interface with OpenRouter API.
Supports streaming, reasoning tokens retrieval, and OpenAI compatibility.
"""

from __future__ import annotations
import json
import logging
import asyncio
from typing import AsyncGenerator
from openai import AsyncOpenAI
import httpx
from ..core.database import settings

logger = logging.getLogger(__name__)

def _api_key() -> str:
    return settings.OPENROUTER_API_KEY or ""

_client: AsyncOpenAI | None = None

def get_openrouter_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        key = _api_key()
        if not key:
            logger.warning("OPENROUTER_API_KEY is not set in settings or env!")
        else:
            masked = key[:6] + "..." + key[-4:] if len(key) > 10 else "..."
            logger.info(f"OpenRouter client initialized with key: {masked}")
        http_client = httpx.AsyncClient(timeout=60.0)
        _client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=key,
            http_client=http_client,
        )
    return _client

async def stream_openrouter_chat(
    messages: list[dict],
    model: str = "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
    temperature: float = 1.0,
    top_p: float = 0.9,
) -> AsyncGenerator[dict, None]:
    """
    Streams responses from OpenRouter and yields structured packets.
    Packets are dictionaries containing either 'text', 'reasoning', or 'usage' keys.
    """
    client = get_openrouter_client()
    try:
        completion = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            temperature=temperature,
            top_p=top_p,
            extra_body={"stream_options": {"include_usage": True}}
        )
        
        async for chunk in completion:
            # 1. Check for usage metrics (normally comes in the final chunk)
            if hasattr(chunk, "usage") and chunk.usage:
                usage_dict = chunk.usage if isinstance(chunk.usage, dict) else getattr(chunk.usage, "__dict__", {})
                reasoning_tokens = usage_dict.get("reasoning_tokens")
                # Fallback to model_extra for custom fields in some client libraries
                if not reasoning_tokens and hasattr(chunk.usage, "model_extra") and chunk.usage.model_extra:
                    reasoning_tokens = chunk.usage.model_extra.get("reasoning_tokens") or chunk.usage.model_extra.get("reasoningTokens")
                
                yield {
                    "usage": {
                        "prompt_tokens": usage_dict.get("prompt_tokens"),
                        "completion_tokens": usage_dict.get("completion_tokens"),
                        "total_tokens": usage_dict.get("total_tokens"),
                        "reasoning_tokens": reasoning_tokens
                    }
                }
                continue

            if not getattr(chunk, "choices", None):
                continue
                
            delta = chunk.choices[0].delta
            
            # 2. Extract reasoning content (if supported by the model)
            reasoning = None
            if hasattr(delta, "reasoning"):
                reasoning = delta.reasoning
            elif hasattr(delta, "reasoning_content"):
                reasoning = delta.reasoning_content
            elif hasattr(delta, "model_extra") and delta.model_extra:
                reasoning = delta.model_extra.get("reasoning") or delta.model_extra.get("reasoning_content")

            if reasoning:
                yield {"reasoning": reasoning}

            # 3. Extract standard content
            content = getattr(delta, "content", None)
            if content:
                yield {"text": content}
                
    except Exception as e:
        logger.error(f"OpenRouter Stream Error: {e}", exc_info=True)
        yield {"error": str(e)}
