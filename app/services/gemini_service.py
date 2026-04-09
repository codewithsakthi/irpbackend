"""
gemini_service.py
----------------
Gemini AI integration for SPARK backend.
"""

import os
import httpx
import logging
from pydantic import BaseModel
from ..core.database import settings
from dotenv import load_dotenv
"""
gemini_service.py
----------------
Gemini AI integration for SPARK backend using google-generativeai SDK.
"""

import os
import logging



import httpx

DEFAULT_MODEL = "gemini-3-flash-preview"  # Change to any available model you want

import asyncio

async def gemini_generate_content(prompt: str, model: str = DEFAULT_MODEL) -> str:
    logger = logging.getLogger("gemini_service")
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("Gemini API key not set in environment.")
        raise RuntimeError("Gemini API key not set in environment.")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json"
    }
    data = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            result = response.json()
            return result["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        logger.error(f"Gemini API HTTP call failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Gemini API error response: {e.response.text}")
        raise
