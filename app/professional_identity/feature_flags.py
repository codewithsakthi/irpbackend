"""
Feature flags for Student Professional Identity & Capability System (SPICS).
All flags read from environment variables with safe defaults.
If the root flag is disabled, the entire module is invisible — no errors.
"""
import os
from functools import lru_cache


def _bool(key: str, default: str = "true") -> bool:
    return os.getenv(key, default).lower() in ("1", "true", "yes", "on")


@lru_cache(maxsize=1)
def get_flags() -> dict:
    return {
        "ENABLE_PROFESSIONAL_IDENTITY": _bool("ENABLE_PROFESSIONAL_IDENTITY", "true"),
        "ENABLE_RESUME_ANALYZER":       _bool("ENABLE_RESUME_ANALYZER", "true"),
        "ENABLE_GITHUB_ANALYTICS":      _bool("ENABLE_GITHUB_ANALYTICS", "true"),
        "ENABLE_AI_CAPABILITY_ENGINE":  _bool("ENABLE_AI_CAPABILITY_ENGINE", "true"),
    }


# Convenience singleton
FLAGS = get_flags()


def require_flag(flag_name: str):
    """FastAPI dependency — raises 503 if a feature flag is disabled."""
    from fastapi import HTTPException
    if not FLAGS.get(flag_name, False):
        raise HTTPException(
            status_code=503,
            detail=f"Feature '{flag_name}' is not enabled on this deployment.",
        )
