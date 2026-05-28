"""SPICS — AI Capability Analyzer.
Calls NVIDIA/OpenAI-compatible API to analyze student professional profile.
All calls have timeout + retry. Failure NEVER propagates to caller — uses fallback.
"""
import json
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

AI_API_URL    = os.getenv("AI_API_URL", "https://integrate.api.nvidia.com/v1")
AI_API_KEY    = os.getenv("AI_API_KEY")
AI_MODEL      = os.getenv("AI_MODEL", "stepfun-ai/step-3.5-flash")
AI_TIMEOUT    = float(os.getenv("SPICS_AI_TIMEOUT_SECONDS", "15"))
AI_MAX_RETRY  = int(os.getenv("SPICS_AI_MAX_RETRIES", "2"))


def _build_profile_prompt(context: dict) -> str:
    return f"""You are an expert MCA student career advisor analyzing a student's professional profile.

Student Profile Data:
- Primary Domain: {context.get('primary_domain', 'Not specified')}
- Bio: {context.get('bio', 'Not provided')}
- Projects: {json.dumps(context.get('projects', []), indent=2)}
- Skills: {json.dumps(context.get('skills', []), indent=2)}
- Certifications: {json.dumps(context.get('certifications', []), indent=2)}
- GitHub Username: {context.get('github_username', 'Not linked')}
- Career Interests: {context.get('career_interest', [])}

Analyze this MCA student's professional profile and return a JSON object with EXACTLY these fields:
{{
  "technical_depth_score": <float 0-100>,
  "communication_score": <float 0-100>,
  "innovation_score": <float 0-100>,
  "collaboration_score": <float 0-100>,
  "project_maturity_score": <float 0-100>,
  "career_readiness_score": <float 0-100>,
  "ai_summary": "<2-3 sentence professional summary>",
  "strengths": ["<strength 1>", "<strength 2>", "<strength 3>"],
  "improvement_areas": ["<area 1>", "<area 2>", "<area 3>"],
  "career_fit_roles": [{{"role": "<role name>", "match": "High|Medium|Low"}}],
  "missing_skills": ["<skill 1>", "<skill 2>", "<skill 3>"]
}}

Return ONLY the JSON object. No markdown, no explanation."""


def _build_resume_prompt(resume_text: str) -> str:
    return f"""You are an expert technical recruiter analyzing an MCA student's resume.

Resume Content:
{resume_text[:3000]}

Analyze the resume and return a JSON object with EXACTLY these fields:
{{
  "extracted_skills": ["<skill1>", "<skill2>"],
  "extracted_projects": ["<project1>", "<project2>"],
  "extracted_technologies": ["<tech1>", "<tech2>"],
  "communication_quality": "<Excellent|Good|Fair|Needs Improvement>",
  "experience_level": "<Fresher|0-1 years|1-2 years|2+ years>",
  "strengths": ["<strength1>", "<strength2>"],
  "missing_skills": ["<skill1>", "<skill2>"],
  "placement_suggestions": ["<suggestion1>", "<suggestion2>"],
  "career_fit": ["<role1>", "<role2>"],
  "overall_score": <float 0-100>
}}

Return ONLY the JSON object. No markdown."""


async def _call_ai(prompt: str) -> Optional[dict]:
    """Makes the AI API call with timeout + retry. Returns parsed JSON or None."""
    if not AI_API_KEY:
        logger.warning("SPICS AI: No API key configured")
        return None

    for attempt in range(AI_MAX_RETRY + 1):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{AI_API_URL}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {AI_API_KEY}",
                        "Content-Type":  "application/json",
                    },
                    json={
                        "model": AI_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.3,
                        "max_tokens":  800,
                    },
                    timeout=AI_TIMEOUT,
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"].strip()
                # Strip markdown code fences if present
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                return json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"SPICS AI: JSON parse error on attempt {attempt+1}: {e}")
        except httpx.TimeoutException:
            logger.warning(f"SPICS AI: Timeout on attempt {attempt+1}")
        except Exception as e:
            logger.warning(f"SPICS AI: Error on attempt {attempt+1}: {e}")
    return None


async def analyze_student_profile(context: dict) -> Optional[dict]:
    """Analyzes student profile data. Returns AI result or None on failure."""
    prompt = _build_profile_prompt(context)
    return await _call_ai(prompt)


async def analyze_resume(resume_text: str) -> Optional[dict]:
    """Analyzes extracted resume text. Returns structured insights or None."""
    if not resume_text or len(resume_text) < 50:
        return None
    prompt = _build_resume_prompt(resume_text)
    return await _call_ai(prompt)
