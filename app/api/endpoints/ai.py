"""
ai.py — /api/v1/ai/* endpoints
────────────────────────────────
DeepSeek-V3 powered AI endpoints for the SPARK platform.

All endpoints require bearer token auth (same as other admin routes).
Streaming endpoints return text/event-stream (SSE).
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ...core import auth
from ...core.database import get_db
from ...models import base as models
from ...services import ai_service, enterprise_analytics
from ...services.gemini_service import gemini_generate_content
from fastapi import Body

from ...core.constants import CURRICULUM_CREDITS



logger = logging.getLogger(__name__)
router = APIRouter(tags=["AI"], responses={401: {"description": "Unauthorized"}})


# ──────────────────────────────────────────────────────────────────────────────
# Gemini LLM Endpoint
# ──────────────────────────────────────────────────────────────────────────────

class GeminiAskRequest(BaseModel):
    prompt: str

@router.post("/gemini/ask")
async def gemini_ask(
    body: GeminiAskRequest = Body(...),
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Get a response from Gemini LLM for a given prompt.
    """
    if not body.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt must not be empty.")
    try:
        result = await gemini_generate_content(body.prompt)
        return {"response": result}
    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        raise HTTPException(status_code=500, detail="Gemini API call failed.")


# ──────────────────────────────────────────────────────────────────────────────
# Schemas
# ──────────────────────────────────────────────────────────────────────────────

class CopilotAskRequest(BaseModel):
    question: str
    dashboard_context: Optional[str] = None  # pre-built context string from frontend
    chat_history: Optional[list[dict]] = None  # [{role, content}] for multi-turn


# ──────────────────────────────────────────────────────────────────────────────
# Gemini LLM Endpoint
# ──────────────────────────────────────────────────────────────────────────────

from fastapi import Body

class GeminiAskRequest(BaseModel):
    prompt: str

@router.post("/gemini/ask")
async def gemini_ask(
    body: GeminiAskRequest = Body(...),
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Get a response from Gemini LLM for a given prompt.
    """
    if not body.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt must not be empty.")
    try:
        result = await gemini_generate_content(body.prompt)
        return {"response": result}
    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        raise HTTPException(status_code=500, detail="Gemini API call failed.")


class StudentSummaryRequest(BaseModel):
    student_data: Optional[dict] = None  # if None, fetched from DB by roll_no


class RiskNarrativeRequest(BaseModel):
    risk_data: list[dict]  # list of at-risk student dicts


# ──────────────────────────────────────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/health")
async def ai_health(
    current_user: models.User = Depends(auth.get_current_user),
):
    """Check Phi-4 Mini availability and model status."""
    return await ai_service.health_check()


# ──────────────────────────────────────────────────────────────────────────────
# Streaming Q&A Co-pilot
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/copilot/ask")
async def copilot_ask(
    body: CopilotAskRequest,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Stream an answer to a free-form question using dashboard telemetry.
    Always fetches live DB context (student names, GPAs, risk data) and merges
    with any context the frontend provides. Returns Server-Sent Events.
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question must not be empty.")

    # ── Always build live context from DB (real student names + GPAs) ──────────
    try:
        live_context = await ai_service.build_live_context_from_db(db)
    except Exception as e:
        logger.warning(f"Live DB context failed: {e}")
        live_context = ""

    # ── Merge with any additional context the frontend sent ────────────────────
    frontend_ctx = body.dashboard_context or ""
    if live_context and frontend_ctx:
        # DB data comes first (most reliable), frontend ctx has UI-specific extras
        dashboard_context = f"{live_context}\n\n--- ADDITIONAL DASHBOARD CONTEXT ---\n{frontend_ctx}"
    elif live_context:
        dashboard_context = live_context
    else:
        dashboard_context = frontend_ctx or "No dashboard context available."

    async def event_stream():
        try:
            async for chunk in ai_service.answer_copilot_question(
                question=body.question,
                dashboard_context=dashboard_context,
                chat_history=body.chat_history,
            ):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
        except Exception as e:
            logger.error(f"Copilot stream error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ──────────────────────────────────────────────────────────────────────────────
# Student narrative (streaming)
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/student-summary/{roll_no}")
async def stream_student_summary(
    roll_no: str,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Stream a personalised AI narrative for a student.
    Fetches Student-360 profile from the DB then generates coaching text.
    Returns SSE.
    """
    # Fetch student 360 data
    try:
        profile = await enterprise_analytics.get_student_360(
            db, CURRICULUM_CREDITS, roll_no=roll_no
        )
        student_dict = profile.model_dump() if hasattr(profile, "model_dump") else dict(profile)
    except Exception as e:
        logger.warning(f"Could not fetch student 360 for {roll_no}: {e}")
        student_dict = {"roll_no": roll_no}

    async def event_stream():
        try:
            async for chunk in ai_service.stream_generate(
                user_prompt=(
                    f"Here is a student's academic profile:\n\n"
                    f"{ai_service.build_student_context(student_dict)}\n\n"
                    "Write a compassionate but honest academic narrative covering: "
                    "(1) current standing and key strengths, (2) areas of concern and root causes, "
                    "(3) 3 specific, actionable steps the student should take this semester. "
                    "Address the student directly using 'you'. Be warm, encouraging, and data-grounded."
                )
            ):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
        except Exception as e:
            logger.error(f"Student summary stream error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ──────────────────────────────────────────────────────────────────────────────
# Placement coaching (streaming)
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/placement-coach/{roll_no}")
async def stream_placement_coach(
    roll_no: str,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Stream placement coaching tips for a specific student. Returns SSE."""
    try:
        profile = await enterprise_analytics.get_student_360(
            db, CURRICULUM_CREDITS, roll_no=roll_no
        )
        student_dict = profile.model_dump() if hasattr(profile, "model_dump") else dict(profile)
    except Exception as e:
        logger.warning(f"Could not fetch student 360 for {roll_no}: {e}")
        student_dict = {"roll_no": roll_no}

    async def event_stream():
        try:
            async for chunk in ai_service.stream_generate(
                user_prompt=(
                    f"Student placement profile:\n\n"
                    f"{ai_service.build_student_context(student_dict)}\n\n"
                    "As a placement coach, write 3-4 specific coaching tips to improve this "
                    "student's industry readiness. Consider their coding score, GPA, internships, "
                    "and projects. Suggest concrete resources, timelines, and skills to develop. "
                    "Be direct and actionable. Use bullet points."
                )
            ):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
        except Exception as e:
            logger.error(f"Placement coach stream error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ──────────────────────────────────────────────────────────────────────────────
# Admin executive briefing (non-streaming, cached-friendly)
# ──────────────────────────────────────────────────────────────────────────────

class ExecBriefingRequest(BaseModel):
    dashboard_data: dict
    leaderboard_data: Optional[dict] = None


@router.post("/executive-briefing")
async def get_executive_briefing(
    body: ExecBriefingRequest,
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Generate an AI executive briefing from dashboard telemetry.
    Non-streaming; returns JSON with the briefing text.
    Intended for the Command Center header card.
    """
    text = await ai_service.get_admin_executive_briefing(
        body.dashboard_data,
        body.leaderboard_data,
    )
    return {"briefing": text, "model": ai_service._model()}


# ──────────────────────────────────────────────────────────────────────────────
# Batch risk narrative
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/risk-narrative")
async def get_risk_narrative(
    body: RiskNarrativeRequest,
    current_user: models.User = Depends(auth.get_current_user),
):
    """Non-streaming batch risk narrative. Returns JSON with the analysis text."""
    text = await ai_service.get_risk_narrative_for_batch(body.risk_data)
    return {"narrative": text, "model": ai_service._model()}

# ── Voice Attendance Parsing ───────────────────────────────────────────────────

@router.post("/attendance/voice-parse")
async def voice_parse_attendance(
    audio: UploadFile = File(...),
    roster: str = Form(...), # JSON stringified roster
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Upload an audio clip of faculty calling out names/rolls.
    Transcribes with Whisper-large-v3 and parses with DeepSeek-V3.
    """
    try:
        content = await audio.read()
        transcript = await ai_service.transcribe_attendance_audio(content)

        roster_list = json.loads(roster)
        if not isinstance(roster_list, list):
            raise HTTPException(status_code=400, detail="Roster must be a JSON array.")
        parsed = await ai_service.parse_attendance_from_transcript(transcript, roster_list)

        return {
            "transcript": transcript,
            "parsed": parsed
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid roster JSON payload.")
    except ai_service.AsrServiceError as e:
        logger.error(f"Voice parse ASR error [{e.source}]: {e}")
        raise HTTPException(status_code=e.status_code, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Voice parse error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected voice parsing failure.")
