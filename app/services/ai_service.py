"""
ai_service.py
-------------
Advanced AI service powered by DeepSeek-V3 via NVIDIA Integrate API.
Uses the OpenAI Python SDK for OpenAI-compatible streaming.
"""

from __future__ import annotations

import json
import asyncio
import logging
import time
import re
from typing import AsyncGenerator
from dataclasses import dataclass

import httpx
from openai import AsyncOpenAI
from ..core.database import settings

import io

try:
    import riva.client as riva_client
except Exception:  # pragma: no cover
    riva_client = None

try:
    import grpc
except Exception:  # pragma: no cover
    grpc = None

try:
    import av
except Exception:  # pragma: no cover
    av = None

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constants / Defaults
# ──────────────────────────────────────────────────────────────────────────────
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 240.0
CLIENT_TIMEOUT = httpx.Timeout(CONNECT_TIMEOUT, read=READ_TIMEOUT)
MAX_TOKENS = 16384
TEMPERATURE = 1.0
TOP_P = 0.9

SYSTEM_PROMPT_BASE = (
    "You are SPARK AI, an intelligent academic analytics assistant embedded inside "
    "the SPARK (Scalable Production-Grade Analytics for Academic Records & Knowledge) "
    "platform. You help administrators, staff, and students with concise, data-driven "
    "insights about academic performance, attendance, placement readiness, and risk. "
    "You have access to COMPLETE, REAL database records including every student's name, "
    "GPA, attendance, backlogs, and risk status. "
    "Always be specific, professional, and compassionate. "
    "When asked to list students, DO list them by name from the data provided — never say "
    "you lack the data if it is in your context. "
    "When listing multiple items, use bullet points or a numbered list for clarity. "
    "Never fabricate data — only reason from the context provided."
)

_GRADE_POINT_SQL = """
    CASE {col}
        WHEN 'O'    THEN 10  WHEN 'S'    THEN 10
        WHEN 'A+'   THEN 9   WHEN 'A'    THEN 8
        WHEN 'B+'   THEN 7   WHEN 'B'    THEN 6
        WHEN 'C'    THEN 5   WHEN 'D'    THEN 4
        WHEN 'E'    THEN 3   WHEN 'P'    THEN 5
        WHEN 'PASS' THEN 5   ELSE NULL
    END
"""

# ──────────────────────────────────────────────────────────────────────────────
# Core API Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _api_url() -> str:
    return settings.AI_API_URL

def _api_key() -> str:
    return settings.AI_API_KEY

def _model() -> str:
    return settings.AI_MODEL

_aio_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    """Singleton AsyncOpenAI client configured for NVIDIA Integrate."""
    global _aio_client
    if _aio_client is None:
        http_client = httpx.AsyncClient(timeout=CLIENT_TIMEOUT)
        _aio_client = AsyncOpenAI(
            base_url=_api_url(),
            api_key=_api_key(),
            max_retries=0,  # we handle retries ourselves
            http_client=http_client,
        )
    return _aio_client

_asr_client: AsyncOpenAI | None = None

def _get_asr_client() -> AsyncOpenAI:
    """Dedicated AsyncOpenAI client for ASR (NVIDIA Catalog URLs are different)."""
    global _asr_client
    if _asr_client is None:
        http_client = httpx.AsyncClient(timeout=CLIENT_TIMEOUT)
        _asr_client = AsyncOpenAI(
            base_url=settings.AI_ASR_URL,
            api_key=_api_key(),
            max_retries=1,
            http_client=http_client,
        )
    return _asr_client


@dataclass
class AsrServiceError(Exception):
    message: str
    source: str = "asr"
    status_code: int = 500

    def __str__(self) -> str:
        return self.message


class AsrUnavailableError(AsrServiceError):
    def __init__(self, message: str, source: str = "asr"):
        super().__init__(message=message, source=source, status_code=503)


class AsrBadRequestError(AsrServiceError):
    def __init__(self, message: str, source: str = "asr"):
        super().__init__(message=message, source=source, status_code=400)


def _build_riva_metadata() -> list[tuple[str, str]]:
    if not settings.AI_API_KEY:
        raise AsrUnavailableError("ASR API key is not configured.", source="riva")
    if not settings.AI_ASR_FUNCTION_ID:
        raise AsrUnavailableError("ASR function id is not configured.", source="riva")

    function_header = (settings.AI_ASR_FUNCTION_HEADER or "function-id").strip().lower()
    metadata = [
        ("authorization", f"Bearer {settings.AI_API_KEY}"),
        (function_header, settings.AI_ASR_FUNCTION_ID),
    ]

    # Some NVCF gateways accept only one of these forms. Sending both is harmless.
    if function_header != "function-id":
        metadata.append(("function-id", settings.AI_ASR_FUNCTION_ID))
    if function_header != "x-nvcf-function-id":
        metadata.append(("x-nvcf-function-id", settings.AI_ASR_FUNCTION_ID))

    return metadata


def _classify_grpc_error(error: Exception) -> AsrServiceError:
    if grpc is None or not hasattr(error, "code"):
        return AsrUnavailableError(f"Primary ASR provider unavailable: {error}", source="riva")

    status = error.code()
    details = error.details() or ""
    if status in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
        return AsrUnavailableError(f"Primary ASR provider unavailable: {details}", source="riva")
    if status in {grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.FAILED_PRECONDITION}:
        return AsrBadRequestError(f"Invalid ASR request: {details}", source="riva")
    return AsrServiceError(message=f"Riva transcription failed: {details}", source="riva", status_code=500)

def _build_payload(messages: list[dict], stream: bool = False, thinking: bool = True) -> dict:
    payload = {
        "model": _model(),
        "messages": messages,
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE,
        "top_p": TOP_P,
        "stream": stream,
    }
    if thinking:
        payload["extra_body"] = {"chat_template_kwargs": {"thinking": True}}
    return payload

# ──────────────────────────────────────────────────────────────────────────────
# ai_service.py — DeepSeek-V3 via NVIDIA Integrate API
# ────────────────────────────────────────────────────
# Core AI logic including tool-use, context building, and streaming.
# Replaced legacy phi4_service with DeepSeek-V3 for superior reasoning.
# ──────────────────────────────────────────────────────────────────────────────

def build_admin_context(data: dict | None, leaderboard: dict | None = None) -> str:
    if not data: return "No live dashboard data available."
    parts: list[str] = []
    health = data.get("department_health") or {}
    parts.append(
        f"DEPT_HEALTH: overall={health.get('overall_health_score', 0):.1f}%, "
        f"active_students={health.get('active_students', 0)}, "
        f"at_risk={health.get('at_risk_count', 0)}, "
        f"avg_gpa={health.get('average_gpa', 0)}, "
        f"avg_attendance={health.get('average_attendance', 0):.1f}%"
    )
    risk = data.get("risk_summary") or {}
    parts.append(f"RISK_SUMMARY: {risk}")
    top = data.get("top_performers") or []
    if top:
        rows = "\n".join(f" - {s.get('name')} (Roll: {s.get('roll_no')}) GPA={s.get('gpa')}" for s in top[:8])
        parts.append(f"TOP_PERFORMERS:\n{rows}")
    return "\n\n".join(parts)

async def build_live_context_from_db(db) -> str:
    from sqlalchemy import text as _t
    sections: list[str] = []
    gp = _GRADE_POINT_SQL
    try:
        snap = (await db.execute(_t(f"""
            SELECT COUNT(DISTINCT st.id) as total, (SELECT COUNT(*) FROM staff) as faculty
            FROM students st
        """))).mappings().first()
        if snap: sections.append(f"=== DEPT SNAPSHOT ===\nTotal Students: {snap['total']} | Faculty: {snap['faculty']}")
    except Exception as e: logger.warning(f"AI ctx [snapshot]: {e}")
    return "\n\n".join(sections)

def build_student_context(student: dict) -> str:
    if not student: return "No student data provided."
    return f"STUDENT: {student.get('name')} | Roll: {student.get('roll_no')} | GPA: {student.get('average_grade_points')} | Att%: {student.get('attendance_percentage')}"

# ──────────────────────────────────────────────────────────────────────────────
# Core API Logic (OpenAI SDK)
# ──────────────────────────────────────────────────────────────────────────────

async def generate(
    user_prompt: str,
    system: str = SYSTEM_PROMPT_BASE,
    thinking: bool = True,
    retries: int = 1
) -> str:
    payload = _build_payload(
        [{"role": "system", "content": system}, {"role": "user", "content": user_prompt}],
        stream=False,
        thinking=thinking,
    )

    for attempt in range(retries):
        try:
            logger.info("AI Gen Call via OpenAI client")
            client = _get_client()
            resp = await client.chat.completions.create(timeout=CLIENT_TIMEOUT, **payload)
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            logger.error(f"AI Gen Error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2**attempt)
            else:
                return "Generation failed."
    return "Generation failed."

async def stream_generate(
    user_prompt: str,
    system: str = SYSTEM_PROMPT_BASE,
    thinking: bool = True
) -> AsyncGenerator[str, None]:
    payload = _build_payload(
        [{"role": "system", "content": system}, {"role": "user", "content": user_prompt}],
        stream=True,
        thinking=thinking,
    )

    try:
        client = _get_client()
        completion = await client.chat.completions.create(timeout=CLIENT_TIMEOUT, **payload)
        async for chunk in completion:
            if not getattr(chunk, "choices", None):
                continue
            delta = chunk.choices[0].delta
            content = delta.content
            if content:
                yield content
    except Exception as e:
        logger.error(f"AI Stream Error: {e}")
        yield f"\n[AI Error: {str(e)}]"

# ──────────────────────────────────────────────────────────────────────────────
# Domain Wrappers
# ──────────────────────────────────────────────────────────────────────────────

async def answer_copilot_question(
    question: str,
    dashboard_context: str,
    chat_history: list[dict] | None = None,
) -> AsyncGenerator[str, None]:
    system = f"{SYSTEM_PROMPT_BASE}\n\n=== LIVE DATABASE KNOWLEDGE ===\n{dashboard_context}\n=== END OF DATA ==="
    messages = [{"role": "system", "content": system}]
    if chat_history: messages.extend(chat_history[-6:])
    messages.append({"role": "user", "content": question})

    payload = _build_payload(messages, stream=True, thinking=True)

    try:
        client = _get_client()
        completion = await client.chat.completions.create(timeout=CLIENT_TIMEOUT, **payload)
        async for chunk in completion:
            if not getattr(chunk, "choices", None):
                continue
            delta = chunk.choices[0].delta
            content = delta.content
            if content:
                yield content
    except Exception as e:
        logger.error(f"Copilot Error: {e}")
        yield f"⚠️ AI service error: {str(e)}"

async def get_admin_executive_briefing(dashboard_data: dict, leaderboard: dict | None = None) -> str:
    context = build_admin_context(dashboard_data, leaderboard)
    prompt = f"Given metrics:\n{context}\n\nWrite a 3-paragraph executive briefing."
    return await generate(prompt, thinking=False)

async def get_risk_narrative_for_batch(risk_data: list[dict]) -> str:
    if not risk_data: return "No risk data available."
    prompt = f"Analyze risk for {len(risk_data)} students. Provide cohort-level interventions."
    return await generate(prompt)

async def health_check() -> dict:
    start = time.monotonic()
    try:
        await generate("ping", thinking=False)
        return {"status": "ok", "model": _model(), "latency_ms": int((time.monotonic() - start) * 1000)}
    except Exception as e:
        return {"status": "error", "message": f"{type(e).__name__}: {str(e)}"}

# ── Voice Attendance (Whisper-large-v3 + DeepSeek-V3) ──────────────────────────

def _get_riva_auth():
    """
    Constructs the Riva Auth object with NVCF metadata.
    """
    if riva_client is None or grpc is None:
        raise AsrUnavailableError(
            "Riva ASR dependencies are not installed in this deployment.",
            source="riva",
        )

    try:
        _ = _build_riva_metadata()
        auth = riva_client.Auth(
            uri=settings.AI_ASR_URL or "grpc.nvcf.nvidia.com:443",
            use_ssl=True
        )
        logger.info(f"Riva gRPC client initialized for URI: {settings.AI_ASR_URL}")
        return auth
    except AsrServiceError:
        raise
    except Exception as e:
        logger.error(f"Failed to initialize Riva gRPC client: {e}")
        raise AsrUnavailableError(f"ASR initialization error: {e}", source="riva")


def _normalize_language_code() -> str:
    lang = (settings.AI_ASR_LANGUAGE_CODE or "en").strip()
    return lang or "en"


def _transcribe_with_riva(pcm_data: bytes) -> str:
    if riva_client is None or grpc is None:
        raise AsrUnavailableError(
            "Riva ASR dependencies are not installed in this deployment.",
            source="riva",
        )

    auth = _get_riva_auth()
    asr_service = riva_client.ASRService(auth)

    config = riva_client.RecognitionConfig(
        encoding=riva_client.AudioEncoding.LINEAR_PCM,
        sample_rate_hertz=16000,
        language_code=_normalize_language_code(),
        max_alternatives=1,
        enable_automatic_punctuation=True,
        audio_channel_count=1,
    )

    metadata = _build_riva_metadata()
    request = riva_client.proto.riva_asr_pb2.RecognizeRequest(
        config=config,
        audio=pcm_data,
    )

    try:
        response = asr_service.stub.Recognize(
            request,
            metadata=metadata,
            timeout=settings.AI_ASR_TIMEOUT_SECONDS,
        )
    except grpc.RpcError as e:
        raise _classify_grpc_error(e) from e
    except Exception as e:
        raise AsrServiceError(message=f"Unexpected Riva failure: {e}", source="riva", status_code=500) from e

    if not response.results:
        return ""
    return " ".join([res.alternatives[0].transcript for res in response.results])


async def _transcribe_with_openai(audio_bytes: bytes) -> str:
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise AsrUnavailableError("Fallback ASR provider is not configured.", source="openai")

    client = AsyncOpenAI(
        api_key=api_key,
        base_url=settings.OPENAI_ASR_BASE_URL,
        max_retries=0,
        http_client=httpx.AsyncClient(timeout=CLIENT_TIMEOUT),
    )
    try:
        response = await client.audio.transcriptions.create(
            model=settings.OPENAI_ASR_MODEL,
            file=("attendance.webm", io.BytesIO(audio_bytes), "audio/webm"),
            language=_normalize_language_code().split("-")[0],
        )
        text = getattr(response, "text", "") or ""
        return text.strip()
    except Exception as e:
        raise AsrUnavailableError(f"Fallback ASR provider unavailable: {e}", source="openai") from e
    finally:
        await client.close()

def _convert_to_pcm(audio_bytes: bytes) -> bytes:
    if av is None:
        raise AsrUnavailableError(
            "Audio conversion dependency 'av' is not installed in this deployment.",
            source="audio",
        )

    try:
        if not audio_bytes or len(audio_bytes) < 10:
            logger.error(f"Audio payload too small: {len(audio_bytes)} bytes.")
            raise ValueError("Empty or corrupt audio data received.")

        input_buffer = io.BytesIO(audio_bytes)
        
        # Explicitly use 'matroska' for WebM containers commonly sent by browsers.
        # This is more robust than auto-sniffing for small buffers.
        try:
            container = av.open(input_buffer, format='matroska')
        except Exception as e:
            logger.warning(f"Failed to open with 'matroska' hint, trying auto-sniff: {e}")
            input_buffer.seek(0)
            container = av.open(input_buffer)

        with container:
            if not container.streams.audio:
                raise Exception("No audio stream found in the input data.")
            stream = container.streams.audio[0]
            
            # Setup resampler for 16kHz Mono s16 (required by Riva)
            resampler = av.AudioResampler(format='s16', layout='mono', rate=16000)
            
            pcm_buffer = bytearray()
            for frame in container.decode(stream):
                resampled_frames = resampler.resample(frame)
                for resampled_frame in resampled_frames:
                    # 'AudioPlane' in some PyAV versions lacks .to_bytes(), 
                    # but the bytes() constructor works reliably.
                    pcm_buffer.extend(bytes(resampled_frame.planes[0]))
            
            # Flush resampler
            for resampled_frame in resampler.resample(None):
                pcm_buffer.extend(bytes(resampled_frame.planes[0]))
                
            logger.info(f"PCM conversion complete: {len(pcm_buffer)} bytes generated.")
            return bytes(pcm_buffer)
            
    except Exception as e:
        logger.error(f"Audio conversion with 'av' failed: {e}", exc_info=True)
        raise

async def transcribe_attendance_audio(audio_bytes: bytes) -> str:
    """
    Transcribes audio using NVIDIA Riva ASR.
    Input: WebM/OGG bytes from browser.
    Output: Transcribed text string.
    """
    try:
        logger.info(f"Starting ASR transcription: {len(audio_bytes)} bytes received.")

        # If PyAV is unavailable, skip Riva path and use OpenAI fallback directly.
        if av is None:
            logger.warning("PyAV not installed; routing ASR directly to fallback provider.")
            return await _transcribe_with_openai(audio_bytes)
        
        # 1. Convert to Mono PCM 16kHz
        try:
            pcm_data = _convert_to_pcm(audio_bytes)
            logger.info(f"Audio conversion successful: {len(pcm_data)} bytes PCM generated.")
        except Exception as e:
            logger.error(f"Audio conversion failed: {e}")
            raise AsrBadRequestError(f"Could not process audio format: {e}", source="audio") from e

        # 2. Primary transcription provider (Riva)
        logger.info("Sending request to Riva gRPC stub with metadata...")
        try:
            transcript = _transcribe_with_riva(pcm_data)
            logger.info("Riva response received via stub.")
            logger.info(f"Transcription successful: '{transcript[:50]}...'")
            return transcript
        except AsrUnavailableError as primary_error:
            logger.warning(f"Primary ASR unavailable ({primary_error.source}): {primary_error}")
            fallback_text = await _transcribe_with_openai(audio_bytes)
            logger.info("Fallback ASR transcription successful.")
            return fallback_text
        except AsrServiceError:
            raise

    except AsrServiceError as e:
        logger.error(f"transcribe_attendance_audio [{e.source}]: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"transcribe_attendance_audio: {e}", exc_info=True)
        raise AsrServiceError(message=f"Transcription engine error: {e}", source="asr", status_code=500) from e


def _ensure_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r"[,;\n]", value) if v.strip()]
    return [str(value).strip()]


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _normalize_roll(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value)


def _build_roster_index(roster: list[str]) -> list[dict]:
    items: list[dict] = []
    for raw in roster:
        raw_text = str(raw).strip()
        if not raw_text:
            continue

        if "-" in raw_text:
            roll, name = [p.strip() for p in raw_text.split("-", 1)]
        else:
            parts = raw_text.split(maxsplit=1)
            roll = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""

        if not roll:
            continue
        items.append(
            {
                "roll": roll,
                "roll_norm": _normalize_roll(roll),
                "roll_digits": _digits_only(roll),
                "name": name,
                "name_norm": _normalize_text(name),
            }
        )
    return items


def _resolve_roster_reference(raw_value: str, roster_index: list[dict]) -> str | None:
    candidate = str(raw_value or "").strip()
    if not candidate:
        return None

    roll_norm = _normalize_roll(candidate)
    roll_digits = _digits_only(candidate)
    candidate_norm = _normalize_text(candidate)

    # 1) Exact roll match (case/space/punctuation agnostic)
    for item in roster_index:
        if roll_norm and item["roll_norm"] == roll_norm:
            return item["roll"]

    # 2) Exact numeric roll match; handles "258 312" -> "258312"
    if roll_digits:
        exact_digit_matches = [item for item in roster_index if item["roll_digits"] == roll_digits]
        if len(exact_digit_matches) == 1:
            return exact_digit_matches[0]["roll"]

        suffix_matches = [
            item
            for item in roster_index
            if item["roll_digits"] and (item["roll_digits"].endswith(roll_digits) or roll_digits.endswith(item["roll_digits"]))
        ]
        if len(suffix_matches) == 1:
            return suffix_matches[0]["roll"]

    # 3) Name-like match from noisy phrase (e.g., "sudarshan absent")
    if candidate_norm:
        name_candidates = []
        for item in roster_index:
            name_norm = item["name_norm"]
            if not name_norm:
                continue
            if name_norm in candidate_norm or candidate_norm in name_norm:
                name_candidates.append((3, item))
                continue

            # Token overlap scoring for partial/fuzzy name mentions
            cand_tokens = {t for t in candidate_norm.split() if len(t) > 2}
            name_tokens = {t for t in name_norm.split() if len(t) > 2}
            overlap = len(cand_tokens.intersection(name_tokens))
            if overlap > 0:
                name_candidates.append((overlap, item))

        if name_candidates:
            name_candidates.sort(key=lambda x: x[0], reverse=True)
            top_score = name_candidates[0][0]
            top_items = [it for score, it in name_candidates if score == top_score]
            if len(top_items) == 1:
                return top_items[0]["roll"]

    return None


def _resolve_parsed_rolls(values, roster_index: list[dict]) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    unresolved: list[str] = []
    seen: set[str] = set()

    for raw in _ensure_list(values):
        match = _resolve_roster_reference(raw, roster_index)
        if match and match not in seen:
            seen.add(match)
            resolved.append(match)
        elif not match:
            unresolved.append(raw)

    return resolved, unresolved


_FILLER_TOKENS = {
    "uh", "um", "okay", "ok", "hmm", "huh", "please", "kindly", "then", "next",
    "students", "student", "mark", "marked", "set", "attendance", "is", "are", "to",
    "for", "the", "a", "an", "and", "also"
}


def _clean_mention_token(token: str) -> str:
    token = re.sub(r"\b(roll\s*no|rollnumber|roll\s*number|no\.?|number)\b", "", token, flags=re.IGNORECASE)
    token = token.strip(" .,:;|-_")
    if not token:
        return ""

    norm = _normalize_text(token)
    if norm in _FILLER_TOKENS:
        return ""
    return token


def _split_mentions(raw: str) -> list[str]:
    if not raw:
        return []
    parts = re.split(r"\b(?:and|&|,|/)\b", raw, flags=re.IGNORECASE)
    mentions: list[str] = []
    for part in parts:
        cleaned = _clean_mention_token(part)
        if cleaned:
            mentions.append(cleaned)
    return mentions


def _rule_based_attendance_parse(transcript: str) -> dict:
    text = str(transcript or "")
    absent: list[str] = []
    od: list[str] = []

    # Pattern 1: "<mentions> absent" or "<mentions> on duty/od"
    trailing_status = re.finditer(
        r"([a-z0-9\s,./&-]+?)\s+(absent|on\s*duty|od)\b",
        text,
        flags=re.IGNORECASE,
    )
    for match in trailing_status:
        mentions = _split_mentions(match.group(1))
        status = match.group(2).lower()
        if "absent" in status:
            absent.extend(mentions)
        else:
            od.extend(mentions)

    # Pattern 2: "absent: <mentions>" or "od: <mentions>"
    leading_status = re.finditer(
        r"\b(absent|on\s*duty|od)\b\s*[:\-]?\s*([a-z0-9\s,./&-]+)",
        text,
        flags=re.IGNORECASE,
    )
    for match in leading_status:
        status = match.group(1).lower()
        mentions = _split_mentions(match.group(2))
        if "absent" in status:
            absent.extend(mentions)
        else:
            od.extend(mentions)

    def _dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            key = _normalize_text(value)
            if key and key not in seen:
                seen.add(key)
                out.append(value)
        return out

    return {
        "absent": _dedupe(absent),
        "od": _dedupe(od),
    }

async def parse_attendance_from_transcript(transcript: str, roster: list[str]) -> dict:
    """
    Uses DeepSeek to parse the transcript into a list of Absent and OD roll numbers.
    Provided roster helps disambiguate names/roll numbers.
    """
    prompt = f"""
Transcript: "{transcript}"

Student Roster (Roll Numbers & Names):
{", ".join(roster[:100])}

Task: Extract students who are "Absent" (A) and students who are "On Duty" (OD).
Others are considered present. If a student is mentioned as "OD", do NOT mark them as "Absent".
If roll numbers aren't clear, match names against the roster.
Ignore filler words/pauses (uh, um, okay, and then, stop words) and focus only on actionable attendance mentions.

Return ONLY a JSON object with this exact structure:
{{
  "absent": ["roll_no1", "roll_no2"],
  "od": ["roll_no3"],
  "confidence_score": 0.0-1.0,
  "summary": "Brief summary of what was parsed"
}}
"""
    rule_based = _rule_based_attendance_parse(transcript)

    llm_parsed = {"absent": [], "od": [], "confidence_score": 0.0, "summary": ""}
    try:
        response_text = await generate(prompt, thinking=False)
        if "```json" in response_text:
            json_str = response_text.split("```json")[-1].split("```")[0].strip()
        else:
            json_str = response_text.strip()

        llm_parsed = json.loads(json_str)
    except Exception as e:
        logger.warning(f"LLM attendance parse fallback to rule-based parser: {e}")

    try:
        roster_index = _build_roster_index(roster)

        absent_candidates = [*_ensure_list(rule_based.get("absent", [])), *_ensure_list(llm_parsed.get("absent", []))]
        od_candidates = [*_ensure_list(rule_based.get("od", [])), *_ensure_list(llm_parsed.get("od", []))]

        absent_resolved, absent_unresolved = _resolve_parsed_rolls(absent_candidates, roster_index)
        od_resolved, od_unresolved = _resolve_parsed_rolls(od_candidates, roster_index)

        # OD always wins over absent if same student appears in both.
        od_set = set(od_resolved)
        absent_resolved = [roll for roll in absent_resolved if roll not in od_set]

        result = {
            "absent": absent_resolved,
            "od": od_resolved,
            "confidence_score": llm_parsed.get("confidence_score", 0.0),
            "summary": llm_parsed.get("summary", ""),
        }
        if absent_unresolved or od_unresolved:
            result["unresolved"] = {
                "absent": absent_unresolved,
                "od": od_unresolved,
            }
        return result
    except Exception as e:
        logger.error(f"Failed to parse AI response for voice attendance: {e}")
        return {"absent": [], "od": [], "error": "Parsing failed"}
