"""
rag_service.py
--------------
Retrieval-Augmented Generation (RAG) engine for SPARK.

Indexes the entire PostgreSQL database into ChromaDB vectors using
Gemini text-embedding-004. Provides context retrieval for AI chat.
"""

from __future__ import annotations

import sys
sys.modules['google._upb._message'] = None
sys.modules['google.protobuf.pyext._message'] = None

import asyncio
import hashlib
import logging
import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
import time
from datetime import datetime
from typing import Optional

import httpx
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.database import settings

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Gemini Embedding
# ──────────────────────────────────────────────────────────────────────────────

_embed_client: httpx.AsyncClient | None = None


def _get_embed_client() -> httpx.AsyncClient:
    global _embed_client
    if _embed_client is None:
        _embed_client = httpx.AsyncClient(timeout=60.0)
    return _embed_client


def _get_active_model_info() -> tuple[str, int]:
    """Return (model_name, dimensions) for the active API key configuration."""
    openrouter_key = settings.OPENROUTER_API_KEY or os.getenv("OPENROUTER_API_KEY")
    gemini_key = settings.GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")
    
    if openrouter_key and openrouter_key.strip():
        return "openrouter_nemotron", 2048
    elif gemini_key and gemini_key.strip():
        return "gemini_embedding_2", 3072
    else:
        raise RuntimeError("Neither OPENROUTER_API_KEY nor GEMINI_API_KEY is configured in .env.")


def _parse_retry_delay(resp_json: dict) -> float | None:
    try:
        details = resp_json.get("error", {}).get("details", [])
        for detail in details:
            if "RetryInfo" in detail.get("@type", ""):
                delay_str = detail.get("retryDelay", "")
                if delay_str.endswith("s"):
                    return float(delay_str[:-1])
    except Exception:
        pass
    return None


async def _embed_openrouter(texts: list[str], api_key: str) -> list[list[float]]:
    url = "https://openrouter.ai/api/v1/embeddings"
    client = _get_embed_client()
    all_embeddings: list[list[float]] = []
    
    batch_size = 50
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        payload = {
            "model": "nvidia/llama-nemotron-embed-vl-1b-v2:free",
            "input": batch,
            "encoding_format": "float"
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        
        max_retries = 7
        backoff = 2.0
        success = False
        resp = None
        
        for attempt in range(max_retries):
            try:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code == 429:
                    logger.warning(
                        f"RAG: OpenRouter 429 rate limit on embedding batch {i // batch_size + 1}. "
                        f"Sleeping for {backoff:.1f}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2.0
                    continue
                resp.raise_for_status()
                success = True
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(
                        f"RAG: OpenRouter 429 rate limit on embedding batch {i // batch_size + 1}. "
                        f"Sleeping for {backoff:.1f}s (attempt {attempt + 1}/{max_retries})...."
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2.0
                    continue
                else:
                    raise
            except Exception as e:
                logger.warning(
                    f"RAG: Network error on OpenRouter embedding batch {i // batch_size + 1}: {e}. "
                    f"Retrying in {backoff:.1f}s..."
                )
                await asyncio.sleep(backoff)
                backoff *= 2.0
                
        if not success:
            if resp is not None:
                resp.raise_for_status()
            raise RuntimeError(f"RAG: OpenRouter embedding failed.")
            
        data = resp.json()
        for item in data.get("data", []):
            all_embeddings.append(item["embedding"])
            
        if i + batch_size < len(texts):
            await asyncio.sleep(0.5)
            
    return all_embeddings


async def _embed_gemini(texts: list[str], api_key: str) -> list[list[float]]:
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:batchEmbedContents"
    client = _get_embed_client()
    all_embeddings: list[list[float]] = []
    
    batch_size = 30
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        payload = {
            "requests": [
                {"model": "models/gemini-embedding-2", "content": {"parts": [{"text": t}]}}
                for t in batch
            ]
        }
        headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
        
        max_retries = 7
        backoff = 4.0
        success = False
        resp = None
        
        for attempt in range(max_retries):
            try:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code == 429:
                    delay = backoff
                    try:
                        resp_json = resp.json()
                        parsed_delay = _parse_retry_delay(resp_json)
                        if parsed_delay is not None:
                            delay = parsed_delay + 1.0
                    except Exception:
                        pass
                    
                    logger.warning(
                        f"RAG: Gemini 429 rate limit on embedding batch {i // batch_size + 1}. "
                        f"Sleeping for {delay:.1f}s as requested by Google API (attempt {attempt + 1}/{max_retries})..."
                    )
                    await asyncio.sleep(delay)
                    backoff *= 2.0
                    continue
                resp.raise_for_status()
                success = True
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    delay = backoff
                    try:
                        resp_json = e.response.json()
                        parsed_delay = _parse_retry_delay(resp_json)
                        if parsed_delay is not None:
                            delay = parsed_delay + 1.0
                    except Exception:
                        pass
                    
                    logger.warning(
                        f"RAG: Gemini 429 rate limit on embedding batch {i // batch_size + 1}. "
                        f"Sleeping for {delay:.1f}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    await asyncio.sleep(delay)
                    backoff *= 2.0
                    continue
                else:
                    raise
            except Exception as e:
                logger.warning(
                    f"RAG: Network error on Gemini embedding batch {i // batch_size + 1}: {e}. "
                    f"Retrying in {backoff:.1f}s..."
                )
                await asyncio.sleep(backoff)
                backoff *= 2.0
                
        if not success:
            if resp is not None:
                resp.raise_for_status()
            raise RuntimeError(f"RAG: Gemini embedding failed.")
            
        data = resp.json()
        for emb in data.get("embeddings", []):
            all_embeddings.append(emb["values"])
            
        if i + batch_size < len(texts):
            is_paid_tier = os.getenv("RAG_PAID_TIER", "false").lower() == "true"
            if is_paid_tier:
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(20.0)
                
    return all_embeddings


async def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed a list of texts using OpenRouter (if configured) or fallback to Gemini."""
    openrouter_key = settings.OPENROUTER_API_KEY or os.getenv("OPENROUTER_API_KEY")
    gemini_key = settings.GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")

    if openrouter_key and openrouter_key.strip():
        return await _embed_openrouter(texts, openrouter_key)
    elif gemini_key and gemini_key.strip():
        return await _embed_gemini(texts, gemini_key)
    else:
        raise RuntimeError("Neither OPENROUTER_API_KEY nor GEMINI_API_KEY is configured in .env.")


async def embed_single(text: str) -> list[float]:
    """Embed a single text string."""
    results = await embed_texts([text])
    return results[0]


# ──────────────────────────────────────────────────────────────────────────────
# ChromaDB Collection
# ──────────────────────────────────────────────────────────────────────────────

_chroma_collection = None


def _get_collection():
    """Get or create the ChromaDB collection dynamically based on the active model."""
    global _chroma_collection
    if _chroma_collection is None:
        import chromadb

        model_name, dims = _get_active_model_info()
        collection_name = f"{settings.RAG_COLLECTION_NAME}_{model_name}"

        persist_dir = os.path.abspath(settings.RAG_PERSIST_DIR)
        os.makedirs(persist_dir, exist_ok=True)
        client = chromadb.PersistentClient(path=persist_dir)
        _chroma_collection = client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            f"ChromaDB collection '{collection_name}' (dim: {dims}) "
            f"loaded ({_chroma_collection.count()} docs) from {persist_dir}"
        )
    return _chroma_collection


# ──────────────────────────────────────────────────────────────────────────────
# Database Extraction & Chunking
# ──────────────────────────────────────────────────────────────────────────────

# Day-of-week map for timetable
_DAYS = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}


def _chunk_id(table: str, row_id) -> str:
    """Generate a deterministic chunk ID."""
    raw = f"{table}:{row_id}"
    return hashlib.md5(raw.encode()).hexdigest()


async def _extract_students(db: AsyncSession) -> list[dict]:
    """Extract student records with program info."""
    query = sql_text("""
        SELECT s.id, s.roll_no, s.name, s.dob, s.email, s.batch, s.section,
               s.current_semester, s.phone_primary, s.city, s.address,
               p.name AS program_name, p.code AS program_code
        FROM students s
        LEFT JOIN programs p ON s.program_id = p.id
        WHERE s.is_deleted = false
        ORDER BY s.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Student {r['name']} (Roll No: {r['roll_no']}, ID: {r['id']}). "
            f"Program: {r['program_name'] or 'N/A'} ({r['program_code'] or 'N/A'}). "
            f"Batch: {r['batch'] or 'N/A'}, Section: {r['section'] or 'N/A'}, "
            f"Current Semester: {r['current_semester'] or 'N/A'}. "
            f"Email: {r['email'] or 'N/A'}. Phone: {r['phone_primary'] or 'N/A'}. "
            f"City: {r['city'] or 'N/A'}. DOB: {r['dob'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("students", r["id"]),
            "text": text,
            "metadata": {
                "table": "students",
                "record_id": str(r["id"]),
                "roll_no": r["roll_no"] or "",
                "batch": r["batch"] or "",
                "section": r["section"] or "",
                "semester": str(r["current_semester"] or ""),
            },
        })
    return chunks


async def _extract_staff(db: AsyncSession) -> list[dict]:
    """Extract staff/faculty records."""
    query = sql_text("""
        SELECT s.id, s.name, s.email, s.department,
               sp.designation, sp.specialisation, sp.years_of_experience, sp.bio
        FROM staff s
        LEFT JOIN staff_profile sp ON s.id = sp.staff_id
        WHERE s.is_deleted = false
        ORDER BY s.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Faculty/Staff: {r['name']} (ID: {r['id']}). "
            f"Department: {r['department'] or 'N/A'}. "
            f"Designation: {r['designation'] or 'N/A'}. "
            f"Specialisation: {r['specialisation'] or 'N/A'}. "
            f"Experience: {r['years_of_experience'] or 'N/A'} years. "
            f"Email: {r['email'] or 'N/A'}. "
            f"Bio: {r['bio'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("staff", r["id"]),
            "text": text,
            "metadata": {"table": "staff", "record_id": str(r["id"])},
        })
    return chunks


async def _extract_subjects(db: AsyncSession) -> list[dict]:
    """Extract subject/course records."""
    query = sql_text("""
        SELECT s.id, s.course_code, s.name, s.credits, s.semester, s.is_active,
               s.pass_threshold, p.name AS program_name
        FROM subjects s
        LEFT JOIN programs p ON s.program_id = p.id
        WHERE s.is_deleted = false
        ORDER BY s.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        status = "Active" if r["is_active"] else "Inactive"
        text = (
            f"Subject: {r['name']} (Code: {r['course_code']}, ID: {r['id']}). "
            f"Program: {r['program_name'] or 'N/A'}. Semester: {r['semester'] or 'N/A'}. "
            f"Credits: {r['credits'] or 'N/A'}. Status: {status}. "
            f"Pass Threshold: {r['pass_threshold'] or 50}."
        )
        chunks.append({
            "id": _chunk_id("subjects", r["id"]),
            "text": text,
            "metadata": {
                "table": "subjects",
                "record_id": str(r["id"]),
                "semester": str(r["semester"] or ""),
            },
        })
    return chunks


async def _extract_assessments(db: AsyncSession) -> list[dict]:
    """Extract aggregated assessments per student per semester."""
    query = sql_text("""
        SELECT sa.student_id, sa.semester,
               sa.assessment_type, sa.marks, sa.grade, sa.result_status,
               st.name AS student_name, st.roll_no,
               sub.name AS subject_name, sub.course_code
        FROM student_assessments sa
        JOIN students st ON sa.student_id = st.id
        JOIN subjects sub ON sa.subject_id = sub.id
        WHERE sa.is_final = true
        ORDER BY sa.student_id, sa.semester, sub.name
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    
    # Group in Python
    groups: dict[tuple[int, str, str, int], list[str]] = {}
    for r in rows:
        key = (r["student_id"], r["student_name"], r["roll_no"], r["semester"])
        if key not in groups:
            groups[key] = []
        score_desc = f"{r['subject_name']} ({r['course_code']}): scored {r['marks'] or 'N/A'} marks (Grade: {r['grade'] or 'N/A'}, {r['assessment_type']}, Result: {r['result_status'] or 'N/A'})"
        groups[key].append(score_desc)
        
    chunks = []
    for (student_id, student_name, roll_no, semester), scores in groups.items():
        text = (
            f"Academic Performance Summary for {student_name} (Roll: {roll_no}, Student ID: {student_id}) in Semester {semester}:\n"
            + "\n".join(f"  - {score}" for score in scores)
        )
        chunks.append({
            "id": _chunk_id("assessments_summary", f"{student_id}_{semester}"),
            "text": text,
            "metadata": {
                "table": "assessments",
                "student_id": str(student_id),
                "roll_no": roll_no or "",
                "semester": str(semester),
            },
        })
    return chunks


async def _extract_attendance_summary(db: AsyncSession) -> list[dict]:
    """Extract aggregated attendance per student per semester."""
    query = sql_text("""
        SELECT pa.student_id, pa.semester,
               st.name AS student_name, st.roll_no,
               sub.name AS subject_name, sub.course_code,
               COUNT(*) AS total_periods,
               SUM(CASE WHEN pa.status = 'P' THEN 1 ELSE 0 END) AS present_count
        FROM period_attendance pa
        JOIN students st ON pa.student_id = st.id
        JOIN subjects sub ON pa.subject_id = sub.id
        GROUP BY pa.student_id, pa.semester, st.name, st.roll_no, sub.name, sub.course_code
        ORDER BY pa.student_id, pa.semester, sub.name
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    
    # Group in Python
    groups: dict[tuple[int, str, str, int], list[dict]] = {}
    for r in rows:
        key = (r["student_id"], r["student_name"], r["roll_no"], r["semester"])
        if key not in groups:
            groups[key] = []
        total = int(r["total_periods"])
        present = int(r["present_count"])
        pct = round((present / total * 100), 1) if total > 0 else 0
        groups[key].append({
            "subject": r["subject_name"],
            "code": r["course_code"],
            "present": present,
            "total": total,
            "pct": pct
        })
        
    chunks = []
    for (student_id, student_name, roll_no, semester), subjects in groups.items():
        total_p = sum(s["total"] for s in subjects)
        present_p = sum(s["present"] for s in subjects)
        overall_pct = round((present_p / total_p * 100), 1) if total_p > 0 else 0
        
        detail_lines = []
        for s in subjects:
            detail_lines.append(f"  - {s['subject']} ({s['code']}): {s['present']}/{s['total']} periods present ({s['pct']}%)")
            
        text = (
            f"Attendance Summary for {student_name} (Roll: {roll_no}, Student ID: {student_id}) in Semester {semester}:\n"
            f"Overall Attendance: {present_p}/{total_p} periods present ({overall_pct}%)\n"
            + "\n".join(detail_lines)
        )
        chunks.append({
            "id": _chunk_id("attendance_summary", f"{student_id}_{semester}"),
            "text": text,
            "metadata": {
                "table": "attendance",
                "student_id": str(student_id),
                "roll_no": roll_no or "",
                "semester": str(semester),
            },
        })
    return chunks


async def _extract_faculty_assignments(db: AsyncSession) -> list[dict]:
    """Extract faculty-subject assignments."""
    query = sql_text("""
        SELECT fsa.id, fsa.section, fsa.academic_year,
               st.name AS faculty_name,
               sub.name AS subject_name, sub.course_code
        FROM faculty_subject_assignments fsa
        JOIN staff st ON fsa.faculty_id = st.id
        JOIN subjects sub ON fsa.subject_id = sub.id
        ORDER BY fsa.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Teaching Assignment: {r['faculty_name']} teaches "
            f"{r['subject_name']} ({r['course_code']}), "
            f"Section: {r['section'] or 'N/A'}, "
            f"Academic Year: {r['academic_year'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("faculty_assignments", r["id"]),
            "text": text,
            "metadata": {"table": "faculty_assignments", "record_id": str(r["id"])},
        })
    return chunks


async def _extract_counselor_diary(db: AsyncSession) -> list[dict]:
    """Extract counselor meeting records."""
    query = sql_text("""
        SELECT cd.meeting_id, cd.semester, cd.meeting_date,
               cd.remark_category, cd.remarks, cd.action_planned,
               st.name AS student_name, st.roll_no,
               staff.name AS counselor_name
        FROM counselor_diary cd
        JOIN students st ON cd.student_id = st.id
        LEFT JOIN staff ON cd.counselor_id = staff.id
        ORDER BY cd.meeting_id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Counselor Meeting: {r['student_name']} (Roll: {r['roll_no']}) "
            f"on {r['meeting_date'] or 'N/A'}, Semester {r['semester'] or 'N/A'}. "
            f"Category: {r['remark_category'] or 'N/A'}. "
            f"Remarks: {r['remarks'] or 'N/A'}. "
            f"Action Planned: {r['action_planned'] or 'N/A'}. "
            f"Counselor: {r['counselor_name'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("counselor_diary", r["meeting_id"]),
            "text": text,
            "metadata": {
                "table": "counselor_diary",
                "record_id": str(r["meeting_id"]),
                "roll_no": r["roll_no"] or "",
                "semester": str(r["semester"] or ""),
            },
        })
    return chunks


async def _extract_extra_curricular(db: AsyncSession) -> list[dict]:
    """Extract extracurricular activity records."""
    query = sql_text("""
        SELECT ec.activity_id, ec.category, ec.description, ec.year, ec.activity_type,
               st.name AS student_name, st.roll_no
        FROM extra_curricular ec
        JOIN students st ON ec.student_id = st.id
        ORDER BY ec.activity_id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Extracurricular: {r['student_name']} (Roll: {r['roll_no']}) "
            f"participated in {r['category'] or 'N/A'} activity. "
            f"Description: {r['description'] or 'N/A'}. "
            f"Type: {r['activity_type'] or 'N/A'}. Year: {r['year'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("extra_curricular", r["activity_id"]),
            "text": text,
            "metadata": {
                "table": "extra_curricular",
                "record_id": str(r["activity_id"]),
                "roll_no": r["roll_no"] or "",
            },
        })
    return chunks


async def _extract_capability_scores(db: AsyncSession) -> list[dict]:
    """Extract student capability/placement scores."""
    query = sql_text("""
        SELECT scs.student_id, scs.academic_score, scs.communication_score,
               scs.leadership_score, scs.technical_score, scs.creativity_score,
               scs.sports_score, scs.discipline_score, scs.consistency_score,
               scs.placement_score, scs.growth_score, scs.spi_score,
               scs.profile_type, scs.placement_probability,
               st.name AS student_name, st.roll_no
        FROM student_capability_scores scs
        JOIN students st ON scs.student_id = st.id
        ORDER BY scs.student_id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Capability Scores for {r['student_name']} (Roll: {r['roll_no']}): "
            f"Academic: {r['academic_score'] or 'N/A'}, "
            f"Technical: {r['technical_score'] or 'N/A'}, "
            f"Communication: {r['communication_score'] or 'N/A'}, "
            f"Leadership: {r['leadership_score'] or 'N/A'}, "
            f"Creativity: {r['creativity_score'] or 'N/A'}, "
            f"Sports: {r['sports_score'] or 'N/A'}, "
            f"Discipline: {r['discipline_score'] or 'N/A'}, "
            f"Consistency: {r['consistency_score'] or 'N/A'}, "
            f"Placement Score: {r['placement_score'] or 'N/A'}, "
            f"Growth: {r['growth_score'] or 'N/A'}, "
            f"SPI: {r['spi_score'] or 'N/A'}. "
            f"Profile Type: {r['profile_type'] or 'N/A'}. "
            f"Placement Probability: {r['placement_probability'] or 'N/A'}%."
        )
        chunks.append({
            "id": _chunk_id("capability_scores", r["student_id"]),
            "text": text,
            "metadata": {
                "table": "capability_scores",
                "student_id": str(r["student_id"]),
                "roll_no": r["roll_no"] or "",
            },
        })
    return chunks


async def _extract_timetable(db: AsyncSession) -> list[dict]:
    """Extract timetable entries."""
    query = sql_text("""
        SELECT t.id, t.day_of_week, t.period, t.batch, t.section,
               t.semester, t.academic_year, t.room_number,
               t.start_time, t.end_time,
               sub.name AS subject_name, sub.course_code,
               st.name AS faculty_name
        FROM timetable t
        LEFT JOIN subjects sub ON t.subject_id = sub.id
        LEFT JOIN staff st ON t.faculty_id = st.id
        ORDER BY t.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        day = _DAYS.get(r["day_of_week"], f"Day {r['day_of_week']}")
        text = (
            f"Timetable: {day} Period {r['period']}, "
            f"Subject: {r['subject_name'] or 'Break/Free'} ({r['course_code'] or 'N/A'}), "
            f"Faculty: {r['faculty_name'] or 'N/A'}, "
            f"Room: {r['room_number'] or 'N/A'}, "
            f"Section: {r['section']}, Batch: {r['batch'] or 'N/A'}, "
            f"Semester: {r['semester'] or 'N/A'}, AY: {r['academic_year'] or 'N/A'}. "
            f"Time: {r['start_time'] or 'N/A'}-{r['end_time'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("timetable", r["id"]),
            "text": text,
            "metadata": {
                "table": "timetable",
                "record_id": str(r["id"]),
                "section": r["section"] or "",
                "batch": r["batch"] or "",
            },
        })
    return chunks


async def _extract_programs(db: AsyncSession) -> list[dict]:
    """Extract program records."""
    query = sql_text("SELECT id, code, name, degree_type, duration_years, total_semesters FROM programs")
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Program: {r['name']} (Code: {r['code']}). "
            f"Degree: {r['degree_type']}. Duration: {r['duration_years']} years "
            f"({r['total_semesters']} semesters)."
        )
        chunks.append({
            "id": _chunk_id("programs", r["id"]),
            "text": text,
            "metadata": {"table": "programs", "record_id": str(r["id"])},
        })
    return chunks


async def _extract_family_details(db: AsyncSession) -> list[dict]:
    """Extract family/guardian information."""
    query = sql_text("""
        SELECT fd.student_id, fd.parent_guardian_name, fd.occupation,
               fd.parent_phone, fd.father_name, fd.mother_name,
               fd.parent_email, fd.emergency_contact_name, fd.emergency_contact_phone,
               st.name AS student_name, st.roll_no
        FROM family_details fd
        JOIN students st ON fd.student_id = st.id
        ORDER BY fd.student_id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Family Details for {r['student_name']} (Roll: {r['roll_no']}): "
            f"Guardian: {r['parent_guardian_name'] or 'N/A'}. "
            f"Father: {r['father_name'] or 'N/A'}. Mother: {r['mother_name'] or 'N/A'}. "
            f"Parent Phone: {r['parent_phone'] or 'N/A'}. "
            f"Parent Email: {r['parent_email'] or 'N/A'}. "
            f"Occupation: {r['occupation'] or 'N/A'}. "
            f"Emergency Contact: {r['emergency_contact_name'] or 'N/A'} ({r['emergency_contact_phone'] or 'N/A'})."
        )
        chunks.append({
            "id": _chunk_id("family_details", r["student_id"]),
            "text": text,
            "metadata": {
                "table": "family_details",
                "student_id": str(r["student_id"]),
                "roll_no": r["roll_no"] or "",
            },
        })
    return chunks


async def _extract_growth_history(db: AsyncSession) -> list[dict]:
    """Extract student growth trajectory data."""
    query = sql_text("""
        SELECT sgh.id, sgh.student_id, sgh.semester, sgh.spi_score, sgh.growth_delta,
               st.name AS student_name, st.roll_no
        FROM student_growth_history sgh
        JOIN students st ON sgh.student_id = st.id
        ORDER BY sgh.student_id, sgh.semester
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        delta_str = ""
        if r["growth_delta"] is not None:
            delta = float(r["growth_delta"])
            delta_str = f" Growth Delta: {'+' if delta >= 0 else ''}{delta}."
        text = (
            f"Growth: {r['student_name']} (Roll: {r['roll_no']}) "
            f"Semester {r['semester']} SPI: {r['spi_score'] or 'N/A'}.{delta_str}"
        )
        chunks.append({
            "id": _chunk_id("growth_history", r["id"]),
            "text": text,
            "metadata": {
                "table": "growth_history",
                "record_id": str(r["id"]),
                "roll_no": r["roll_no"] or "",
                "semester": str(r["semester"]),
            },
        })
    return chunks


async def _extract_enrollments(db: AsyncSession) -> list[dict]:
    """Extract student-subject enrollment records."""
    query = sql_text("""
        SELECT sse.id, sse.semester, sse.academic_year, sse.status,
               st.name AS student_name, st.roll_no,
               sub.name AS subject_name, sub.course_code
        FROM student_subject_enrollment sse
        JOIN students st ON sse.student_id = st.id
        JOIN subjects sub ON sse.subject_id = sub.id
        ORDER BY sse.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Enrollment: {r['student_name']} (Roll: {r['roll_no']}) "
            f"enrolled in {r['subject_name']} ({r['course_code']}), "
            f"Semester {r['semester']}, AY: {r['academic_year']}. "
            f"Status: {r['status']}."
        )
        chunks.append({
            "id": _chunk_id("enrollments", r["id"]),
            "text": text,
            "metadata": {
                "table": "enrollments",
                "record_id": str(r["id"]),
                "roll_no": r["roll_no"] or "",
                "semester": str(r["semester"]),
            },
        })
    return chunks


async def _extract_academic_calendar(db: AsyncSession) -> list[dict]:
    """Extract academic calendar events."""
    query = sql_text("""
        SELECT ac.id, ac.academic_year, ac.semester, ac.event_type,
               ac.title, ac.start_date, ac.end_date, ac.description,
               p.name AS program_name
        FROM academic_calendar ac
        LEFT JOIN programs p ON ac.program_id = p.id
        ORDER BY ac.start_date
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Academic Calendar: {r['title']} ({r['event_type']}). "
            f"Dates: {r['start_date']} to {r['end_date']}. "
            f"Semester {r['semester']}, AY: {r['academic_year']}. "
            f"Program: {r['program_name'] or 'All'}. "
            f"Description: {r['description'] or 'N/A'}."
        )
        chunks.append({
            "id": _chunk_id("academic_calendar", r["id"]),
            "text": text,
            "metadata": {"table": "academic_calendar", "record_id": str(r["id"])},
        })
    return chunks


async def _extract_previous_academics(db: AsyncSession) -> list[dict]:
    """Extract previous academic records grouped by student."""
    query = sql_text("""
        SELECT pa.passing_year, pa.percentage, pa.institution, pa.board_university,
               st.id AS student_id, st.name AS student_name, st.roll_no
        FROM previous_academics pa
        JOIN students st ON pa.student_id = st.id
        ORDER BY pa.student_id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    
    # Group in Python
    groups: dict[tuple[int, str, str], list[str]] = {}
    for r in rows:
        key = (r["student_id"], r["student_name"], r["roll_no"])
        if key not in groups:
            groups[key] = []
        desc = f"Passing Year: {r['passing_year'] or 'N/A'}, Percentage: {r['percentage'] or 'N/A'}%, Institution: {r['institution'] or 'N/A'} ({r['board_university'] or 'N/A'})"
        groups[key].append(desc)
        
    chunks = []
    for (student_id, student_name, roll_no), records in groups.items():
        text = (
            f"Previous Academic History (Pre-college) for {student_name} (Roll: {roll_no}, Student ID: {student_id}):\n"
            + "\n".join(f"  - {rec}" for rec in records)
        )
        chunks.append({
            "id": _chunk_id("previous_academics_summary", student_id),
            "text": text,
            "metadata": {
                "table": "previous_academics",
                "student_id": str(student_id),
                "roll_no": roll_no or "",
            },
        })
    return chunks


async def _extract_staff_publications(db: AsyncSession) -> list[dict]:
    """Extract staff publication records."""
    query = sql_text("""
        SELECT sp.id, sp.title, sp.publication_type, sp.journal_or_conf,
               sp.year, sp.doi_or_url, sp.is_indexed,
               st.name AS staff_name
        FROM staff_publications sp
        JOIN staff st ON sp.staff_id = st.id
        ORDER BY sp.id
    """)
    result = await db.execute(query)
    rows = result.mappings().all()
    chunks = []
    for r in rows:
        text = (
            f"Publication by {r['staff_name']}: \"{r['title']}\". "
            f"Type: {r['publication_type'] or 'N/A'}. "
            f"Journal/Conference: {r['journal_or_conf'] or 'N/A'}. "
            f"Year: {r['year'] or 'N/A'}. "
            f"Indexed: {'Yes' if r['is_indexed'] else 'No'}."
        )
        chunks.append({
            "id": _chunk_id("staff_publications", r["id"]),
            "text": text,
            "metadata": {"table": "staff_publications", "record_id": str(r["id"])},
        })
    return chunks


# ──────────────────────────────────────────────────────────────────────────────
# Index Builder
# ──────────────────────────────────────────────────────────────────────────────

# Track indexing state
_index_state = {
    "is_indexing": False,
    "last_indexed_at": None,
    "total_docs": 0,
    "tables_indexed": [],
    "index_duration_seconds": 0,
    "error": None,
}


async def build_index(db: AsyncSession) -> dict:
    """
    Full RAG index pipeline:
    1. Extract all tables from DB
    2. Chunk rows into natural language
    3. Embed chunks via Gemini
    4. Upsert into ChromaDB
    """
    global _index_state

    if _index_state["is_indexing"]:
        return {"status": "already_indexing", "message": "Index build already in progress."}

    _index_state["is_indexing"] = True
    _index_state["error"] = None
    start_time = time.time()

    extractors = [
        ("students", _extract_students),
        ("staff", _extract_staff),
        ("subjects", _extract_subjects),
        ("programs", _extract_programs),
        ("assessments", _extract_assessments),
        ("attendance", _extract_attendance_summary),
        ("faculty_assignments", _extract_faculty_assignments),
        ("counselor_diary", _extract_counselor_diary),
        ("extra_curricular", _extract_extra_curricular),
        ("capability_scores", _extract_capability_scores),
        ("timetable", _extract_timetable),
        ("family_details", _extract_family_details),
        ("growth_history", _extract_growth_history),
        ("academic_calendar", _extract_academic_calendar),
        ("previous_academics", _extract_previous_academics),
        ("staff_publications", _extract_staff_publications),
    ]

    all_chunks: list[dict] = []
    tables_done: list[str] = []

    try:
        for table_name, extractor_fn in extractors:
            try:
                chunks = await extractor_fn(db)
                all_chunks.extend(chunks)
                tables_done.append(f"{table_name}({len(chunks)})")
                logger.info(f"RAG: Extracted {len(chunks)} chunks from {table_name}")
            except Exception as e:
                logger.warning(f"RAG: Failed to extract {table_name}: {e}")
                tables_done.append(f"{table_name}(ERROR)")

        if not all_chunks:
            _index_state["is_indexing"] = False
            _index_state["error"] = "No data extracted from database."
            return {"status": "error", "message": "No data found in database."}

        # Embed all chunks
        logger.info(f"RAG: Embedding {len(all_chunks)} chunks...")
        texts = [c["text"] for c in all_chunks]
        embeddings = await embed_texts(texts)
        logger.info(f"RAG: Embedding complete. Got {len(embeddings)} vectors.")

        # Upsert into ChromaDB
        collection = _get_collection()

        # Clear old data first
        try:
            existing = collection.count()
            if existing > 0:
                # Delete all existing docs
                all_ids = collection.get()["ids"]
                if all_ids:
                    # Delete in batches to avoid memory issues
                    batch_size = 5000
                    for i in range(0, len(all_ids), batch_size):
                        collection.delete(ids=all_ids[i : i + batch_size])
                logger.info(f"RAG: Cleared {existing} old documents.")
        except Exception as e:
            logger.warning(f"RAG: Could not clear old data: {e}")

        # Upsert in batches
        batch_size = 500
        for i in range(0, len(all_chunks), batch_size):
            batch_end = min(i + batch_size, len(all_chunks))
            batch_ids = [c["id"] for c in all_chunks[i:batch_end]]
            batch_docs = [c["text"] for c in all_chunks[i:batch_end]]
            batch_metas = [c["metadata"] for c in all_chunks[i:batch_end]]
            batch_embeds = embeddings[i:batch_end]

            collection.upsert(
                ids=batch_ids,
                documents=batch_docs,
                metadatas=batch_metas,
                embeddings=batch_embeds,
            )

        elapsed = round(time.time() - start_time, 1)
        _index_state.update({
            "is_indexing": False,
            "last_indexed_at": datetime.utcnow().isoformat(),
            "total_docs": len(all_chunks),
            "tables_indexed": tables_done,
            "index_duration_seconds": elapsed,
            "error": None,
        })

        logger.info(f"RAG: Index built successfully. {len(all_chunks)} docs in {elapsed}s.")
        return {
            "status": "success",
            "total_docs": len(all_chunks),
            "tables": tables_done,
            "duration_seconds": elapsed,
        }

    except Exception as e:
        _index_state["is_indexing"] = False
        _index_state["error"] = str(e)
        logger.error(f"RAG: Index build failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────────────────────────────────────
# Query / Retrieval
# ──────────────────────────────────────────────────────────────────────────────


async def query_rag(
    question: str,
    top_k: int | None = None,
    table_filter: str | None = None,
) -> tuple[str, list[dict]]:
    """
    Query the RAG index. Returns (context_string, source_chunks).
    """
    if top_k is None:
        top_k = settings.RAG_TOP_K

    collection = _get_collection()
    if collection.count() == 0:
        return "", []

    # Embed the question
    query_embedding = await embed_single(question)

    # Build filters
    where_filter = None
    if table_filter:
        where_filter = {"table": table_filter}

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=min(top_k, collection.count()),
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    if not results or not results["documents"] or not results["documents"][0]:
        return "", []

    docs = results["documents"][0]
    metas = results["metadatas"][0]
    distances = results["distances"][0]

    # Build context string
    context_parts: list[str] = []
    sources: list[dict] = []

    for doc, meta, dist in zip(docs, metas, distances):
        similarity = round(1 - dist, 3)  # cosine distance → similarity
        context_parts.append(doc)
        sources.append({
            "text": doc[:200],  # truncated for frontend display
            "table": meta.get("table", "unknown"),
            "similarity": similarity,
            "roll_no": meta.get("roll_no", ""),
        })

    context_string = "\n".join(context_parts)
    return context_string, sources


# ──────────────────────────────────────────────────────────────────────────────
# Status
# ──────────────────────────────────────────────────────────────────────────────


def get_index_status() -> dict:
    """Return current RAG index status."""
    try:
        collection = _get_collection()
        doc_count = collection.count()
    except Exception:
        doc_count = 0

    return {
        "indexed": doc_count > 0,
        "total_docs": doc_count,
        "is_indexing": _index_state["is_indexing"],
        "last_indexed_at": _index_state["last_indexed_at"],
        "tables_indexed": _index_state["tables_indexed"],
        "index_duration_seconds": _index_state["index_duration_seconds"],
        "error": _index_state["error"],
    }
