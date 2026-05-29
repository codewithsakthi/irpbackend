"""SPICS — Profile, Project, Certification, Skill, and Career Readiness routers"""
import json
import logging
import os
from typing import Optional
import httpx
from pydantic import BaseModel
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile, Response, Body
from fastapi.responses import FileResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path

logger = logging.getLogger(__name__)

from ...core.database import get_db
from ...core import auth
from ...models import base as core_models
from ..feature_flags import FLAGS, require_flag
from ..schemas.schemas import (
    CareerReadinessResponse, CertificationCreateRequest, CertificationResponse,
    CertificationUpdateRequest,
    MessageResponse, ProfileCreateRequest, ProfileResponse,
    ProjectCreateRequest, ProjectResponse, ProjectUpdateRequest,
    SkillCreateRequest, SkillResponse, SkillUpdateRequest,
    ResumeUploadResponse,
)
from ..services.services import (
    CertificationService, ProfileService, ProjectService,
    SkillService, CareerReadinessService,
)
from ..validators.validators import validate_upload_file
from ..services.resume_service import extract_resume_text, save_resume_file
from ..repositories.profile_repo import ProfileRepository
from ..utils.utils import UPLOAD_BASE, get_picture_upload_dir

# ── Profile Router ─────────────────────────────────────────────────────────────

profile_router = APIRouter(prefix="/profile", tags=["SPICS — Profile"])


@profile_router.get("/linkedin-picture")
async def get_linkedin_picture(
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch and proxy the cached LinkedIn profile picture to avoid CORS and auth issues in the browser."""
    _check_root_flag()
    student_id = _get_student_id(current_user)
    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile or not profile.linkedin_cache_data:
        raise HTTPException(status_code=404, detail="No LinkedIn profile linked or cached.")

    # Parse JSON if stored as a string
    cache = profile.linkedin_cache_data
    if isinstance(cache, str):
        try:
            cache = json.loads(cache)
        except Exception:
            cache = {}

    # Extract picture URL from cached LinkedIn data
    pic_url = cache.get("picture") or cache.get("picture_url") or cache.get("profilePicture")
    if not pic_url:
        raise HTTPException(status_code=404, detail="No LinkedIn picture URL found in cached data.")

    # Proxy the picture
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(pic_url, timeout=10.0)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type=resp.headers.get("content-type", "image/jpeg"),
                    headers={
                        "Cache-Control": "public, max-age=86400",
                    }
                )
        except Exception as e:
            logger.warning(f"Failed to proxy LinkedIn picture from {pic_url}: {e}")

    raise HTTPException(status_code=404, detail="Failed to fetch LinkedIn picture from remote URL.")


@profile_router.get("/{student_id}", response_model=ProfileResponse)
async def get_profile(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get professional profile for a student."""
    _check_root_flag()
    profile = await ProfileService.get_or_none(student_id, db)
    if not profile:
        raise HTTPException(status_code=404, detail="Professional profile not found. Student has not created a profile yet.")
    return profile


@profile_router.post("", response_model=ProfileResponse, status_code=201)
async def upsert_profile(
    req: ProfileCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create or update the current student's professional profile."""
    _check_root_flag()
    student_id = _get_student_id(current_user)
    profile = await ProfileService.upsert(student_id, req, db)
    # Queue AI analysis asynchronously — non-blocking
    if FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True):
        from ..workers.ai_worker import run_ai_analysis
        background_tasks.add_task(run_ai_analysis, student_id, db)
    return profile


@profile_router.post("/connect-linkedin", response_model=MessageResponse)
async def connect_linkedin(
    username: str = Body(..., embed=True),
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Store LinkedIn profile URL from username."""
    _check_root_flag()
    student_id = _get_student_id(current_user)

    # Normalize to URL
    username = username.strip().replace("https://www.linkedin.com/in/", "").replace("linkedin.com/in/", "").strip("/")
    linkedin_url = f"https://www.linkedin.com/in/{username}/"

    from ..repositories.profile_repo import ProfileRepository
    profile_repo = ProfileRepository(db)
    profile = await profile_repo.get_by_student_id(student_id)
    if not profile:
        profile = await profile_repo.create(student_id, {})

    await profile_repo.update(profile, {
        "linkedin_url": linkedin_url,
    })

    return MessageResponse(
        message=f"LinkedIn profile linked: {linkedin_url}",
        detail=linkedin_url,
    )


@profile_router.post("/upload-picture", response_model=MessageResponse)
async def upload_profile_picture(
    file: UploadFile = File(...),
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload professional profile picture."""
    _check_root_flag()
    student_id = _get_student_id(current_user)

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image size exceeds 5MB limit.")

    ext = Path(file.filename).suffix.lower() if file.filename else ".png"
    if ext not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
        raise HTTPException(
            status_code=400,
            detail="Unsupported image format. Allowed formats: PNG, JPEG, GIF, WEBP."
        )

    # 1. Get and clean directory
    upload_dir = get_picture_upload_dir(student_id)
    for existing_file in upload_dir.glob("custom.*"):
        try:
            existing_file.unlink()
        except Exception:
            pass

    # 2. Save new image
    filename = f"custom{ext}"
    dest_path = upload_dir / filename
    dest_path.write_bytes(content)

    # 3. Update database
    profile_repo = ProfileRepository(db)
    profile = await profile_repo.get_by_student_id(student_id)
    if not profile:
        profile = await profile_repo.create(student_id, {})

    # Save relative path so the server can serve it locally
    picture_rel_path = f"custom{ext}"
    await profile_repo.update(profile, {
        "picture_url": picture_rel_path
    })

    return MessageResponse(
        message="Profile picture uploaded successfully.",
        detail=picture_rel_path,
    )


@profile_router.get("/{student_id}/picture")
async def get_profile_picture(
    student_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Serve or redirect profile picture for a student."""
    _check_root_flag()
    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile:
        raise HTTPException(status_code=404, detail="No profile picture found.")

    picture_url = profile.picture_url

    # Fallback to GitHub avatar if no picture is set but GitHub is available
    if not picture_url and profile.github_username:
        picture_url = f"https://github.com/{profile.github_username}.png"

    if not picture_url:
        raise HTTPException(status_code=404, detail="No profile picture found.")

    # If it is a remote URL (e.g. GitHub/LinkedIn), redirect directly
    if picture_url.startswith(("http://", "https://")):
        return RedirectResponse(url=picture_url)

    # Otherwise, resolve local file path
    upload_dir = get_picture_upload_dir(student_id)
    full_path = (upload_dir / picture_url).resolve()

    uploads_root = UPLOAD_BASE if UPLOAD_BASE.is_absolute() else (Path.cwd() / UPLOAD_BASE)
    uploads_root = uploads_root.resolve()

    # Prevent path traversal and verify existence
    if uploads_root not in full_path.parents and full_path != uploads_root:
        raise HTTPException(status_code=400, detail="Invalid picture file path.")

    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Profile picture file not found.")

    suffix = full_path.suffix.lower()
    media_type = "image/png"
    if suffix in {".jpg", ".jpeg"}:
        media_type = "image/jpeg"
    elif suffix == ".gif":
        media_type = "image/gif"
    elif suffix == ".webp":
        media_type = "image/webp"

    return FileResponse(
        path=str(full_path),
        media_type=media_type,
    )




# ── Project Router ─────────────────────────────────────────────────────────────

project_router = APIRouter(prefix="/projects", tags=["SPICS — Projects"])


@project_router.get("/{student_id}", response_model=list[ProjectResponse])
async def list_projects(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    return await ProjectService.list(student_id, db)


@project_router.post("", response_model=ProjectResponse, status_code=201)
async def create_project(
    req: ProjectCreateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    _ensure_profile_exists(student_id)
    return await ProjectService.create(student_id, req, db)


@project_router.patch("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: int,
    req: ProjectUpdateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    return await ProjectService.update(project_id, student_id, req, db)


@project_router.delete("/{project_id}", response_model=MessageResponse)
async def delete_project(
    project_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    await ProjectService.delete(project_id, student_id, db)
    return MessageResponse(message="Project deleted successfully")


# ── Certification Router ───────────────────────────────────────────────────────

cert_router = APIRouter(prefix="/certifications", tags=["SPICS — Certifications"])


@cert_router.get("/{student_id}", response_model=list[CertificationResponse])
async def list_certifications(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    return await CertificationService.list(student_id, db)


@cert_router.post("", response_model=CertificationResponse, status_code=201)
async def create_certification(
    req: CertificationCreateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    return await CertificationService.create(student_id, req, db)


@cert_router.patch("/{cert_id}", response_model=CertificationResponse)
async def update_certification(
    cert_id: int,
    req: CertificationUpdateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    return await CertificationService.update(cert_id, student_id, req, db)


@cert_router.delete("/{cert_id}", response_model=MessageResponse)
async def delete_certification(
    cert_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    await CertificationService.delete(cert_id, student_id, db)
    return MessageResponse(message="Certification deleted")


# ── Skill Router ───────────────────────────────────────────────────────────────

skill_router = APIRouter(prefix="/skills", tags=["SPICS — Skills"])


@skill_router.get("/{student_id}", response_model=list[SkillResponse])
async def list_skills(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    return await SkillService.list(student_id, db)


@skill_router.post("", response_model=SkillResponse, status_code=201)
async def create_skill(
    req: SkillCreateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    return await SkillService.create(student_id, req, db)


@skill_router.patch("/{skill_id}", response_model=SkillResponse)
async def update_skill(
    skill_id: int,
    req: SkillUpdateRequest,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    return await SkillService.update(skill_id, student_id, req, db)


@skill_router.delete("/{skill_id}", response_model=MessageResponse)
async def delete_skill(
    skill_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    student_id = _get_student_id(current_user)
    await SkillService.delete(skill_id, student_id, db)
    return MessageResponse(message="Skill deleted")


# ── Career Readiness Router ────────────────────────────────────────────────────

readiness_router = APIRouter(prefix="/career-readiness", tags=["SPICS — Career"])


@readiness_router.get("/{student_id}", response_model=CareerReadinessResponse)
async def get_career_readiness(
    student_id: int,
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _check_root_flag()
    return await CareerReadinessService.get_readiness(student_id, db)


# ── Resume Router ──────────────────────────────────────────────────────────────

resume_router = APIRouter(prefix="/resume", tags=["SPICS — Resume"])


@resume_router.post("/upload", response_model=ResumeUploadResponse)
async def upload_resume(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload PDF or DOCX resume. AI analysis triggered asynchronously."""
    _check_root_flag()
    if not FLAGS.get("ENABLE_RESUME_ANALYZER", True):
        raise HTTPException(status_code=503, detail="Resume analyzer is not enabled.")

    content = await file.read()
    validate_upload_file(file.filename or "resume.pdf", file.content_type or "application/pdf", len(content))

    student_id = _get_student_id(current_user)

    # Ensure profile exists
    profile_repo = ProfileRepository(db)
    profile = await profile_repo.get_by_student_id(student_id)
    if not profile:
        # Auto-create minimal profile
        profile = await profile_repo.create(student_id, {})

    # Save file
    from datetime import datetime
    file_path = await save_resume_file(student_id, file.filename or "resume.pdf", content)

    # Update profile record
    await profile_repo.update(profile, {
        "resume_file_path":   file_path,
        "resume_uploaded_at": datetime.utcnow(),
    })

    # Queue AI resume analysis
    resume_text = extract_resume_text(content, file.filename or "resume.pdf")
    if resume_text and FLAGS.get("ENABLE_AI_CAPABILITY_ENGINE", True):
        from ..workers.ai_worker import run_resume_ai_analysis
        background_tasks.add_task(run_resume_ai_analysis, student_id, resume_text, db)

    return ResumeUploadResponse(
        student_id=student_id,
        file_path=file_path,
        file_size_kb=round(len(content) / 1024, 2),
        uploaded_at=datetime.utcnow(),
        analysis_queued=bool(resume_text),
        message="Resume uploaded successfully. AI analysis queued." if resume_text else "Resume uploaded. Text extraction unavailable — install pypdf/python-docx.",
    )


@resume_router.get("/download")
async def download_resume(
    current_user: core_models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Download the authenticated student's latest uploaded resume."""
    _check_root_flag()
    student_id = _get_student_id(current_user)

    profile = await ProfileRepository(db).get_by_student_id(student_id)
    if not profile or not profile.resume_file_path:
        raise HTTPException(status_code=404, detail="No resume uploaded yet.")

    uploads_root = UPLOAD_BASE if UPLOAD_BASE.is_absolute() else (Path.cwd() / UPLOAD_BASE)
    uploads_root = uploads_root.resolve()

    resume_path = Path(profile.resume_file_path)
    full_path = resume_path if resume_path.is_absolute() else (Path.cwd() / resume_path)
    full_path = full_path.resolve()

    # Prevent path traversal and ensure served files are under configured uploads root.
    if uploads_root not in full_path.parents and full_path != uploads_root:
        raise HTTPException(status_code=400, detail="Invalid resume file path.")

    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="Uploaded resume file not found on server.")

    suffix = full_path.suffix.lower()
    media_type = "application/octet-stream"
    if suffix == ".pdf":
        media_type = "application/pdf"
    elif suffix in {".doc", ".docx"}:
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

    return FileResponse(
        path=str(full_path),
        media_type=media_type,
        filename=full_path.name,
    )


# ── Helper Functions ───────────────────────────────────────────────────────────

def _check_root_flag():
    if not FLAGS.get("ENABLE_PROFESSIONAL_IDENTITY", True):
        raise HTTPException(status_code=503, detail="Professional Identity module is not enabled.")


def _get_student_id(user: core_models.User) -> int:
    """Extract student_id from the authenticated user's linked student record."""
    if hasattr(user, "student_id") and user.student_id:
        return user.student_id
    # Fallback: use user.id as student_id (adjust based on actual User model)
    return user.id


def _ensure_profile_exists(student_id: int):
    """Profile must exist before adding projects/skills."""
    pass  # Handled at service layer — auto-creates if needed
