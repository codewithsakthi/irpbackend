"""SPICS — Profile, Project, Skill, Certification, and Verification services"""
import logging
from datetime import datetime
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from ..models.models import (
    AIStatus, StudentProfessionalProfile, StudentProject,
    StudentSkill, VerificationStatus, SkillVerificationStatus,
)
from ..repositories.profile_repo import ProfileRepository
from ..repositories.data_repos import (
    ProjectRepository, CertificationRepository,
    SkillRepository, InsightRepository,
)
from ..schemas.schemas import (
    ProfileCreateRequest, ProjectCreateRequest, ProjectUpdateRequest,
    CertificationCreateRequest, CertificationUpdateRequest, SkillCreateRequest, SkillUpdateRequest,
    FacultyVerifyProjectRequest, FacultyVerifySkillRequest,
)
from ..utils.utils import compute_completion_score

logger = logging.getLogger(__name__)


class ProfileService:
    """Handles student professional profile creation and updates."""

    @staticmethod
    async def get_or_none(student_id: int, db: AsyncSession):
        repo = ProfileRepository(db)
        return await repo.get_by_student_id(student_id)

    @staticmethod
    async def upsert(student_id: int, req: ProfileCreateRequest, db: AsyncSession):
        repo = ProfileRepository(db)
        data = req.model_dump(exclude_unset=True)
        profile = await repo.upsert(student_id, data)
        # Recompute completion score
        await ProfileService._refresh_completion(profile, student_id, db)
        return profile

    @staticmethod
    async def _refresh_completion(
        profile: StudentProfessionalProfile,
        student_id: int,
        db: AsyncSession,
    ) -> None:
        proj_r = ProjectRepository(db)
        skill_r = SkillRepository(db)
        cert_r  = CertificationRepository(db)
        projects = await proj_r.list_by_student(student_id)
        skills   = await skill_r.list_by_student(student_id)
        certs    = await cert_r.list_by_student(student_id)
        score = compute_completion_score(profile, len(projects), len(skills), len(certs))
        profile.profile_completion_score = score
        await db.commit()


class ProjectService:
    @staticmethod
    async def list(student_id: int, db: AsyncSession):
        return await ProjectRepository(db).list_by_student(student_id)

    @staticmethod
    async def create(student_id: int, req: ProjectCreateRequest, db: AsyncSession):
        data = req.model_dump(exclude_unset=True)
        project = await ProjectRepository(db).create(student_id, data)
        await ProfileService._refresh_profile_completion(student_id, db)
        return project

    @staticmethod
    async def update(project_id: int, student_id: int, req: ProjectUpdateRequest, db: AsyncSession):
        repo = ProjectRepository(db)
        project = await repo.get_by_id(project_id)
        if not project:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Project not found")
        if project.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your project")
        data = req.model_dump(exclude_unset=True)
        return await repo.update(project, data)

    @staticmethod
    async def delete(project_id: int, student_id: int, db: AsyncSession):
        repo = ProjectRepository(db)
        project = await repo.get_by_id(project_id)
        if not project:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Project not found")
        if project.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your project")
        if project.is_github_imported:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=403,
                detail="Cannot delete GitHub-imported projects. Remove the GitHub connection instead."
            )
        await repo.delete(project)
        await ProjectService._refresh_profile_completion(student_id, db)

    @staticmethod
    async def _refresh_profile_completion(student_id: int, db: AsyncSession):
        profile = await ProfileRepository(db).get_by_student_id(student_id)
        if profile:
            await ProfileService._refresh_completion(profile, student_id, db)


class CertificationService:
    @staticmethod
    async def list(student_id: int, db: AsyncSession):
        return await CertificationRepository(db).list_by_student(student_id)

    @staticmethod
    async def create(student_id: int, req: CertificationCreateRequest, db: AsyncSession):
        data = req.model_dump(exclude_unset=True)
        cert = await CertificationRepository(db).create(student_id, data)
        await ProjectService._refresh_profile_completion(student_id, db)
        return cert

    @staticmethod
    async def update(cert_id: int, student_id: int, req: CertificationUpdateRequest, db: AsyncSession):
        repo = CertificationRepository(db)
        cert = await repo.get_by_id(cert_id)
        if not cert:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Certification not found")
        if cert.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your certification")
        updated = await repo.update(cert, req.model_dump(exclude_unset=True))
        await ProjectService._refresh_profile_completion(student_id, db)
        return updated

    @staticmethod
    async def delete(cert_id: int, student_id: int, db: AsyncSession):
        repo = CertificationRepository(db)
        cert = await repo.get_by_id(cert_id)
        if not cert:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Certification not found")
        if cert.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your certification")
        await repo.delete(cert)


class SkillService:
    @staticmethod
    async def list(student_id: int, db: AsyncSession):
        return await SkillRepository(db).list_by_student(student_id)

    @staticmethod
    async def create(student_id: int, req: SkillCreateRequest, db: AsyncSession):
        data = req.model_dump(exclude_unset=True)
        skill = await SkillRepository(db).create(student_id, data)
        await ProjectService._refresh_profile_completion(student_id, db)
        return skill

    @staticmethod
    async def update(skill_id: int, student_id: int, req: SkillUpdateRequest, db: AsyncSession):
        repo = SkillRepository(db)
        skill = await repo.get_by_id(skill_id)
        if not skill:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Skill not found")
        if skill.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your skill")
        return await repo.update(skill, req.model_dump(exclude_unset=True))

    @staticmethod
    async def delete(skill_id: int, student_id: int, db: AsyncSession):
        repo = SkillRepository(db)
        skill = await repo.get_by_id(skill_id)
        if not skill:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Skill not found")
        if skill.student_id != student_id:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Not your skill")
        await repo.delete(skill)


class VerificationService:
    """Faculty-side verification of projects and skills."""

    @staticmethod
    async def get_pending_verifications(db: AsyncSession) -> dict:
        proj_repo  = ProjectRepository(db)
        skill_repo = SkillRepository(db)
        pending_projects = await proj_repo.list_pending_verifications()
        pending_skills   = await skill_repo.list_pending_verifications()

        # Enrich with student names (read-only query on existing students table)
        items = []
        for proj in pending_projects:
            student_name, roll_no = await _get_student_info(proj.student_id, db)
            items.append({
                "entity_type": "project",
                "entity_id":   proj.project_id,
                "student_id":  proj.student_id,
                "student_name": student_name,
                "roll_no":     roll_no,
                "title":       proj.title,
                "status":      proj.verification_status.value if hasattr(proj.verification_status, "value") else proj.verification_status,
                "created_at":  proj.created_at,
            })
        for skill in pending_skills:
            student_name, roll_no = await _get_student_info(skill.student_id, db)
            items.append({
                "entity_type": "skill",
                "entity_id":   skill.skill_id,
                "student_id":  skill.student_id,
                "student_name": student_name,
                "roll_no":     roll_no,
                "title":       skill.skill_name,
                "status":      skill.verification_status.value if hasattr(skill.verification_status, "value") else skill.verification_status,
                "created_at":  skill.created_at,
            })
        return {"items": items, "total": len(items)}

    @staticmethod
    async def verify_project(
        project_id: int,
        faculty_id: int,
        req: FacultyVerifyProjectRequest,
        db: AsyncSession,
    ):
        repo = ProjectRepository(db)
        project = await repo.get_by_id(project_id)
        if not project:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Project not found")
        return await repo.update(project, {
            "verification_status": req.verification_status,
            "faculty_remarks":    req.faculty_remarks,
            "verified_by_faculty": faculty_id,
            "verified_at":        datetime.utcnow(),
        })

    @staticmethod
    async def verify_skill(
        skill_id: int,
        faculty_id: int,
        req: FacultyVerifySkillRequest,
        db: AsyncSession,
    ):
        repo = SkillRepository(db)
        skill = await repo.get_by_id(skill_id)
        if not skill:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Skill not found")
        return await repo.update(skill, {
            "faculty_rating":     req.faculty_rating,
            "verification_status": req.verification_status,
            "faculty_rater_id":   faculty_id,
        })


async def _get_student_info(student_id: int, db: AsyncSession):
    """Read-only lookup of student name + roll_no from existing students table."""
    try:
        row = (await db.execute(
            text("SELECT name, roll_no FROM students WHERE id = :sid"),
            {"sid": student_id},
        )).first()
        if row:
            return row[0], row[1]
    except Exception:
        pass
    return None, None


class CareerReadinessService:
    @staticmethod
    async def get_readiness(student_id: int, db: AsyncSession) -> dict:
        profile    = await ProfileRepository(db).get_by_student_id(student_id)
        projects   = await ProjectRepository(db).list_by_student(student_id)
        certs      = await CertificationRepository(db).list_by_student(student_id)
        skills     = await SkillRepository(db).list_by_student(student_id)
        insights   = await InsightRepository(db).get_by_student_id(student_id)

        verified_projects = [p for p in projects if str(p.verification_status) in ("verified", VerificationStatus.VERIFIED)]
        verified_skills   = [s for s in skills if str(s.verification_status) in ("faculty_verified", SkillVerificationStatus.FACULTY_VERIFIED)]
        top_skills = [s.skill_name for s in skills[:5]]

        completion = float(profile.profile_completion_score or 0) if profile else 0.0
        ai_score   = float(insights.career_readiness_score or 0) if insights else None

        # Determine readiness band
        combined = (completion * 0.4 + (ai_score or completion) * 0.6)
        if combined >= 80:
            band = "Ready"
        elif combined >= 60:
            band = "Near Ready"
        elif combined >= 40:
            band = "Building"
        else:
            band = "Early Stage"

        # Next steps
        steps = []
        if not profile or not profile.github_username:
            steps.append("Add your GitHub username to showcase your coding activity.")
        if len(projects) < 2:
            steps.append("Add at least 2 projects with GitHub links for better visibility.")
        if len(skills) < 5:
            steps.append("Add your top 5 technical skills to complete your skill matrix.")
        if not (profile and profile.resume_file_path):
            steps.append("Upload your resume for AI-powered gap analysis.")
        if len(certs) == 0:
            steps.append("Add NPTEL/Coursera/AWS certifications to strengthen your profile.")
        if not steps:
            steps.append("Your profile is strong! Focus on getting projects verified by faculty.")

        return {
            "student_id":               student_id,
            "profile_completion_score": completion,
            "total_projects":           len(projects),
            "verified_projects":        len(verified_projects),
            "total_certifications":     len(certs),
            "total_skills":             len(skills),
            "verified_skills":          len(verified_skills),
            "has_github":               bool(profile and profile.github_username),
            "has_resume":               bool(profile and profile.resume_file_path),
            "ai_career_readiness_score": ai_score,
            "readiness_band":           band,
            "top_skills":               top_skills,
            "recommended_next_steps":   steps[:3],
        }
