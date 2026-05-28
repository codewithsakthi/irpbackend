"""SPICS — Repository layer for StudentProject, StudentCertification, StudentSkill, AIProfessionalInsight"""
from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update as sa_update
from ..models.models import (
    AIProfessionalInsight, AIStatus, StudentCertification,
    StudentProject, StudentSkill, VerificationStatus,
)


class ProjectRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_by_student(self, student_id: int) -> List[StudentProject]:
        r = await self.db.execute(
            select(StudentProject)
            .where(StudentProject.student_id == student_id)
            .order_by(StudentProject.created_at.desc())
        )
        return list(r.scalars().all())

    async def get_by_id(self, project_id: int) -> Optional[StudentProject]:
        r = await self.db.execute(
            select(StudentProject).where(StudentProject.project_id == project_id)
        )
        return r.scalars().first()

    async def create(self, student_id: int, data: dict) -> StudentProject:
        proj = StudentProject(student_id=student_id, **data)
        self.db.add(proj)
        await self.db.commit()
        await self.db.refresh(proj)
        return proj

    async def update(self, project: StudentProject, data: dict) -> StudentProject:
        for k, v in data.items():
            if hasattr(project, k):
                setattr(project, k, v)
        await self.db.commit()
        await self.db.refresh(project)
        return project

    async def delete(self, project: StudentProject) -> None:
        await self.db.delete(project)
        await self.db.commit()

    async def list_pending_verifications(self) -> List[StudentProject]:
        r = await self.db.execute(
            select(StudentProject).where(
                StudentProject.verification_status == VerificationStatus.PENDING
            ).order_by(StudentProject.created_at.asc())
        )
        return list(r.scalars().all())


class CertificationRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_by_student(self, student_id: int) -> List[StudentCertification]:
        r = await self.db.execute(
            select(StudentCertification)
            .where(StudentCertification.student_id == student_id)
            .order_by(StudentCertification.issue_date.desc().nullslast())
        )
        return list(r.scalars().all())

    async def get_by_id(self, cert_id: int) -> Optional[StudentCertification]:
        r = await self.db.execute(
            select(StudentCertification).where(StudentCertification.certification_id == cert_id)
        )
        return r.scalars().first()

    async def create(self, student_id: int, data: dict) -> StudentCertification:
        cert = StudentCertification(student_id=student_id, **data)
        self.db.add(cert)
        await self.db.commit()
        await self.db.refresh(cert)
        return cert

    async def update(self, cert: StudentCertification, data: dict) -> StudentCertification:
        for k, v in data.items():
            if hasattr(cert, k):
                setattr(cert, k, v)
        await self.db.commit()
        await self.db.refresh(cert)
        return cert

    async def delete(self, cert: StudentCertification) -> None:
        await self.db.delete(cert)
        await self.db.commit()


class SkillRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_by_student(self, student_id: int) -> List[StudentSkill]:
        r = await self.db.execute(
            select(StudentSkill)
            .where(StudentSkill.student_id == student_id)
            .order_by(StudentSkill.category, StudentSkill.skill_name)
        )
        return list(r.scalars().all())

    async def get_by_id(self, skill_id: int) -> Optional[StudentSkill]:
        r = await self.db.execute(
            select(StudentSkill).where(StudentSkill.skill_id == skill_id)
        )
        return r.scalars().first()

    async def create(self, student_id: int, data: dict) -> StudentSkill:
        skill = StudentSkill(student_id=student_id, **data)
        self.db.add(skill)
        await self.db.commit()
        await self.db.refresh(skill)
        return skill

    async def update(self, skill: StudentSkill, data: dict) -> StudentSkill:
        for k, v in data.items():
            if hasattr(skill, k):
                setattr(skill, k, v)
        await self.db.commit()
        await self.db.refresh(skill)
        return skill

    async def delete(self, skill: StudentSkill) -> None:
        await self.db.delete(skill)
        await self.db.commit()

    async def list_pending_verifications(self) -> List[StudentSkill]:
        from ..models.models import SkillVerificationStatus
        r = await self.db.execute(
            select(StudentSkill).where(
                StudentSkill.verification_status == SkillVerificationStatus.SELF_REPORTED
            ).order_by(StudentSkill.created_at.asc())
        )
        return list(r.scalars().all())


class InsightRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_student_id(self, student_id: int) -> Optional[AIProfessionalInsight]:
        r = await self.db.execute(
            select(AIProfessionalInsight).where(
                AIProfessionalInsight.student_id == student_id
            )
        )
        return r.scalars().first()

    async def upsert_status(self, student_id: int, status: AIStatus) -> AIProfessionalInsight:
        insight = await self.get_by_student_id(student_id)
        if insight:
            insight.ai_status = status
            await self.db.commit()
            await self.db.refresh(insight)
            return insight
        insight = AIProfessionalInsight(student_id=student_id, ai_status=status)
        self.db.add(insight)
        await self.db.commit()
        await self.db.refresh(insight)
        return insight

    async def save_result(self, student_id: int, data: dict) -> AIProfessionalInsight:
        insight = await self.get_by_student_id(student_id)
        data["generated_at"] = datetime.utcnow()
        if insight:
            for k, v in data.items():
                if hasattr(insight, k):
                    setattr(insight, k, v)
        else:
            insight = AIProfessionalInsight(student_id=student_id, **data)
            self.db.add(insight)
        await self.db.commit()
        await self.db.refresh(insight)
        return insight
