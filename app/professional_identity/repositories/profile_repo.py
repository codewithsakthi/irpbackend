"""SPICS — Repository layer for StudentProfessionalProfile"""
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from ..models.models import StudentProfessionalProfile


class ProfileRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_student_id(self, student_id: int) -> Optional[StudentProfessionalProfile]:
        result = await self.db.execute(
            select(StudentProfessionalProfile).where(
                StudentProfessionalProfile.student_id == student_id
            )
        )
        return result.scalars().first()

    async def create(self, student_id: int, data: dict) -> StudentProfessionalProfile:
        profile = StudentProfessionalProfile(student_id=student_id, **data)
        self.db.add(profile)
        await self.db.commit()
        await self.db.refresh(profile)
        return profile

    async def update(self, profile: StudentProfessionalProfile, data: dict) -> StudentProfessionalProfile:
        for key, value in data.items():
            if hasattr(profile, key):
                setattr(profile, key, value)
        await self.db.commit()
        await self.db.refresh(profile)
        return profile

    async def upsert(self, student_id: int, data: dict) -> StudentProfessionalProfile:
        profile = await self.get_by_student_id(student_id)
        if profile:
            return await self.update(profile, data)
        return await self.create(student_id, data)
