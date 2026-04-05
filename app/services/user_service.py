from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import joinedload

from .. import models, schemas
from .student_service import StudentService
from .admin_service import AdminService
from ..core.constants import CURRICULUM_CREDITS

class UserService:
    @staticmethod
    async def get_staff_record(user: models.User, db: AsyncSession):
        result = await db.execute(
            select(models.Staff).filter(models.Staff.id == user.id)
        )
        return result.scalars().first()

    @classmethod
    async def build_current_user_response(cls, user: models.User, db: AsyncSession) -> schemas.CurrentUser:
        student_joined = await StudentService.get_student_profile_joined(user.id, db)
        student = student_joined[0] if student_joined else None
        program = student_joined[2] if student_joined else None
        contact = student_joined[3] if student_joined else None
        staff = None if student else await cls.get_staff_record(user, db)
        
        rank = None
        if student:
            from .ranking_service import RankingService
            rank_info = await RankingService.get_student_rank_by_cgpa(
                db=db,
                roll_no=student.roll_no,
                curriculum_credits=CURRICULUM_CREDITS
            )
            if rank_info:
                rank = rank_info['rank']

        return schemas.CurrentUser(
            id=user.id,
            username=user.username,
            role_id=user.role_id,
            is_initial_password=user.is_initial_password,
            created_at=user.created_at,
            role=user.role.name if user.role else "student",
            name=student.name if student else (staff.name if staff else None),
            email=(contact.email if (student and contact and contact.email) else student.email) if student else (staff.email if staff else None),
            roll_no=student.roll_no if student else None,
            reg_no=student.reg_no if student else None,
            batch=student.batch if student else None,
            current_semester=student.current_semester if student else None,
            program_name=program.name if student and program else None,
            program_code=program.code if student and program else None,
            rank=rank,
        )
    @classmethod
    async def update_user_profile(cls, user: models.User, update_data: schemas.ProfileUpdate, db: AsyncSession) -> schemas.CurrentUser:
        # 1. Update basic user data (none currently in models.User, but we could add if needed)
        
        # 2. Update Student Profile if applicable
        result = await db.execute(select(models.Student).filter(models.Student.id == user.id))
        student = result.scalars().first()
        if student:
            if update_data.name:
                student.name = update_data.name
            if update_data.email:
                student.email = update_data.email
            if update_data.batch:
                student.batch = update_data.batch
        else:
            # 3. Update Staff Profile if applicable
            result = await db.execute(select(models.Staff).filter(models.Staff.id == user.id))
            staff = result.scalars().first()
            if staff:
                if update_data.name:
                    staff.name = update_data.name
                if update_data.email:
                    staff.email = update_data.email
                    
        await db.commit()
        await db.refresh(user)
        return await cls.build_current_user_response(user, db)
