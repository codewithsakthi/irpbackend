from __future__ import annotations

from typing import Iterable

from fastapi import HTTPException
from datetime import datetime
from sqlalchemy import select, update, func, text, tuple_, insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import base as models
from ..schemas import base as schemas
from ..utils.grading import compute_grade
from sqlalchemy.dialects.postgresql import insert as pg_insert

class AssessmentService:

    @staticmethod
    async def apply_staff_assessment_updates(
        *,
        db: AsyncSession,
        faculty_id: int,
        updates: Iterable[schemas.StaffStudentAssessmentUpdate],
        updated_by: int,
    ) -> None:
        """
        True Bulk Upsert for Marks.
        Sends a single PostgreSQL command for the entire batch.
        """
        update_list = list(updates)
        if not update_list:
            return

        all_subject_ids = {u.subject_id for u in update_list}
        all_student_ids = {u.student_id for u in update_list}

        # 1. Batch verify faculty assignment
        assigned_subs_res = await db.execute(
            select(models.FacultySubjectAssignment.subject_id)
            .where(models.FacultySubjectAssignment.faculty_id == faculty_id)
            .where(models.FacultySubjectAssignment.subject_id.in_(all_subject_ids))
        )
        assigned_subs = {r for r in assigned_subs_res.scalars().all()}
        for u in update_list:
            if u.subject_id not in assigned_subs:
                raise HTTPException(status_code=403, detail=f"Not assigned to subject ID {u.subject_id}")

        # 2. Batch fetch Subject details and current Internals
        subject_details_res = await db.execute(
            select(models.Subject.id, models.Subject.course_code)
            .where(models.Subject.id.in_(all_subject_ids))
        )
        course_codes = {r.id: r.course_code for r in subject_details_res.all()}

        internals_res = await db.execute(
            text(
                """
                SELECT student_id, subject_id, MAX(marks) as internal_marks
                FROM student_assessments
                WHERE student_id = ANY(:sids)
                  AND subject_id = ANY(:subids)
                  AND assessment_type IN ('CIT1', 'CIT2', 'CIT3')
                  AND is_final = true
                GROUP BY student_id, subject_id
                """
            ),
            {"sids": list(all_student_ids), "subids": list(all_subject_ids)},
        )
        internals_map = {(r.student_id, r.subject_id): r.internal_marks for r in internals_res.all()}

        # 3. Prepare data for Batch Operations
        now = datetime.now()
        
        # Fetch current states to determine if we update or insert
        # We query by the primary unique key components
        assessment_keys = []
        for item in update_list:
            assessment_keys.append((item.student_id, item.subject_id, item.semester, item.assessment_type, item.attempt or 1))
            
        existing_stmt = select(models.StudentAssessment).where(
            tuple_(
                models.StudentAssessment.student_id,
                models.StudentAssessment.subject_id,
                models.StudentAssessment.semester,
                models.StudentAssessment.assessment_type,
                models.StudentAssessment.attempt
            ).in_(assessment_keys)
        )
        existing_res = await db.execute(existing_stmt)
        existing_lookup = {
            (r.student_id, r.subject_id, r.semester, r.assessment_type, r.attempt): r 
            for r in existing_res.scalars().all()
        }

        to_insert = []
        to_update = []
        
        for item in update_list:
            marks = item.marks  # Preserve None/NULL from schema
            assessment_type = str(item.assessment_type or "").strip().upper()
            attempt = item.attempt or 1
            
            # Calculate metrics
            grade: str | None = None
            result_status: str | None = None

            # Only compute grade for semester exam if marks are present
            if assessment_type == "SEMESTER_EXAM" and marks is not None:
                ccode = course_codes.get(item.subject_id)
                internal_m = internals_map.get((item.student_id, item.subject_id))
                computed = compute_grade(
                    course_code=str(ccode) if ccode is not None else None,
                    cit1=internal_m,
                    semester_exam=marks,
                )
                grade = computed.grade
                result_status = computed.result_status

            key = (item.student_id, item.subject_id, item.semester, assessment_type, attempt)
            
            val_dict = {
                "student_id": item.student_id,
                "subject_id": item.subject_id,
                "semester": item.semester,
                "assessment_type": assessment_type,
                "marks": marks,
                "grade": grade,
                "result_status": result_status,
                "attempt": attempt,
                "remarks": item.remarks,
                "is_final": True,
                "updated_at": now,
                "updated_by": updated_by,
            }

            if key in existing_lookup:
                # Add to batch update list
                to_update.append(val_dict)
            else:
                # Add to batch insert list
                to_insert.append(val_dict)

        # 4. Execute Batch Operations
        # Using SQLAlchemy's high-performance bulk execute
        if to_insert:
            await db.execute(insert(models.StudentAssessment), to_insert)
            
        if to_update:
            for upd in to_update:
                stmt = (
                    update(models.StudentAssessment)
                    .where(models.StudentAssessment.student_id == upd["student_id"])
                    .where(models.StudentAssessment.subject_id == upd["subject_id"])
                    .where(models.StudentAssessment.semester == upd["semester"])
                    .where(models.StudentAssessment.assessment_type == upd["assessment_type"])
                    .where(models.StudentAssessment.attempt == upd["attempt"])
                    .values(**{k: v for k, v in upd.items() if k not in ["student_id", "subject_id", "semester", "assessment_type", "attempt"]})
                )
                await db.execute(stmt)
