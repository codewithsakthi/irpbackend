from __future__ import annotations

from typing import Iterable

from fastapi import HTTPException
from sqlalchemy import select, update, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import base as models
from ..schemas import base as schemas
from ..utils.grading import compute_grade


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
        Apply mark/grade updates posted by staff.

        - Always sets `is_final=true` when staff posts.
        - For `SEMESTER_EXAM`: derives `grade` and `result_status` using
          internal = MAX(CIT1/2/3 marks) and total = internal + exam marks.
          Semester-exam marks are stored but should never be exposed by student APIs.
        """
        for item in updates:
            assessment_type = str(item.assessment_type or "").strip().upper()

            # Verify faculty assignment for this subject
            assignment = (
                await db.execute(
                    select(models.FacultySubjectAssignment.id)
                    .where(models.FacultySubjectAssignment.faculty_id == faculty_id)
                    .where(models.FacultySubjectAssignment.subject_id == item.subject_id)
                    .limit(1)
                )
            ).scalar()
            if not assignment:
                raise HTTPException(status_code=403, detail=f"Not assigned to subject ID {item.subject_id}")

            # Compute grade/result for semester exam submissions
            grade: str | None = None
            result_status: str | None = None

            if assessment_type == "SEMESTER_EXAM":
                course_code = (
                    await db.execute(
                        select(models.Subject.course_code).where(models.Subject.id == item.subject_id).limit(1)
                    )
                ).scalar()

                internal_marks = (
                    await db.execute(
                        text(
                            """
                            SELECT MAX(marks) AS internal_marks
                            FROM student_assessments
                            WHERE student_id = :sid
                              AND subject_id = :subid
                              AND assessment_type IN ('CIT1', 'CIT2', 'CIT3')
                              AND is_final = true
                            """
                        ),
                        {"sid": item.student_id, "subid": item.subject_id},
                    )
                ).scalar()

                computed = compute_grade(
                    course_code=str(course_code) if course_code is not None else None,
                    cit1=internal_marks,
                    semester_exam=item.marks,
                )
                grade = computed.grade
                result_status = computed.result_status

            # Try to find existing row for this assessment (latest attempt semantics)
            existing = (
                await db.execute(
                    select(models.StudentAssessment)
                    .where(models.StudentAssessment.student_id == item.student_id)
                    .where(models.StudentAssessment.subject_id == item.subject_id)
                    .where(models.StudentAssessment.semester == item.semester)
                    .where(models.StudentAssessment.assessment_type == assessment_type)
                    .order_by(models.StudentAssessment.attempt.desc())
                    .limit(1)
                )
            ).scalars().first()

            values = dict(
                marks=item.marks,
                remarks=item.remarks,
                grade=grade,
                result_status=result_status,
                is_final=True,
                updated_at=func.now(),
                updated_by=updated_by,
            )

            if existing:
                await db.execute(
                    update(models.StudentAssessment)
                    .where(models.StudentAssessment.id == existing.id)
                    .values(**values)
                )
            else:
                db.add(
                    models.StudentAssessment(
                        student_id=item.student_id,
                        subject_id=item.subject_id,
                        semester=item.semester,
                        assessment_type=assessment_type,
                        marks=item.marks,
                        remarks=item.remarks,
                        grade=grade,
                        result_status=result_status,
                        is_final=True,
                        attempt=1,
                        updated_by=updated_by,
                    )
                )

