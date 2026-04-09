from typing import Optional, List, Dict, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func, case
from sqlalchemy.orm import joinedload
from fastapi import HTTPException
from datetime import datetime, timedelta

from .. import models, schemas
from ..core.constants import GRADE_POINTS, CURRICULUM_CREDITS
from ..utils.grading import compute_grade, grade_point_from_grade, compute_hybrid_performance_metrics, SubjectPerformanceData

class StudentService:
    @staticmethod
    async def get_student_record(user: models.User, db: AsyncSession):
        result = await db.execute(
            select(models.Student).options(joinedload(models.Student.program)).filter(models.Student.id == user.id)
        )
        return result.scalars().first()

    @staticmethod
    async def get_student_profile_joined(student_id: int, db: AsyncSession):
        """
        Normalized Student Profile fetch:
        students + contact_info + family_details + programs + users
        """
        res = await db.execute(
            select(
                models.Student,
                models.User,
                models.Program,
                models.ContactInfo,
                models.FamilyDetail,
            )
            .join(models.User, models.User.id == models.Student.id)
            .outerjoin(models.Program, models.Program.id == models.Student.program_id)
            .outerjoin(models.ContactInfo, models.ContactInfo.student_id == models.Student.id)
            .outerjoin(models.FamilyDetail, models.FamilyDetail.student_id == models.Student.id)
            .filter(models.Student.id == student_id)
        )
        row = res.first()
        if not row:
            return None
        return row  # (Student, User, Program|None, ContactInfo|None, FamilyDetail|None)

    @staticmethod
    async def get_accessible_student(roll_no: str, current_user_id: int, role_name: str, db: AsyncSession):
        stmt = (
            select(models.Student)
            .options(
                joinedload(models.Student.program),
                joinedload(models.Student.assessments).joinedload(models.StudentAssessment.subject),
                joinedload(models.Student.user),
            )
        )

        if role_name == 'student':
            stmt = stmt.filter(models.Student.id == current_user_id)
            result = await db.execute(stmt)
            student = result.scalars().first()
            if not student or student.roll_no != roll_no:
                raise HTTPException(status_code=403, detail='Students can only access their own records')
            return student

        stmt = stmt.filter(models.Student.roll_no == roll_no)
        result = await db.execute(stmt)
        student = result.scalars().first()
        if not student:
            raise HTTPException(status_code=404, detail='Student not found')
        return student

    @staticmethod
    async def get_report_card_marks(student_id: int, db: AsyncSession) -> list[schemas.ReportCardMark]:
        """
        Pivot finalized assessment rows into per-subject report-card marks.

        Source of truth: v_final_assessments (is_final = true rows).
        """
        q = text(
            """
            WITH student_ctx AS (
                SELECT
                    program_id,
                    current_semester,
                    section,
                    batch,
                    GREATEST(
                        COALESCE(current_semester, 0),
                        COALESCE((
                            SELECT MAX(sa.semester)
                            FROM student_assessments sa
                            WHERE sa.student_id = :sid
                        ), 0)
                    ) AS max_semester
                FROM students
                WHERE id = :sid
            ),
            subject_catalog AS (
                SELECT
                    s.id AS subject_id,
                    s.course_code,
                    s.name AS subject_name,
                    COALESCE(s.credits, 0) AS credits,
                    s.semester,
                    COALESCE(s.pass_threshold, 50.0) AS pass_threshold,
                    s.target_average,
                    COALESCE(s.percentile_excellent, 85.0) AS percentile_excellent,
                    COALESCE(s.percentile_good, 60.0) AS percentile_good,
                    COALESCE(s.percentile_average, 30.0) AS percentile_average
                FROM subjects s
                CROSS JOIN student_ctx st
                WHERE (s.program_id = st.program_id OR s.program_id IS NULL)
                  AND s.is_active = true  -- Only include active subjects
                  AND (
                      s.course_code NOT LIKE '24MCBC%'
                      OR EXISTS (
                          SELECT 1
                          FROM student_assessments sa
                          WHERE sa.student_id = :sid AND sa.subject_id = s.id
                      )
                  )
                  AND s.semester IS NOT NULL
                  AND s.semester <= st.max_semester
                  -- Filter subjects to only those assigned to the student's section
                  AND (
                      st.section IS NULL 
                      OR EXISTS (
                          SELECT 1 
                          FROM faculty_subject_assignments fsa 
                          WHERE fsa.subject_id = s.id 
                            AND fsa.section = st.section
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM student_assessments sa
                          WHERE sa.student_id = :sid AND sa.subject_id = s.id
                      )
                  )
            ),
            marks_pivot AS (
                SELECT
                    a.subject_id,
                    a.semester,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT1') AS cit1,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT2') AS cit2,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT3') AS cit3,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS sem_exam_marks,
                    MAX(a.grade) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM' AND a.is_final = true) AS sem_grade,
                    MAX(a.result_status) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM' AND a.is_final = true) AS sem_result_status,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'LAB') AS lab,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'PROJECT') AS project,
                    MAX(a.attempt) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS attempt,
                    MAX(a.remarks) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS remarks
                FROM student_assessments a
                WHERE a.student_id = :sid
                GROUP BY a.subject_id, a.semester
            ),
            -- Enhanced CTE: Subject-wise performance metrics for hybrid evaluation
            subject_performance AS (
                SELECT
                    sa.subject_id,
                    sa.semester,
                    -- Collect all total marks for percentile calculation
                    array_agg(
                        CASE 
                            WHEN sa.assessment_type = 'CIT1' THEN sa.marks 
                            WHEN sa.assessment_type = 'CIT2' THEN sa.marks
                            WHEN sa.assessment_type = 'CIT3' THEN sa.marks
                            WHEN sa.assessment_type = 'SEMESTER_EXAM' THEN sa.marks
                            WHEN sa.assessment_type = 'LAB' THEN sa.marks
                            WHEN sa.assessment_type = 'PROJECT' THEN sa.marks
                        END
                    ) FILTER (WHERE sa.marks IS NOT NULL) AS all_assessment_marks,
                    -- Calculate subject average for normalization
                    AVG(sa.marks) FILTER (WHERE sa.marks IS NOT NULL) AS subject_average_marks
                FROM student_assessments sa
                INNER JOIN student_ctx st ON true
                WHERE sa.is_final = true
                  AND EXISTS (
                      SELECT 1 FROM subjects s 
                      WHERE s.id = sa.subject_id 
                        AND (s.program_id = st.program_id OR s.program_id IS NULL)
                        AND s.is_active = true
                  )
                GROUP BY sa.subject_id, sa.semester
            ),
            -- Enhanced marks with computed total marks for each student
            marks_with_totals AS (
                SELECT
                    mp.*,
                    -- Compute total marks using existing grading logic
                    CASE 
                        WHEN mp.sem_exam_marks IS NOT NULL THEN 
                            COALESCE(
                                (CASE 
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit2 IS NOT NULL AND mp.cit3 IS NOT NULL THEN
                                        (GREATEST(mp.cit1, mp.cit2) + GREATEST(mp.cit2, mp.cit3) + GREATEST(mp.cit1, mp.cit3) - LEAST(mp.cit1, mp.cit2, mp.cit3)) / 2.0
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit2 IS NOT NULL THEN (mp.cit1 + mp.cit2) / 2.0
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit3 IS NOT NULL THEN (mp.cit1 + mp.cit3) / 2.0
                                    WHEN mp.cit2 IS NOT NULL AND mp.cit3 IS NOT NULL THEN (mp.cit2 + mp.cit3) / 2.0
                                    WHEN mp.cit1 IS NOT NULL THEN mp.cit1
                                    WHEN mp.cit2 IS NOT NULL THEN mp.cit2
                                    WHEN mp.cit3 IS NOT NULL THEN mp.cit3
                                    ELSE 0
                                END), 0
                            ) + mp.sem_exam_marks
                        WHEN mp.lab IS NOT NULL THEN mp.lab
                        WHEN mp.project IS NOT NULL THEN mp.project
                        ELSE NULL
                    END AS computed_total_marks
                FROM marks_pivot mp
            ),
            -- Subject cohort data for percentile calculations
            subject_cohort_totals AS (
                SELECT 
                    sa.subject_id,
                    sa.semester,
                    sa.student_id,
                    -- Compute total marks for all students in the subject
                    CASE 
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'SEMESTER_EXAM') IS NOT NULL THEN
                            COALESCE(
                                (SELECT 
                                    CASE 
                                        WHEN c1.marks IS NOT NULL AND c2.marks IS NOT NULL AND c3.marks IS NOT NULL THEN
                                            (GREATEST(c1.marks, c2.marks) + GREATEST(c2.marks, c3.marks) + GREATEST(c1.marks, c3.marks) - LEAST(c1.marks, c2.marks, c3.marks)) / 2.0
                                        WHEN c1.marks IS NOT NULL AND c2.marks IS NOT NULL THEN (c1.marks + c2.marks) / 2.0
                                        WHEN c1.marks IS NOT NULL AND c3.marks IS NOT NULL THEN (c1.marks + c3.marks) / 2.0  
                                        WHEN c2.marks IS NOT NULL AND c3.marks IS NOT NULL THEN (c2.marks + c3.marks) / 2.0
                                        WHEN c1.marks IS NOT NULL THEN c1.marks
                                        WHEN c2.marks IS NOT NULL THEN c2.marks
                                        WHEN c3.marks IS NOT NULL THEN c3.marks
                                        ELSE 0
                                    END
                                 FROM (
                                     SELECT 
                                         MAX(marks) FILTER (WHERE assessment_type = 'CIT1') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c1
                                 CROSS JOIN (
                                     SELECT 
                                         MAX(marks) FILTER (WHERE assessment_type = 'CIT2') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c2
                                 CROSS JOIN (
                                     SELECT 
                                         MAX(marks) FILTER (WHERE assessment_type = 'CIT3') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c3
                                ), 0
                            ) + MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'SEMESTER_EXAM')
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'LAB') IS NOT NULL THEN 
                            MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'LAB')
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'PROJECT') IS NOT NULL THEN 
                            MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'PROJECT')
                        ELSE NULL
                    END AS student_total_marks
                FROM student_assessments sa
                INNER JOIN student_ctx st ON true
                WHERE sa.is_final = true
                  AND EXISTS (
                      SELECT 1 FROM subjects s 
                      WHERE s.id = sa.subject_id 
                        AND (s.program_id = st.program_id OR s.program_id IS NULL)
                        AND s.is_active = true
                  )
                GROUP BY sa.subject_id, sa.semester, sa.student_id
            ),
            -- Percentile calculations per subject
            subject_percentiles AS (
                SELECT
                    sct.subject_id,
                    sct.semester,
                    sct.student_id,
                    sct.student_total_marks,
                    -- Calculate percentile rank within subject
                    PERCENT_RANK() OVER (
                        PARTITION BY sct.subject_id, sct.semester 
                        ORDER BY sct.student_total_marks ASC
                    ) * 100 AS percentile_rank,
                    -- Calculate subject average for normalization
                    AVG(sct.student_total_marks) OVER (
                        PARTITION BY sct.subject_id, sct.semester
                    ) AS subject_average
                FROM subject_cohort_totals sct
                WHERE sct.student_total_marks IS NOT NULL
            )
            SELECT
                sc.semester AS semester,
                sc.subject_id AS subject_id,
                sc.course_code AS course_code,
                sc.subject_name AS subject_name,
                sc.credits AS credits,
                mwt.cit1,
                mwt.cit2,
                mwt.cit3,
                mwt.sem_exam_marks,
                mwt.sem_grade,
                mwt.sem_result_status,
                mwt.lab,
                mwt.project,
                mwt.attempt,
                mwt.remarks,
                mwt.computed_total_marks,
                -- Hybrid performance metrics
                COALESCE(sp.percentile_rank, 0) AS percentile_rank,
                CASE 
                    WHEN mwt.computed_total_marks IS NOT NULL AND sp.subject_average > 0 THEN
                        ROUND(mwt.computed_total_marks / sp.subject_average, 3)
                    ELSE NULL
                END AS normalized_score,
                sp.subject_average,
                -- Performance label based on hybrid classification using subject's custom thresholds
                CASE 
                    WHEN mwt.computed_total_marks IS NULL OR sp.percentile_rank IS NULL THEN NULL
                    WHEN sp.percentile_rank < sc.percentile_average OR mwt.computed_total_marks < sc.pass_threshold THEN 'At Risk'
                    WHEN sp.percentile_rank <= sc.percentile_good THEN 'Average'
                    WHEN sp.percentile_rank <= sc.percentile_excellent THEN 'Good'
                    ELSE 'Excellent'
                END AS performance_label
            FROM subject_catalog sc
            LEFT JOIN marks_with_totals mwt
              ON mwt.subject_id = sc.subject_id
             AND mwt.semester = sc.semester
            LEFT JOIN subject_percentiles sp
              ON sp.subject_id = sc.subject_id
             AND sp.semester = sc.semester
             AND sp.student_id = :sid
            ORDER BY sc.semester ASC, sc.course_code ASC
            """
        )

        rows = (await db.execute(q, {"sid": student_id})).mappings().all()

        marks: list[schemas.ReportCardMark] = []
        for row in rows:
            course_code = row["course_code"]
            is_audit = str(course_code or "").upper().startswith("24AC")
            has_exam_component = (
                row.get("sem_grade") is not None
                or row.get("sem_exam_marks") is not None
                or row.get("lab") is not None
                or row.get("project") is not None
            )

            sem_grade = row.get("sem_grade")
            sem_result_status = row.get("sem_result_status")
            sem_exam_marks = row.get("sem_exam_marks")

            computed = compute_grade(
                course_code=course_code,
                cit1=row["cit1"],
                cit2=row["cit2"],
                cit3=row["cit3"],
                semester_exam=sem_exam_marks,
                lab=row["lab"],
                project=row["project"],
            )
            subject = schemas.Subject(
                id=int(row["subject_id"]),
                course_code=str(course_code),
                name=str(row["subject_name"]),
                credits=float(
                    CURRICULUM_CREDITS.get(str(course_code), row["credits"] or 0) or 0
                ),
                semester=int(row["semester"]),
            )
            marks.append(
                schemas.ReportCardMark(
                    semester=int(row["semester"]),
                    subject=subject,
                    cit1=float(row["cit1"]) if row["cit1"] is not None else None,
                    cit2=float(row["cit2"]) if row["cit2"] is not None else None,
                    cit3=float(row["cit3"]) if row["cit3"] is not None else None,
                    # Policy: never expose SEMESTER_EXAM marks to student-facing clients.
                    sem_exam=None,
                    lab=float(row["lab"]) if row["lab"] is not None else None,
                    project=float(row["project"]) if row["project"] is not None else None,
                    internal_marks=computed.internal,
                    # Semester Results should not be inferred from internals-only.
                    # If there is no exam/lab/project component yet, keep final fields blank.
                    total_marks=(
                        computed.total
                        if (
                            is_audit
                            or (sem_grade is not None and sem_exam_marks is not None)
                            or (sem_grade is None and has_exam_component)
                        )
                        else None
                    ),
                    grade=(str(sem_grade) if sem_grade is not None else computed.grade) if (is_audit or has_exam_component) else None,
                    result_status=(str(sem_result_status) if sem_result_status is not None else computed.result_status) if (is_audit or has_exam_component) else None,
                    attempt=int(row["attempt"]) if row["attempt"] is not None else None,
                    remarks=str(row["remarks"]) if row["remarks"] is not None else None,
                    # Enhanced hybrid performance metrics
                    percentile=float(row["percentile_rank"]) if row["percentile_rank"] is not None else None,
                    normalized_score=float(row["normalized_score"]) if row["normalized_score"] is not None else None,
                    performance_label=str(row["performance_label"]) if row["performance_label"] is not None else None,
                )
            )
        return marks

    @classmethod
    async def build_full_student_record(cls, roll_no: str, *, student_id: int, db: AsyncSession) -> schemas.FullStudentRecord:
        contact_info = (await db.execute(select(models.ContactInfo).filter(models.ContactInfo.student_id == student_id))).scalars().first()
        family_details = (await db.execute(select(models.FamilyDetail).filter(models.FamilyDetail.student_id == student_id))).scalars().first()
        previous_academics = (
            (await db.execute(select(models.PreviousAcademic).filter(models.PreviousAcademic.student_id == student_id).order_by(models.PreviousAcademic.id.asc())))
            .scalars()
            .all()
        )
        extra_curricular = (
            (await db.execute(select(models.ExtraCurricular).filter(models.ExtraCurricular.student_id == student_id).order_by(models.ExtraCurricular.activity_id.asc())))
            .scalars()
            .all()
        )

        # Counselor diary joined with counselor (staff) name
        diary_rows = (
            await db.execute(
                select(models.CounselorDiary, models.Staff.name.label("counselor_name"))
                .outerjoin(models.Staff, models.Staff.id == models.CounselorDiary.counselor_id)
                .filter(models.CounselorDiary.student_id == student_id)
                .order_by(models.CounselorDiary.meeting_date.desc().nullslast(), models.CounselorDiary.meeting_id.desc())
            )
        ).all()
        counselor_diary: list[schemas.CounselorDiaryRecord] = []
        for diary, counselor_name in diary_rows:
            counselor_diary.append(
                schemas.CounselorDiaryRecord(
                    meeting_id=diary.meeting_id,
                    semester=diary.semester,
                    meeting_date=diary.meeting_date,
                    remark_category=diary.remark_category,
                    remarks=diary.remarks,
                    action_planned=diary.action_planned,
                    follow_up_date=diary.follow_up_date,
                    counselor_name=counselor_name,
                    counselor_id=diary.counselor_id,
                    created_at=diary.created_at,
                )
            )

        marks = await cls.get_report_card_marks(student_id, db)

        semester_grades: list[schemas.SemesterGradeRecord] = []
        internal_marks: list[schemas.InternalMarkRecord] = []
        for m in marks:
            credits = m.subject.credits
            if not credits:
                credits = CURRICULUM_CREDITS.get(m.subject.course_code, 0)

            semester_grades.append(
                schemas.SemesterGradeRecord(
                    semester=m.semester,
                    subject_code=m.subject.course_code,
                    subject_name=m.subject.name,
                    subject_title=m.subject.name,
                    credits=float(credits) if credits is not None else None,
                    grade=m.grade,
                    marks=m.total_marks,
                    internal_marks=m.internal_marks,
                    attempt=m.attempt,
                    remarks=m.remarks,
                    grade_point=grade_point_from_grade(m.grade),
                )
            )
            if m.cit1 is not None:
                internal_marks.append(
                    schemas.InternalMarkRecord(
                        semester=m.semester,
                        test_number=1,
                        percentage=m.cit1,
                        subject_code=m.subject.course_code,
                        subject_title=m.subject.name,
                    )
                )
            if m.cit2 is not None:
                internal_marks.append(
                    schemas.InternalMarkRecord(
                        semester=m.semester,
                        test_number=2,
                        percentage=m.cit2,
                        subject_code=m.subject.course_code,
                        subject_title=m.subject.name,
                    )
                )
            if m.cit3 is not None:
                internal_marks.append(
                    schemas.InternalMarkRecord(
                        semester=m.semester,
                        test_number=3,
                        percentage=m.cit3,
                        subject_code=m.subject.course_code,
                        subject_title=m.subject.name,
                    )
                )

        record_health = cls.build_record_health(
            contact_info=contact_info,
            family_details=family_details,
            previous_academics=previous_academics,
            extra_curricular=extra_curricular,
            counselor_diary=counselor_diary,
            semester_grades=semester_grades,
            internal_marks=internal_marks,
        )

        return schemas.FullStudentRecord(
            roll_no=roll_no,
            core_profile=None,
            contact_info=contact_info,
            family_details=family_details,
            previous_academics=list(previous_academics or []),
            extra_curricular=list(extra_curricular or []),
            counselor_diary=list(counselor_diary or []),
            semester_grades=semester_grades,
            internal_marks=internal_marks,
            record_health=record_health,
            academic_snapshot=None,
        )

    @staticmethod
    def has_internal_component(subject_code: Optional[str], subject_name: Optional[str], credits: float = 0.0) -> bool:
        code = (subject_code or '').upper()
        name = (subject_name or '').lower()
        if code.startswith('24AC') or 'audit' in name or 'value added' in name or 'non credit' in name:
            return False
        if any(token in name for token in ['lab', 'project', 'practic', 'workshop']):
            return False
        if credits == 0:
            return False
        return True

    @classmethod
    async def calculate_analytics(cls, student: models.Student, db: AsyncSession, semester: Optional[int] = None) -> schemas.AnalyticsSummary:
        # Filter assessments by semester if specified
        all_assessments = list(student.assessments or [])
        if semester:
            assessments = [a for a in all_assessments if a.semester == semester]
        else:
            assessments = all_assessments
            
        assessments = [
            a for a in assessments 
            if a.subject and CURRICULUM_CREDITS.get(a.subject.course_code, a.subject.credits or 0.0) > 0
        ]
        
        # Pull attendance summary from the view for this student
        att_query = "SELECT * FROM v_attendance_summary WHERE student_id = :sid"
        if semester:
            att_query += " AND semester = :sem"
        
        att_res = await db.execute(
            text(att_query),
            {"sid": student.id, "sem": semester},
        )
        att_rows = att_res.mappings().all()

        # Group assessments by subject and semester to calculate GPA
        subject_buckets = {}
        for ass in assessments:
            if not ass.subject: continue
            key = (ass.subject_id, ass.semester)
            if key not in subject_buckets:
                subject_buckets[key] = {
                    'subject': ass.subject, 
                    'semester': ass.semester, 
                    'CIT': [], 
                    'EXAM': None,
                    'grade': None,
                    'result_status': None
                }
            
            if ass.assessment_type.startswith('CIT'):
                subject_buckets[key]['CIT'].append(float(ass.marks or 0.0))
            elif ass.assessment_type == 'SEMESTER_EXAM':
                subject_buckets[key]['EXAM'] = float(ass.marks) if ass.marks is not None else None
                subject_buckets[key]['grade'] = getattr(ass, 'grade', None)
                subject_buckets[key]['result_status'] = getattr(ass, 'result_status', None)

        graded_subjects = []
        for key, data in subject_buckets.items():
            if data['EXAM'] is not None or data['grade'] is not None:
                graded_subjects.append(data)

        # GPA Calculation (Simplified for this refactor)
        total_credit_points = 0.0
        total_credits = 0.0
        for sub in graded_subjects:
            credits = CURRICULUM_CREDITS.get(sub['subject'].course_code, sub['subject'].credits or 0.0)
            # Use grade point if available, else derive from marks
            gp = getattr(sub, 'grade_point', None)
            if gp is None and sub['EXAM'] is not None:
                gp = (sub['EXAM'] / 10)
            
            if gp is not None:
                total_credit_points += float(credits) * float(gp)
                total_credits += float(credits)
        
        average_grade_points = round(total_credit_points / total_credits, 2) if total_credits > 0 else 0.0

        # Internals
        internals = [float(ass.marks) for ass in assessments if ass.assessment_type.startswith('CIT') and ass.marks is not None]
        average_internal = round(sum(internals) / len(internals), 2) if internals else 0.0

        # Backlogs: count only explicitly failed final semester exams.
        fail_grades = {"U", "F", "FAIL", "RA", "AB", "ABSENT"}
        fail_statuses = {"FAIL", "F", "ABSENT", "AB"}
        total_backlogs = 0
        for ass in assessments:
            if str(getattr(ass, "assessment_type", "")).upper() != "SEMESTER_EXAM":
                continue
            if getattr(ass, "is_final", False) is not True:
                continue

            status = str(getattr(ass, "result_status", "") or "").strip().upper()
            grade = str(getattr(ass, "grade", "") or "").strip().upper()
            if status in fail_statuses or grade in fail_grades:
                total_backlogs += 1

        # Implement Grade Distribution (Subject Mastery) - Filter out NULLs
        grade_counts = {}
        for sub in subject_buckets.values():
            grade = sub.get('grade')
            if grade and grade.strip() and grade.upper() not in ('NULL', 'NONE'):
                g_key = grade.upper()
                grade_counts[g_key] = grade_counts.get(g_key, 0) + 1
        
        grade_distribution = [
            schemas.GradeDistributionItem(grade=g, count=c)
            for g, c in sorted(grade_counts.items())
        ]

        # Semester Performance (Rollup)
        # Note: If semester is filtered, this list will only contain that semester or be derived accordingly.
        semester_perf_map = {}
        
        # Populate from subject_buckets which contains the aggregated marks/grades
        for key, data in subject_buckets.items():
            sem = data['semester']
            if sem not in semester_perf_map:
                semester_perf_map[sem] = {
                    "count": 0,
                    "internal_sum": 0,
                    "internal_count": 0,
                    "gp_sum": 0.0,
                    "credits_sum": 0.0,
                    "backlogs": 0
                }
            
            group = semester_perf_map[sem]
            group["count"] += 1
            
            # Internals
            if data['CIT']:
                avg_cit = sum(data['CIT']) / len(data['CIT'])
                group["internal_sum"] += avg_cit
                group["internal_count"] += 1
            
            # Grade Points & Credits
            credits = CURRICULUM_CREDITS.get(data['subject'].course_code, data['subject'].credits or 0.0)
            gp = grade_point_from_grade(data['grade'])
            
            # Fallback to derivation from marks if grade is missing but exam marks are present
            if gp is None and data['EXAM'] is not None:
                gp = data['EXAM'] / 10.0
            
            if gp is not None and credits > 0:
                group["gp_sum"] += float(credits) * float(gp)
                group["credits_sum"] += float(credits)

            # Backlogs
            status = str(data['result_status'] or "").strip().upper()
            grade = str(data['grade'] or "").strip().upper()
            if status in fail_statuses or grade in fail_grades:
                group["backlogs"] += 1

        semester_performance = [
            schemas.SemesterPerformanceItem(
                semester=s,
                subject_count=data["count"],
                average_internal=round(data["internal_sum"] / data["internal_count"], 2) if data["internal_count"] > 0 else 0.0,
                average_grade_points=round(data["gp_sum"] / data["credits_sum"], 2) if data["credits_sum"] > 0 else 0.0,
                backlog_count=data["backlogs"]
            )
            for s, data in sorted(semester_perf_map.items())
        ]

        risk_subjects = []
        strength_subjects = []

        # Aggregating attendance from view rows
        total_present = sum((row.get("present") or 0) + (row.get("on_duty") or 0) for row in att_rows)
        total_hours = sum(row.get("total_periods") or 0 for row in att_rows)
        absent_days = sum(row.get("absent") or 0 for row in att_rows)
        
        return schemas.AnalyticsSummary(
            average_grade_points=average_grade_points,
            average_internal=average_internal,
            total_backlogs=total_backlogs,
            total_subjects=len(subject_buckets),
            grade_distribution=grade_distribution,
            semester_performance=semester_performance,
            risk_subjects=risk_subjects,
            strength_subjects=strength_subjects,
            attendance=schemas.AttendanceInsight(
                total_present=int(total_present),
                total_hours=int(total_hours),
                percentage=round((total_present / total_hours) * 100, 2) if total_hours > 0 else 0.0,
                recent_streak_days=0,
                absent_days=int(absent_days),
            ),
        )

    @classmethod
    async def calculate_student_risk(cls, student: models.Student, db: AsyncSession, semester: Optional[int] = None) -> schemas.StudentRiskScore:
        analytics = await cls.calculate_analytics(student, db, semester)
        
        risk_score = 0.0
        alerts = []
        
        att_percentage = analytics.attendance.percentage
        att_risk = max(0, (75 - att_percentage) / 75 * 100) if att_percentage < 75 else 0
        risk_score += att_risk * 0.3
        if att_percentage < 75:
            alerts.append(f"Low Attendance: {att_percentage}%")
            
        internal_avg = analytics.average_internal
        internal_marks_available = any(ass.assessment_type.startswith('CIT') for ass in (student.assessments or []))
        if internal_marks_available:
            internal_risk = max(0, (60 - internal_avg) / 60 * 100) if internal_avg < 60 else 0
            risk_score += internal_risk * 0.3
            if internal_avg < 60:
                alerts.append(f"Low Internals: {internal_avg}%")
            
        gpa_drop = 0.0
        if len(analytics.semester_performance) >= 2:
            perf = sorted(analytics.semester_performance, key=lambda x: x.semester)
            current_gpa = perf[-1].average_grade_points
            prev_gpa = perf[-2].average_grade_points
            gpa_drop = max(0, prev_gpa - current_gpa)
            velocity_risk = min(100, gpa_drop * 50)
            risk_score += velocity_risk * 0.4
            if gpa_drop > 0.5:
                alerts.append(f"Significant GPA Drop: -{round(gpa_drop, 2)}")

        risk_level = "Low"
        if risk_score > 70: risk_level = "Critical"
        elif risk_score > 50: risk_level = "High"
        elif risk_score > 30: risk_level = "Moderate"

        return schemas.StudentRiskScore(
            roll_no=student.roll_no,
            name=student.name,
            risk_score=round(risk_score, 2),
            attendance_factor=round(att_percentage, 2),
            internal_marks_factor=round(internal_avg, 2),
            gpa_drop_factor=round(gpa_drop, 2),
            is_at_risk=risk_score > 50,
            risk_level=risk_level,
            alerts=alerts
        )

    @classmethod
    async def build_student_command_center(cls, student: models.Student, db: AsyncSession, semester: Optional[int] = None) -> schemas.StudentCommandCenterResponse:
        analytics = await cls.calculate_analytics(student, db, semester)
        risk = await cls.calculate_student_risk(student, db, semester)
        
        # Populate metrics
        gpa_trend = 0.0
        placement_readiness = 80.0 # Placeholder
        
        metrics = [
            schemas.StudentMetricCard(
                label="GPA Proxy", 
                value=analytics.average_grade_points, 
                trend=gpa_trend,
                icon="TrendingUp",
                hint="Based on current semester internals and historic grades"
            ),
            schemas.StudentMetricCard(
                label="Attendance", 
                value=analytics.attendance.percentage, 
                unit="%",
                icon="Calendar",
                hint=f"{analytics.attendance.total_present}/{analytics.attendance.total_hours} Hours | {analytics.attendance.absent_days} absences recorded"
            ),
            schemas.StudentMetricCard(
                label="Placement Readiness", 
                value=placement_readiness, 
                unit="%",
                icon="Target",
                hint="Score based on CGPA, Backlogs, and Skill Domain performance"
            ),
            schemas.StudentMetricCard(
                label="Active Backlogs", 
                value=float(analytics.total_backlogs),
                icon="AlertTriangle",
                hint="Clear existing backlogs to improve placement eligibility"
            ),
        ]

        recommended_actions = []
        if analytics.attendance.percentage < 75:
            recommended_actions.append(schemas.StudentActionItem(
                title="Improve Attendance",
                detail=f"Your attendance is {analytics.attendance.percentage}%. You need 75% to be eligible for exams.",
                tone="critical"
            ))
        
        if analytics.total_backlogs > 0:
            recommended_actions.append(schemas.StudentActionItem(
                title="Clear Backlogs",
                detail=f"You have {analytics.total_backlogs} active backlogs. Focus on clearing them in the next attempt.",
                tone="warning"
            ))

        if analytics.average_grade_points < 6.0:
            recommended_actions.append(schemas.StudentActionItem(
                title="Academic Support",
                detail="Your current GPA proxy is below 6.0. Consider reaching out to your counselor for guidance.",
                tone="warning"
            ))

        if not recommended_actions:
            recommended_actions.append(schemas.StudentActionItem(
                title="Maintain Momentum",
                detail="Your academic profile looks strong! Keep up the consistent performance.",
                tone="positive"
            ))

        # Recent Results
        marks = await cls.get_report_card_marks(student.id, db)
        recent_results: List[schemas.SemesterGradeRecord] = []
        for m in sorted(marks, key=lambda x: (x.semester, x.subject.name), reverse=True)[:10]:
            recent_results.append(
                schemas.SemesterGradeRecord(
                    semester=m.semester,
                    subject_code=m.subject.course_code,
                    subject_name=m.subject.name,
                    subject_title=m.subject.name,
                    credits=m.subject.credits,
                    grade=m.grade,
                    marks=m.total_marks,
                    internal_marks=m.internal_marks,
                    attempt=m.attempt,
                    remarks=m.remarks,
                    grade_point=grade_point_from_grade(m.grade),
                )
            )

        # Record Health
        contact_info = (await db.execute(select(models.ContactInfo).filter(models.ContactInfo.student_id == student.id))).scalars().first()
        family_details = (await db.execute(select(models.FamilyDetail).filter(models.FamilyDetail.student_id == student.id))).scalars().first()
        previous_academics = (await db.execute(select(models.PreviousAcademic).filter(models.PreviousAcademic.student_id == student.id))).scalars().all()
        extra_curricular = (await db.execute(select(models.ExtraCurricular).filter(models.ExtraCurricular.student_id == student.id))).scalars().all()
        diary_rows = (await db.execute(select(models.CounselorDiary).filter(models.CounselorDiary.student_id == student.id))).scalars().all()
        
        internal_marks_count = len([m for m in marks if m.cit1 is not None or m.cit2 is not None or m.cit3 is not None])
        
        record_health = cls.build_record_health(
            contact_info=contact_info,
            family_details=family_details,
            previous_academics=previous_academics,
            extra_curricular=extra_curricular,
            counselor_diary=diary_rows,
            semester_grades=marks,
            internal_marks=[1] * internal_marks_count # Dummy list for length check if needed OR adjust build_record_health
        )

        return schemas.StudentCommandCenterResponse(
            roll_no=student.roll_no,
            student_name=student.name,
            batch=student.batch,
            current_semester=student.current_semester,
            analytics=analytics,
            risk=risk,
            metrics=metrics,
            recommended_actions=recommended_actions,
            semester_focus=analytics.semester_performance,
            recent_results=recent_results,
            record_health=record_health
        )

    @staticmethod
    def build_record_health(contact_info, family_details, previous_academics, extra_curricular, counselor_diary, semester_grades, internal_marks):
        sections = {
            'contact': bool(contact_info),
            'family': bool(family_details),
            'previous_academics': bool(previous_academics),
            'activities': bool(extra_curricular),
            'counselor_notes': bool(counselor_diary),
            'semester_grades': bool(semester_grades),
            'internal_marks': bool(internal_marks),
        }
        available_sections = [label for label, present in sections.items() if present]
        missing_sections = [label for label, present in sections.items() if not present]
        completion_percentage = round((len(available_sections) / len(sections)) * 100, 2) if sections else 0.0
        
        return schemas.StudentRecordHealth(
            completion_percentage=completion_percentage,
            available_sections=available_sections,
            missing_sections=missing_sections,
        )
    @staticmethod
    async def get_detailed_attendance(
        student_id: int, 
        semester: Optional[int], 
        page: int, 
        size: int, 
        db: AsyncSession
    ) -> schemas.PaginatedAttendance:
        # Paginate by distinct attendance dates.
        total_stmt = text(
            """
            SELECT COUNT(DISTINCT date) AS total
            FROM period_attendance
            WHERE student_id = :sid
              AND (CAST(:sem AS INTEGER) IS NULL OR semester = CAST(:sem AS INTEGER))
            """
        )
        total = int((await db.execute(total_stmt, {"sid": student_id, "sem": semester})).scalar() or 0)

        dates_stmt = text(
            """
            SELECT date, MAX(semester) AS semester
            FROM period_attendance
            WHERE student_id = :sid
              AND (CAST(:sem AS INTEGER) IS NULL OR semester = CAST(:sem AS INTEGER))
            GROUP BY date
            ORDER BY date DESC
            OFFSET :offset LIMIT :limit
            """
        )
        date_rows = (
            await db.execute(
                dates_stmt,
                {"sid": student_id, "sem": semester, "offset": (page - 1) * size, "limit": size},
            )
        ).mappings().all()

        per_day_stmt = text(
            """
            WITH periods AS (
                SELECT generate_series(1, 7) AS period
            )
            SELECT
                ARRAY_AGG(
                    CASE COALESCE(pa.status, 'A')
                        WHEN 'O' THEN 'OD'
                        ELSE COALESCE(pa.status, 'A')::text
                    END
                    ORDER BY p.period
                ) AS status_array,
                SUM(CASE WHEN COALESCE(pa.status, 'A') IN ('P', 'O') THEN 1 ELSE 0 END) AS present_periods,
                SUM(CASE WHEN COALESCE(pa.status, 'A') = 'A' THEN 1 ELSE 0 END) AS absent_periods,
                SUM(CASE WHEN COALESCE(pa.status, 'A') = 'L' THEN 1 ELSE 0 END) AS leave_periods
            FROM periods p
            LEFT JOIN period_attendance pa
             ON pa.student_id = :sid
             AND pa.date = :d
             AND pa.period = p.period
             AND (CAST(:sem AS INTEGER) IS NULL OR pa.semester = CAST(:sem AS INTEGER))
            """
        )

        items: list[schemas.AttendanceResponse] = []
        for row in date_rows:
            d = row["date"]
            sem_val = int(row["semester"] or (semester or 0) or 0)
            day = (await db.execute(per_day_stmt, {"sid": student_id, "d": d, "sem": semester})).mappings().first()
            present = int(day["present_periods"] or 0)
            absent = int(day["absent_periods"] or 0)
            leave = int(day["leave_periods"] or 0)
            total_periods = 7

            items.append(
                schemas.AttendanceResponse(
                    student_id=student_id,
                    semester=sem_val,
                    date=d,
                    status_array=list(day["status_array"] or []),
                    total_present=present,
                    total_hours=total_periods,
                    present_periods=present,
                    absent_periods=absent,
                    leave_periods=leave,
                    total_periods=total_periods,
                    attendance_percentage=round((present / total_periods) * 100, 2) if total_periods > 0 else 0.0,
                )
            )
            
        # Summary from view
        sum_stmt = text(
            """
            SELECT
                student_id,
                subject_id,
                semester,
                total_periods,
                present,
                absent,
                leave,
                on_duty,
                attendance_pct
            FROM v_attendance_summary
            WHERE student_id = :sid
              AND (CAST(:sem AS INTEGER) IS NULL OR semester = CAST(:sem AS INTEGER))
            """
        )
        sum_rows = (await db.execute(sum_stmt, {"sid": student_id, "sem": semester})).mappings().all()
        
        summary = None
        if sum_rows:
            # Aggregate across semesters
            t_present = sum((r["present"] or 0) + (r["on_duty"] or 0) for r in sum_rows)
            t_total = sum(r["total_periods"] or 0 for r in sum_rows)
            summary = schemas.AttendanceInsight(
                total_present=t_present,
                total_hours=t_total,
                percentage=round((t_present / t_total) * 100, 2) if t_total > 0 else 0,
                recent_streak_days=0,
                absent_days=sum(r["absent"] or 0 for r in sum_rows),
            )
            
        pages = (total + size - 1) // size if size > 0 else 0
        
        return schemas.PaginatedAttendance(
            items=items,
            total=total,
            page=page,
            size=size,
            pages=pages,
            summary=summary
        )

    @classmethod
    async def compute_batch_hybrid_performance(
        cls, 
        student_ids: List[int], 
        db: AsyncSession,
        semester: Optional[int] = None
    ) -> Dict[int, List[schemas.ReportCardMark]]:
        """
        Compute hybrid performance metrics for multiple students efficiently in batch.
        
        This method processes entire classes/cohorts in a single operation rather than
        individual student queries, significantly reducing database overhead.
        
        Args:
            student_ids: List of student IDs to process
            semester: Optional semester filter (if None, all semesters)
            db: Database session
            
        Returns:
            Dictionary mapping student_id to their list of ReportCardMark with hybrid metrics
        """
        if not student_ids:
            return {}
            
        # Build query for batch processing - similar to individual query but for multiple students
        student_ids_param = ','.join(str(sid) for sid in student_ids)
        
        batch_query = text(f"""
            WITH student_ctx AS (
                SELECT
                    s.id as student_id,
                    s.program_id,
                    s.current_semester,
                    s.section,
                    s.batch,
                    GREATEST(
                        COALESCE(s.current_semester, 0),
                        COALESCE((
                            SELECT MAX(sa.semester)
                            FROM student_assessments sa
                            WHERE sa.student_id = s.id
                        ), 0)
                    ) AS max_semester
                FROM students s
                WHERE s.id IN ({student_ids_param})
            ),
            subject_catalog AS (
                SELECT
                    sc.student_id,
                    s.id AS subject_id,
                    s.course_code,
                    s.name AS subject_name,
                    COALESCE(s.credits, 0) AS credits,
                    s.semester,
                    COALESCE(s.pass_threshold, 50.0) AS pass_threshold,
                    s.target_average,
                    COALESCE(s.percentile_excellent, 85.0) AS percentile_excellent,
                    COALESCE(s.percentile_good, 60.0) AS percentile_good,
                    COALESCE(s.percentile_average, 30.0) AS percentile_average
                FROM student_ctx sc
                CROSS JOIN subjects s
                WHERE (s.program_id = sc.program_id OR s.program_id IS NULL)
                  AND s.is_active = true
                  AND (s.course_code NOT LIKE '24MCBC%'
                       OR EXISTS (
                           SELECT 1
                           FROM student_assessments sa
                           WHERE sa.student_id = sc.student_id AND sa.subject_id = s.id
                       ))
                  AND s.semester IS NOT NULL
                  AND s.semester <= sc.max_semester
                  {'AND s.semester = :sem_filter' if semester else ''}
                  AND (sc.section IS NULL 
                       OR EXISTS (
                           SELECT 1 FROM faculty_subject_assignments fsa 
                           WHERE fsa.subject_id = s.id AND fsa.section = sc.section
                       )
                       OR EXISTS (
                           SELECT 1 FROM student_assessments sa
                           WHERE sa.student_id = sc.student_id AND sa.subject_id = s.id
                       ))
            ),
            marks_pivot AS (
                SELECT
                    a.student_id,
                    a.subject_id,
                    a.semester,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT1') AS cit1,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT2') AS cit2,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'CIT3') AS cit3,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS sem_exam_marks,
                    MAX(a.grade) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM' AND a.is_final = true) AS sem_grade,
                    MAX(a.result_status) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM' AND a.is_final = true) AS sem_result_status,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'LAB') AS lab,
                    MAX(a.marks) FILTER (WHERE a.assessment_type = 'PROJECT') AS project,
                    MAX(a.attempt) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS attempt,
                    MAX(a.remarks) FILTER (WHERE a.assessment_type = 'SEMESTER_EXAM') AS remarks
                FROM student_assessments a
                WHERE a.student_id IN ({student_ids_param})
                GROUP BY a.student_id, a.subject_id, a.semester
            ),
            marks_with_totals AS (
                SELECT
                    mp.*,
                    CASE 
                        WHEN mp.sem_exam_marks IS NOT NULL THEN 
                            COALESCE(
                                (CASE 
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit2 IS NOT NULL AND mp.cit3 IS NOT NULL THEN
                                        (GREATEST(mp.cit1, mp.cit2) + GREATEST(mp.cit2, mp.cit3) + GREATEST(mp.cit1, mp.cit3) - LEAST(mp.cit1, mp.cit2, mp.cit3)) / 2.0
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit2 IS NOT NULL THEN (mp.cit1 + mp.cit2) / 2.0
                                    WHEN mp.cit1 IS NOT NULL AND mp.cit3 IS NOT NULL THEN (mp.cit1 + mp.cit3) / 2.0
                                    WHEN mp.cit2 IS NOT NULL AND mp.cit3 IS NOT NULL THEN (mp.cit2 + mp.cit3) / 2.0
                                    WHEN mp.cit1 IS NOT NULL THEN mp.cit1
                                    WHEN mp.cit2 IS NOT NULL THEN mp.cit2
                                    WHEN mp.cit3 IS NOT NULL THEN mp.cit3
                                    ELSE 0
                                END), 0
                            ) + mp.sem_exam_marks
                        WHEN mp.lab IS NOT NULL THEN mp.lab
                        WHEN mp.project IS NOT NULL THEN mp.project
                        ELSE NULL
                    END AS computed_total_marks
                FROM marks_pivot mp
            ),
            subject_cohort_totals AS (
                SELECT 
                    sa.subject_id,
                    sa.semester,
                    sa.student_id,
                    CASE 
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'SEMESTER_EXAM') IS NOT NULL THEN
                            COALESCE(
                                (SELECT 
                                    CASE 
                                        WHEN c1.marks IS NOT NULL AND c2.marks IS NOT NULL AND c3.marks IS NOT NULL THEN
                                            (GREATEST(c1.marks, c2.marks) + GREATEST(c2.marks, c3.marks) + GREATEST(c1.marks, c3.marks) - LEAST(c1.marks, c2.marks, c3.marks)) / 2.0
                                        WHEN c1.marks IS NOT NULL AND c2.marks IS NOT NULL THEN (c1.marks + c2.marks) / 2.0
                                        WHEN c1.marks IS NOT NULL AND c3.marks IS NOT NULL THEN (c1.marks + c3.marks) / 2.0  
                                        WHEN c2.marks IS NOT NULL AND c3.marks IS NOT NULL THEN (c2.marks + c3.marks) / 2.0
                                        WHEN c1.marks IS NOT NULL THEN c1.marks
                                        WHEN c2.marks IS NOT NULL THEN c2.marks
                                        WHEN c3.marks IS NOT NULL THEN c3.marks
                                        ELSE 0
                                    END
                                 FROM (
                                     SELECT MAX(marks) FILTER (WHERE assessment_type = 'CIT1') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c1
                                 CROSS JOIN (
                                     SELECT MAX(marks) FILTER (WHERE assessment_type = 'CIT2') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c2
                                 CROSS JOIN (
                                     SELECT MAX(marks) FILTER (WHERE assessment_type = 'CIT3') AS marks
                                     FROM student_assessments 
                                     WHERE student_id = sa.student_id AND subject_id = sa.subject_id AND semester = sa.semester
                                 ) c3
                                ), 0
                            ) + MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'SEMESTER_EXAM')
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'LAB') IS NOT NULL THEN 
                            MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'LAB')
                        WHEN MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'PROJECT') IS NOT NULL THEN 
                            MAX(sa.marks) FILTER (WHERE sa.assessment_type = 'PROJECT')
                        ELSE NULL
                    END AS student_total_marks
                FROM student_assessments sa
                INNER JOIN student_ctx sc ON sa.student_id = sc.student_id
                WHERE sa.is_final = true
                  AND EXISTS (
                      SELECT 1 FROM subjects s 
                      WHERE s.id = sa.subject_id 
                        AND (s.program_id = sc.program_id OR s.program_id IS NULL)
                        AND s.is_active = true
                  )
                GROUP BY sa.subject_id, sa.semester, sa.student_id
            ),
            subject_percentiles AS (
                SELECT
                    sct.subject_id,
                    sct.semester,
                    sct.student_id,
                    sct.student_total_marks,
                    PERCENT_RANK() OVER (
                        PARTITION BY sct.subject_id, sct.semester 
                        ORDER BY sct.student_total_marks ASC
                    ) * 100 AS percentile_rank,
                    AVG(sct.student_total_marks) OVER (
                        PARTITION BY sct.subject_id, sct.semester
                    ) AS subject_average
                FROM subject_cohort_totals sct
                WHERE sct.student_total_marks IS NOT NULL
            )
            SELECT
                sc.student_id,
                sc.semester AS semester,
                sc.subject_id AS subject_id,
                sc.course_code AS course_code,
                sc.subject_name AS subject_name,
                sc.credits AS credits,
                mwt.cit1,
                mwt.cit2,
                mwt.cit3,
                mwt.sem_exam_marks,
                mwt.sem_grade,
                mwt.sem_result_status,
                mwt.lab,
                mwt.project,
                mwt.attempt,
                mwt.remarks,
                mwt.computed_total_marks,
                COALESCE(sp.percentile_rank, 0) AS percentile_rank,
                CASE 
                    WHEN mwt.computed_total_marks IS NOT NULL AND sp.subject_average > 0 THEN
                        ROUND(mwt.computed_total_marks / sp.subject_average, 3)
                    ELSE NULL
                END AS normalized_score,
                sp.subject_average,
                CASE 
                    WHEN mwt.computed_total_marks IS NULL OR sp.percentile_rank IS NULL THEN NULL
                    WHEN sp.percentile_rank < sc.percentile_average OR mwt.computed_total_marks < sc.pass_threshold THEN 'At Risk'
                    WHEN sp.percentile_rank <= sc.percentile_good THEN 'Average'
                    WHEN sp.percentile_rank <= sc.percentile_excellent THEN 'Good'
                    ELSE 'Excellent'
                END AS performance_label
            FROM subject_catalog sc
            LEFT JOIN marks_with_totals mwt
              ON mwt.student_id = sc.student_id
             AND mwt.subject_id = sc.subject_id
             AND mwt.semester = sc.semester
            LEFT JOIN subject_percentiles sp
              ON sp.subject_id = sc.subject_id
             AND sp.semester = sc.semester
             AND sp.student_id = sc.student_id
            ORDER BY sc.student_id, sc.semester ASC, sc.course_code ASC
        """)
        
        # Execute query with semester filter if provided
        params = {}
        if semester:
            params['sem_filter'] = semester
            
        rows = (await db.execute(batch_query, params)).mappings().all()
        
        # Group results by student_id
        student_results: Dict[int, List[schemas.ReportCardMark]] = {}
        
        for row in rows:
            student_id = int(row["student_id"])
            if student_id not in student_results:
                student_results[student_id] = []
                
            course_code = row["course_code"]
            is_audit = str(course_code or "").upper().startswith("24AC")
            has_exam_component = (
                row.get("sem_grade") is not None
                or row.get("sem_exam_marks") is not None
                or row.get("lab") is not None
                or row.get("project") is not None
            )

            sem_grade = row.get("sem_grade")
            sem_result_status = row.get("sem_result_status")
            sem_exam_marks = row.get("sem_exam_marks")

            computed = compute_grade(
                course_code=course_code,
                cit1=row["cit1"],
                cit2=row["cit2"],
                cit3=row["cit3"],
                semester_exam=sem_exam_marks,
                lab=row["lab"],
                project=row["project"],
            )
            
            subject = schemas.Subject(
                id=int(row["subject_id"]),
                course_code=str(course_code),
                name=str(row["subject_name"]),
                credits=float(
                    CURRICULUM_CREDITS.get(str(course_code), row["credits"] or 0) or 0
                ),
                semester=int(row["semester"]),
            )
            
            mark = schemas.ReportCardMark(
                semester=int(row["semester"]),
                subject=subject,
                cit1=float(row["cit1"]) if row["cit1"] is not None else None,
                cit2=float(row["cit2"]) if row["cit2"] is not None else None,
                cit3=float(row["cit3"]) if row["cit3"] is not None else None,
                sem_exam=None,  # Policy: never expose SEMESTER_EXAM marks to student-facing clients
                lab=float(row["lab"]) if row["lab"] is not None else None,
                project=float(row["project"]) if row["project"] is not None else None,
                internal_marks=computed.internal,
                total_marks=(
                    computed.total
                    if (
                        is_audit
                        or (sem_grade is not None and sem_exam_marks is not None)
                        or (sem_grade is None and has_exam_component)
                    )
                    else None
                ),
                grade=(str(sem_grade) if sem_grade is not None else computed.grade) if (is_audit or has_exam_component) else None,
                result_status=(str(sem_result_status) if sem_result_status is not None else computed.result_status) if (is_audit or has_exam_component) else None,
                attempt=int(row["attempt"]) if row["attempt"] is not None else None,
                remarks=str(row["remarks"]) if row["remarks"] is not None else None,
                # Enhanced hybrid performance metrics
                percentile=float(row["percentile_rank"]) if row["percentile_rank"] is not None else None,
                normalized_score=float(row["normalized_score"]) if row["normalized_score"] is not None else None,
                performance_label=str(row["performance_label"]) if row["performance_label"] is not None else None,
            )
            
            student_results[student_id].append(mark)
            
        return student_results

    @classmethod
    async def get_batch_performance_summary(
        cls, 
        student_ids: List[int], 
        db: AsyncSession,
        semester: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Generate batch performance summary statistics for administrative dashboards.
        
        Returns aggregated metrics across the student cohort including:
        - Performance label distribution
        - Average percentiles by subject
        - Normalization statistics
        - Risk indicators
        
        Args:
            student_ids: List of student IDs to analyze
            semester: Optional semester filter
            db: Database session
            
        Returns:
            Dictionary with aggregated performance statistics
        """
        if not student_ids:
            return {}
            
        # Get batch hybrid performance data
        batch_results = await cls.compute_batch_hybrid_performance(student_ids, db, semester)
        
        if not batch_results:
            return {}
            
        # Aggregate statistics
        performance_distribution = {"At Risk": 0, "Average": 0, "Good": 0, "Excellent": 0}
        subject_stats = {}
        total_marks_count = 0
        risk_students = []
        
        for student_id, marks in batch_results.items():
            student_risk_count = 0
            
            for mark in marks:
                if mark.performance_label:
                    performance_distribution[mark.performance_label] += 1
                    
                if mark.performance_label == "At Risk":
                    student_risk_count += 1
                    
                if mark.total_marks is not None:
                    total_marks_count += 1
                    
                # Track subject-wise statistics
                subject_key = f"{mark.subject.course_code}_{mark.semester}"
                if subject_key not in subject_stats:
                    subject_stats[subject_key] = {
                        "course_code": mark.subject.course_code,
                        "subject_name": mark.subject.name,
                        "semester": mark.semester,
                        "percentiles": [],
                        "normalized_scores": [],
                        "performance_labels": []
                    }
                    
                if mark.percentile is not None:
                    subject_stats[subject_key]["percentiles"].append(mark.percentile)
                if mark.normalized_score is not None:
                    subject_stats[subject_key]["normalized_scores"].append(mark.normalized_score)
                if mark.performance_label:
                    subject_stats[subject_key]["performance_labels"].append(mark.performance_label)
                    
            # Track students with high risk (multiple "At Risk" subjects)
            if student_risk_count >= 2:
                risk_students.append(student_id)
                
        # Calculate subject aggregates
        subject_summaries = []
        for subject_key, stats in subject_stats.items():
            avg_percentile = sum(stats["percentiles"]) / len(stats["percentiles"]) if stats["percentiles"] else 0
            avg_normalized = sum(stats["normalized_scores"]) / len(stats["normalized_scores"]) if stats["normalized_scores"] else 0
            risk_count = stats["performance_labels"].count("At Risk")
            
            subject_summaries.append({
                "course_code": stats["course_code"],
                "subject_name": stats["subject_name"],
                "semester": stats["semester"],
                "average_percentile": round(avg_percentile, 2),
                "average_normalized_score": round(avg_normalized, 3),
                "student_count": len(stats["performance_labels"]),
                "at_risk_count": risk_count,
                "risk_percentage": round((risk_count / len(stats["performance_labels"])) * 100, 2) if stats["performance_labels"] else 0
            })
            
        return {
            "cohort_size": len(batch_results),
            "total_assessments": total_marks_count,
            "performance_distribution": performance_distribution,
            "high_risk_students": risk_students,
            "high_risk_count": len(risk_students),
            "subject_summaries": sorted(subject_summaries, key=lambda x: (x["semester"], x["course_code"])),
            "overall_risk_percentage": round((performance_distribution["At Risk"] / sum(performance_distribution.values())) * 100, 2) if sum(performance_distribution.values()) > 0 else 0
        }
