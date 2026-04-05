from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import joinedload
from collections import Counter

from .. import models, schemas
from ..core.constants import DIRECTORY_SORT_KEYS, GRADE_POINTS
from ..utils.academic_calculations import (
    best_2_of_3_cits_null_check_sql,
    best_2_of_3_cits_with_fallback_sql,
    total_marks_calculation_sql,
    grade_point_from_grade_or_marks_sql,
)

class AdminService:
    @staticmethod
    def _admin_directory_query_text(credits_cte_values: str):
        return f"""
            WITH curriculum_credits_map AS (
                SELECT * FROM (VALUES {credits_cte_values}) AS t(course_code, credit)
            ),
            marks_pivot AS (
                SELECT
                    v.student_id,
                    v.subject_id,
                    v.semester,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'CIT1') AS cit1,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'CIT2') AS cit2,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'CIT3') AS cit3,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'SEMESTER_EXAM') AS sem_exam,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'LAB') AS lab,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'PROJECT') AS project,
                    MAX(v.result_status) FILTER (WHERE v.assessment_type = 'SEMESTER_EXAM') AS sem_result_status,
                    MAX(v.grade) FILTER (WHERE v.assessment_type = 'SEMESTER_EXAM') AS sem_grade
                FROM student_assessments v
                WHERE v.is_final = true
                GROUP BY v.student_id, v.subject_id, v.semester
            ),
            marks_scored AS (
                SELECT
                    mp.student_id,
                    st.roll_no,
                    sb.course_code,
                    CASE 
                        WHEN sb.course_code LIKE '24AC%' THEN 0.0
                        ELSE COALESCE(NULLIF(sb.credits, 0), ccm.credit, 0)
                    END AS credit,
                    ({best_2_of_3_cits_null_check_sql()}) AS internal_max,
                    COALESCE(mp.sem_exam, mp.lab, mp.project) AS exam_component,
                    mp.sem_result_status,
                    mp.sem_grade,
                    CASE
                        WHEN sb.course_code LIKE '24AC%' AND mp.cit1 IS NULL AND mp.cit2 IS NULL AND mp.cit3 IS NULL AND mp.sem_exam IS NULL AND mp.lab IS NULL AND mp.project IS NULL
                        THEN NULL
                        WHEN ({best_2_of_3_cits_null_check_sql()}) IS NULL
                             AND COALESCE(mp.sem_exam, mp.lab, mp.project) IS NULL
                        THEN NULL
                        ELSE ({best_2_of_3_cits_with_fallback_sql()})
                             + COALESCE(COALESCE(mp.sem_exam, mp.lab, mp.project), 0)
                    END AS total_marks
                FROM marks_pivot mp
                JOIN students st ON st.id = mp.student_id
                JOIN subjects sb ON sb.id = mp.subject_id
                LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = sb.course_code
            ),
            grade_agg AS (
                SELECT
                    roll_no,
                    COUNT(*) FILTER (
                        WHERE total_marks IS NOT NULL
                           OR NULLIF(trim(coalesce(sem_grade, '')), '') IS NOT NULL
                    ) AS marks_count,
                    ROUND(AVG(internal_max) FILTER (WHERE internal_max IS NOT NULL)::numeric, 2) AS average_internal_percentage,
                    SUM(
                        CASE
                            WHEN course_code NOT LIKE '24AC%'
                                AND (
                                    upper(coalesce(sem_result_status, '')) IN ('FAIL', 'F', 'ABSENT', 'AB')
                                    OR upper(coalesce(sem_grade, '')) IN ('U', 'F', 'FAIL', 'RA', 'AB', 'ABSENT', 'WH')
                                )
                            THEN 1
                            ELSE 0
                        END
                    ) AS backlogs,
                    (
                        CASE
                            WHEN SUM(credit) FILTER (
                                WHERE credit > 0
                                  AND ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) IS NOT NULL
                            ) > 0 THEN
                                SUM(
                                    ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) * credit
                                ) / SUM(credit) FILTER (
                                    WHERE credit > 0
                                      AND ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) IS NOT NULL
                                )
                            ELSE AVG(
                                ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()})
                            ) FILTER (
                                WHERE ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) IS NOT NULL
                                  AND course_code NOT LIKE '24AC%'
                            )
                        END
                    ) AS average_grade_points_sort
                FROM marks_scored
                GROUP BY roll_no
            ),
            attendance_agg AS (
                SELECT
                    st.roll_no,
                    SUM(v.total_periods) AS attendance_count,
                    ROUND(
                        (100.0 * SUM(v.present + v.on_duty) / NULLIF(SUM(v.total_periods), 0))::numeric,
                        2
                    ) AS attendance_percentage
                FROM v_attendance_summary v
                JOIN students st ON st.id = v.student_id
                GROUP BY st.roll_no
            )
            SELECT
                s.roll_no,
                s.reg_no,
                s.name,
                ci.city,
                COALESCE(ci.email, s.email) AS email,
                ci.phone_primary,
                s.batch,
                s.current_semester,
                s.section,
                COALESCE(ga.marks_count, 0) AS marks_count,
                COALESCE(aa.attendance_count, 0) AS attendance_count,
                COALESCE(aa.attendance_percentage, 0) AS attendance_percentage,
                ROUND(COALESCE(ga.average_grade_points_sort, 0)::numeric, 2) AS average_grade_points,
                COALESCE(ga.average_grade_points_sort, 0) AS average_grade_points_sort,
                COALESCE(ga.average_internal_percentage, 0) AS average_internal_percentage,
                COALESCE(ga.backlogs, 0) AS backlogs,
                u.is_initial_password,
                DENSE_RANK() OVER (
                    ORDER BY
                        COALESCE(ga.average_grade_points_sort, 0) DESC,
                        COALESCE(ga.backlogs, 0) ASC,
                        COALESCE(aa.attendance_percentage, 0) DESC,
                        s.roll_no ASC
                ) AS rank
            FROM students s
            JOIN users u ON u.id = s.id
            LEFT JOIN contact_info ci ON ci.student_id = s.id
            LEFT JOIN grade_agg ga ON ga.roll_no = s.roll_no
            LEFT JOIN attendance_agg aa ON aa.roll_no = s.roll_no
        """

    @classmethod
    async def build_admin_directory(cls, db: AsyncSession, credits_cte_values: str):
        query = text(f"{cls._admin_directory_query_text(credits_cte_values)} ORDER BY s.roll_no DESC")
        result = await db.execute(query)
        rows = result.mappings().all()
        return [schemas.AdminDirectoryStudent(**dict(row)) for row in rows]

    @staticmethod
    def filter_admin_directory(
        directory: list[schemas.AdminDirectoryStudent],
        search: str = '',
        city: str = '',
        batch: str = '',
        semester: Optional[int] = None,
        section: str = '',
        risk_only: bool = False,
        sort_by: str = 'roll_no',
        sort_dir: str = 'desc',
        limit: int = 200,
    ):
        results = directory
        search_term = search.strip().lower()
        if search_term:
            results = [
                item for item in results
                if search_term in ' '.join([
                    item.roll_no or '',
                    item.name or '',
                    item.email or '',
                    item.city or '',
                ]).lower()
            ]
        if city:
            results = [item for item in results if (item.city or '').lower() == city.strip().lower()]
        if batch:
            results = [item for item in results if (item.batch or '').lower() == batch.strip().lower()]
        if semester is not None:
            results = [item for item in results if item.current_semester == semester]
        if section:
            results = [item for item in results if (item.section or '').lower() == section.strip().lower()]
        if risk_only:
            results = [
                item for item in results
                if item.backlogs > 0
                or item.average_grade_points < 6
                or item.average_internal_percentage < 60
                or item.attendance_percentage < 75
                or item.attendance_count == 0
            ]

        key_fn = DIRECTORY_SORT_KEYS.get(sort_by, DIRECTORY_SORT_KEYS['roll_no'])
        reverse = sort_dir.lower() != 'asc'
        results = sorted(results, key=key_fn, reverse=reverse)
        return results[:max(1, min(limit, 500))]

    @staticmethod
    def build_directory_insights(directory: list[schemas.AdminDirectoryStudent]) -> schemas.AdminDirectoryInsights:
        def make_counter(values):
            counter = Counter([value for value in values if value])
            return [schemas.AdminDirectoryInsightItem(label=label, count=count) for label, count in counter.most_common(12)]

        return schemas.AdminDirectoryInsights(
            total_records=len(directory),
            risk_students=sum(
                1 for item in directory
                if item.backlogs > 0
                or item.average_grade_points < 6
                or item.average_internal_percentage < 60
                or item.attendance_percentage < 75
                or item.attendance_count == 0
            ),
            cities=make_counter(item.city for item in directory),
            batches=make_counter(item.batch for item in directory),
            semesters=make_counter(str(item.current_semester) for item in directory if item.current_semester is not None),
            missing_email_count=sum(1 for item in directory if not item.email),
            missing_phone_count=sum(1 for item in directory if not item.phone_primary),
            missing_batch_count=sum(1 for item in directory if not item.batch),
        )

    @classmethod
    def build_admin_analytics(cls, directory: list[schemas.AdminDirectoryStudent]) -> schemas.AdminAnalyticsResponse:
        directory_insights = cls.build_directory_insights(directory)
        risk_breakdown = schemas.AdminRiskBreakdown()
        attendance_bands = Counter()
        gpa_bands = Counter()

        for item in directory:
            if item.attendance_count == 0 and item.marks_count == 0:
                risk_breakdown.missing_data += 1
            elif item.backlogs > 1 or item.attendance_percentage < 65 or item.average_grade_points < 5:
                risk_breakdown.critical += 1
            elif item.backlogs > 0 or item.attendance_percentage < 75 or item.average_grade_points < 6.5:
                risk_breakdown.warning += 1
            else:
                risk_breakdown.healthy += 1

            if item.attendance_count == 0:
                attendance_bands['No data'] += 1
            elif item.attendance_percentage < 75:
                attendance_bands['< 75%'] += 1
            elif item.attendance_percentage < 85:
                attendance_bands['75-85%'] += 1
            else:
                attendance_bands['> 85%'] += 1

            if item.marks_count == 0:
                gpa_bands['No data'] += 1
            elif item.average_grade_points < 6:
                gpa_bands['< 6.0'] += 1
            elif item.average_grade_points < 8:
                gpa_bands['6.0-8.0'] += 1
            else:
                gpa_bands['> 8.0'] += 1

        return schemas.AdminAnalyticsResponse(
            risk_breakdown=risk_breakdown,
            batch_distribution=[schemas.AdminDirectoryInsightItem(label=item.label, count=item.count) for item in directory_insights.batches],
            semester_distribution=[schemas.AdminDirectoryInsightItem(label=item.label, count=item.count) for item in directory_insights.semesters],
            city_distribution=[schemas.AdminDirectoryInsightItem(label=item.label, count=item.count) for item in directory_insights.cities],
            attendance_bands=[schemas.AdminDirectoryInsightItem(label=label, count=count) for label, count in attendance_bands.items()],
            gpa_bands=[schemas.AdminDirectoryInsightItem(label=label, count=count) for label, count in gpa_bands.items()],
        )
    @classmethod
    async def assign_sections(cls, db: AsyncSession, batch: str):
        """
        Orders students by RegNo and divides them into section A (first half) and B (second half).
        """
        # Sanitize batch by removing spaces
        clean_batch = str(batch).replace(" ", "")
        
        # Fetch all students in the given batch (comparing without spaces), ordered by reg_no
        stmt = select(models.Student).where(func.replace(models.Student.batch, ' ', '') == clean_batch).order_by(models.Student.reg_no)
        result = await db.execute(stmt)
        students = result.scalars().all()
        
        if not students:
            return 0
            
        n = len(students)
        mid = n // 2
        
        # Update first half to Section A
        for i, s in enumerate(students):
            if i < mid:
                s.section = 'A'
            else:
                s.section = 'B'
        
        await db.commit()
        return n
