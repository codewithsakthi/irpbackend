from __future__ import annotations

from io import BytesIO
import time
from typing import Iterable, Optional

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .. import schemas
from ..utils.academic_calculations import (
    best_2_of_3_cits_null_check_sql,
    best_2_of_3_cits_with_fallback_sql,
    total_marks_calculation_sql,
    grade_point_from_grade_or_marks_sql,
)


AVG_MARKS_CASE = """
CASE upper(coalesce({grade_col}, ''))
    WHEN 'O' THEN 95
    WHEN 'S' THEN 95
    WHEN 'A+' THEN 85
    WHEN 'A' THEN 75
    WHEN 'B+' THEN 65
    WHEN 'B' THEN 55
    WHEN 'C' THEN 48
    WHEN 'D' THEN 43
    WHEN 'E' THEN 38
    WHEN 'PASS' THEN 50
    WHEN 'P' THEN 50
    ELSE 0
END
"""

GRADE_POINT_CASE = """
CASE upper(coalesce({grade_col}, ''))
    WHEN 'O' THEN 10
    WHEN 'S' THEN 10
    WHEN 'A+' THEN 9
    WHEN 'A' THEN 8
    WHEN 'B+' THEN 7
    WHEN 'B' THEN 6
    WHEN 'C' THEN 5
    WHEN 'D' THEN 4
    WHEN 'E' THEN 3
    WHEN 'PASS' THEN 5
    WHEN 'P' THEN 5
    ELSE 0
END
"""

CODING_PATTERNS = ["python", "program", "java", "data structure", "algorithm", "web", "coding", "problem solving", "logic"]
SYSTEMS_PATTERNS = ["network", "operating system", "database", "architecture", "microprocessor", "hardware", "embedded", "security", "cloud", "digital", "system"]
MATH_PATTERNS = ["math", "discrete", "probability", "statistics", "algebra", "calculus", "numerical", "theory", "analysis"]
PROFESSIONAL_PATTERNS = ["english", "communication", "management", "ethics", "privacy", "entrepreneurship", "environment", "society", "humanities", "economics", "professional"]
LAB_PATTERNS = ["lab", "project", "practic", "workshop", "internship", "seminar"]
AUDIT_PATTERNS = ["audit", "value added", "mandatory", "non credit"]


def _credits_values(curriculum_credits: dict) -> str:
    """Helper to format credits dictionary for SQL VALUES clause safely."""
    vals = []
    if not curriculum_credits:
         return "('', 0)"
    
    for code, credit in curriculum_credits.items():
        if isinstance(credit, dict):
            # Try to find the first numeric value in the nested dictionary
            found_val = 0
            for v in credit.values():
                if isinstance(v, (int, float)):
                    found_val = v
                    break
            vals.append(f"('{code}', {found_val})")
        else:
            # Assume it's a numeric value
            vals.append(f"('{code}', {credit if isinstance(credit, (int, float)) else 0})")
    
    if not vals:
        return "('', 0)"
    return ", ".join(vals)


def _risk_level(score: float) -> str:
    if score >= 70:
        return "Critical"
    if score >= 55:
        return "High"
    if score >= 35:
        return "Moderate"
    return "Low"


def _cast_text_param(name: str) -> str:
    return f"CAST(:{name} AS TEXT)"


def _cast_int_param(name: str) -> str:
    return f"CAST(:{name} AS INTEGER)"


def _attendance_band(attendance_percentage: float) -> str:
    if attendance_percentage >= 90:
        return "Exemplary"
    if attendance_percentage >= 80:
        return "Stable"
    if attendance_percentage >= 75:
        return "Watchlist"
    return "Critical"


def _placement_signal(overall_gpa: float, active_arrears: int, attendance_percentage: float) -> str:
    if overall_gpa >= 7 and active_arrears == 0 and attendance_percentage >= 80:
        return "Placement Ready"
    if overall_gpa >= 6 and active_arrears <= 1:
        return "Needs Finishing Push"
    return "Intervention Required"


def _tone_from_metric(value: float, warning_cutoff: float, critical_cutoff: float, *, reverse: bool = False) -> str:
    if reverse:
        if value <= critical_cutoff:
            return "critical"
        if value <= warning_cutoff:
            return "warning"
        return "positive"
    if value >= critical_cutoff:
        return "critical"
    if value >= warning_cutoff:
        return "warning"
    return "positive"


def _base_ctes(curriculum_credits: dict[str, float]) -> str:
    credits_values = _credits_values(curriculum_credits)

    internal_expr = best_2_of_3_cits_null_check_sql().strip()

    # NOTE: We intentionally compute total-marks inline here instead of using
    # total_marks_calculation_sql(), because that helper references SELECT aliases
    # (e.g. effective_internal_marks) which are not real columns in this query.
    exam_marks_expr = "COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks)"
    raw_total_marks_expr = f"""
    CASE
        WHEN {exam_marks_expr} IS NULL AND ({internal_expr}) IS NULL THEN NULL
        WHEN {exam_marks_expr} IS NULL THEN ({internal_expr})
        WHEN sc.has_internal_component THEN COALESCE(({internal_expr}), 0) + COALESCE({exam_marks_expr}, 0)
        ELSE COALESCE({exam_marks_expr}, 0)
    END
    """.strip()

    # Only treat a subject as "finalized" if an exam/lab/project mark exists OR a semester grade/result exists.
    # This prevents low CIT-only internals from being counted as failures/backlogs.
    final_component_expr = "(\n        COALESCE(mp.has_semester_exam, 0) = 1\n        OR COALESCE(mp.has_lab, 0) = 1\n        OR COALESCE(mp.has_project, 0) = 1\n        OR COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks) IS NOT NULL\n        OR NULLIF(trim(coalesce(mp.sem_grade, '')), '') IS NOT NULL\n        OR NULLIF(trim(coalesce(mp.sem_result_status, '')), '') IS NOT NULL\n    )"

    total_marks_expr = f"CASE WHEN {final_component_expr} THEN ({raw_total_marks_expr}) ELSE NULL END"
    grade_point_expr = grade_point_from_grade_or_marks_sql("mp.sem_grade", f"({total_marks_expr})").strip()
    failed_expr = f"""
    CASE
        WHEN sc.course_code ILIKE '24AC%' THEN 0
        WHEN NOT {final_component_expr} THEN 0
        WHEN upper(coalesce(mp.sem_result_status, '')) IN ('FAIL', 'F', 'ABSENT', 'AB') THEN 1
        WHEN upper(coalesce(mp.sem_grade, '')) IN ('U', 'F', 'FAIL', 'RA', 'AB', 'ABSENT', 'WH') THEN 1
        WHEN ({total_marks_expr}) < 50 AND ({total_marks_expr}) IS NOT NULL THEN 1
        ELSE 0
    END
    """.strip()

    return f"""
    WITH curriculum_credits_map AS (
        SELECT * FROM (VALUES {credits_values}) AS t(course_code, credit)
    ),
    subject_catalog AS (
        SELECT
            s.id,
            s.course_code,
            s.name,
            s.semester,
            COALESCE(NULLIF(s.credits, 0), ccm.credit, 0) AS credit,
            CASE
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(CODING_PATTERNS)})%' THEN 'Programming'
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(MATH_PATTERNS)})%' THEN 'Math'
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(SYSTEMS_PATTERNS)})%' THEN 'Systems'
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(LAB_PATTERNS)})%' THEN 'Practical'
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(PROFESSIONAL_PATTERNS)})%' THEN 'Professional'
                ELSE 'Core'
            END AS skill_domain,
            CASE
                WHEN lower(s.name) SIMILAR TO '%({'|'.join(LAB_PATTERNS + AUDIT_PATTERNS)})%' THEN FALSE
                WHEN s.course_code ILIKE '24AC%' THEN FALSE
                ELSE TRUE
            END AS has_internal_component
        FROM subjects s
        LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = s.course_code
    ),
    final_assessments AS (
        SELECT *
        FROM student_assessments sa
        WHERE sa.is_final = true
    ),
    marks_pivot AS (
        SELECT
            student_id,
            subject_id,
            semester,
            MAX(marks) FILTER (WHERE assessment_type = 'CIT1') AS cit1,
            MAX(marks) FILTER (WHERE assessment_type = 'CIT2') AS cit2,
            MAX(marks) FILTER (WHERE assessment_type = 'CIT3') AS cit3,
            MAX(marks) FILTER (WHERE assessment_type = 'SEMESTER_EXAM') AS sem_exam,
            MAX(grade) FILTER (WHERE assessment_type = 'SEMESTER_EXAM') AS sem_grade,
            MAX(result_status) FILTER (WHERE assessment_type = 'SEMESTER_EXAM') AS sem_result_status,
            MAX(marks) FILTER (WHERE assessment_type = 'LAB') AS lab_marks,
            MAX(marks) FILTER (WHERE assessment_type = 'PROJECT') AS project_marks,
            MAX(CASE WHEN assessment_type = 'SEMESTER_EXAM' THEN 1 ELSE 0 END) AS has_semester_exam,
            MAX(CASE WHEN assessment_type = 'LAB' THEN 1 ELSE 0 END) AS has_lab,
            MAX(CASE WHEN assessment_type = 'PROJECT' THEN 1 ELSE 0 END) AS has_project,
            COALESCE(MAX(remarks) FILTER (WHERE assessment_type = 'SEMESTER_EXAM'), '') AS exam_remarks
        FROM final_assessments
        GROUP BY student_id, subject_id, semester
    ),
    marks_enriched AS (
        SELECT
            st.id AS student_id,
            st.roll_no,
            st.reg_no,
            st.section,
            st.name AS student_name,
            st.batch,
            st.current_semester,
            COALESCE(ci.email, st.email) AS email,
            ci.city,
            mp.semester,
            sc.course_code AS subject_code,
            sc.name AS subject_name,
            sc.skill_domain,
            sc.has_internal_component,
            sc.credit,
            ({internal_expr}) AS internal_raw,
            CASE
                WHEN sc.has_internal_component THEN ({internal_expr})
                ELSE NULL
            END AS internal_marks,
            CASE
                WHEN sc.has_internal_component THEN ({internal_expr})
                ELSE NULL
            END AS effective_internal_marks,
            COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks) AS exam_marks,
            ({total_marks_expr}) AS total_marks,
            CASE
                WHEN NULLIF(trim(coalesce(mp.sem_grade, '')), '') IS NOT NULL THEN mp.sem_grade
                WHEN ({total_marks_expr}) IS NULL THEN NULL
                WHEN ({total_marks_expr}) >= 90 THEN 'O'
                WHEN ({total_marks_expr}) >= 80 THEN 'A+'
                WHEN ({total_marks_expr}) >= 70 THEN 'A'
                WHEN ({total_marks_expr}) >= 60 THEN 'B+'
                WHEN ({total_marks_expr}) >= 50 THEN 'B'
                WHEN ({total_marks_expr}) >= 45 THEN 'C'
                ELSE 'F'
            END AS grade,
            ({grade_point_expr}) AS grade_point,
            ({failed_expr}) AS failed
        FROM marks_pivot mp
        JOIN students st ON st.id = mp.student_id
        JOIN subject_catalog sc ON sc.id = mp.subject_id
        LEFT JOIN contact_info ci ON ci.student_id = st.id
    ),
    attendance_rollup AS (
        SELECT
            student_id,
            ROUND(
                (100.0 * SUM(present + on_duty) / NULLIF(SUM(total_periods), 0))::numeric,
                2
            ) AS attendance_percentage
        FROM v_attendance_summary
        GROUP BY student_id
    ),
    semester_gpa AS (
        SELECT
            me.student_id,
            me.roll_no,
            me.student_name,
            me.batch,
            me.semester,
            ROUND(
                CASE
                    WHEN SUM(me.credit) FILTER (WHERE me.credit > 0) > 0
                    THEN (SUM(me.grade_point * me.credit)::numeric / SUM(me.credit) FILTER (WHERE me.credit > 0))
                    ELSE AVG(me.grade_point)::numeric
                END, 3
            ) AS sgpa,
            ROUND(AVG(me.effective_internal_marks)::numeric, 2) AS internal_avg,
            ROUND(AVG(me.total_marks)::numeric, 2) AS marks_avg
        FROM marks_enriched me
        WHERE me.grade_point IS NOT NULL
          AND me.subject_code NOT ILIKE '24AC%'
        GROUP BY me.student_id, me.roll_no, me.student_name, me.batch, me.semester
    ),
    velocity AS (
        SELECT
            sg.*,
            LAG(sg.sgpa) OVER (PARTITION BY sg.student_id ORDER BY sg.semester) AS previous_sgpa,
            ROUND((sg.sgpa - LAG(sg.sgpa) OVER (PARTITION BY sg.student_id ORDER BY sg.semester))::numeric, 2) AS gpa_velocity
        FROM semester_gpa sg
    ),
    cumulative_gpa AS (
        SELECT
            sg.student_id,
            ROUND(
                CASE
                    WHEN SUM(me.credit) FILTER (WHERE me.credit > 0) > 0
                    THEN (SUM(me.grade_point * me.credit) / SUM(me.credit) FILTER (WHERE me.credit > 0))
                    ELSE AVG(me.grade_point)
                END::numeric, 2
            ) AS cgpa
        FROM semester_gpa sg
        JOIN marks_enriched me ON me.student_id = sg.student_id AND me.semester = sg.semester
        WHERE me.grade_point IS NOT NULL
          AND me.subject_code NOT ILIKE '24AC%'
        GROUP BY sg.student_id
    ),
    student_current AS (
        SELECT DISTINCT ON (st.id)
            st.id AS student_id,
            st.roll_no,
            st.reg_no,
            st.section,
            st.name AS student_name,
            st.batch,
            st.current_semester,
            COALESCE(ci.email, st.email) AS email,
            ci.city,
            COALESCE(ar.attendance_percentage, 0) AS attendance_percentage,
            COALESCE(cg.cgpa, v.sgpa, 0) AS cgpa_proxy,
            v.internal_avg AS internal_avg,
            COALESCE(v.gpa_velocity, 0) AS gpa_velocity,
            COALESCE(v.previous_sgpa, v.sgpa, 0) AS previous_sgpa,
            COALESCE((
                SELECT COUNT(*) FROM marks_enriched m2
                WHERE m2.student_id = st.id AND m2.failed = 1
            ), 0) AS active_arrears,
            u.is_initial_password
        FROM students st
        JOIN users u ON u.id = st.id
        LEFT JOIN contact_info ci ON ci.student_id = st.id
        LEFT JOIN attendance_rollup ar ON ar.student_id = st.id
        LEFT JOIN cumulative_gpa cg ON cg.student_id = st.id
        LEFT JOIN velocity v ON v.student_id = st.id AND v.semester = st.current_semester
        ORDER BY st.id, st.current_semester DESC
    ),
    risk_scores AS (
        SELECT
            sc.*,
            ROUND(LEAST(
                100,
                GREATEST(0, (75 - sc.attendance_percentage) / 75.0 * 100) * 0.30 +
                CASE
                    WHEN sc.internal_avg IS NULL THEN 0
                    ELSE GREATEST(0, (60 - sc.internal_avg) / 60.0 * 100) * 0.30
                END +
                GREATEST(0, (COALESCE(sc.previous_sgpa, sc.cgpa_proxy) - sc.cgpa_proxy) * 20) * 0.40
            )::numeric, 2) AS risk_score
        FROM student_current sc
    )
    """


async def get_subject_leaderboard(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    subject_code: str,
    section: Optional[str],
    limit: int,
    offset: int,
    semester: Optional[int] = None,
) -> schemas.SubjectLeaderboardResponse:
    section_partition = ", me.section" if section else ""
    section_filter = "AND (CAST(:section AS TEXT) IS NULL OR lower(me.section) = lower(CAST(:section AS TEXT)))"
    semester_filter = "AND (CAST(:semester AS INTEGER) IS NULL OR me.semester = CAST(:semester AS INTEGER))"

    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        ranked_subject AS (
            SELECT
                me.roll_no,
                me.student_name,
                me.section,
                me.batch,
                me.current_semester,
                me.subject_code,
                me.subject_name,
                me.total_marks,
                me.internal_marks,
                me.grade,
                RANK() OVER (
                    PARTITION BY me.subject_code, me.batch, me.current_semester{section_partition}
                    ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC, me.roll_no ASC
                ) AS class_rank,
                RANK() OVER (
                    PARTITION BY me.subject_code, me.batch{section_partition}
                    ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC, me.roll_no ASC
                ) AS batch_rank,
                ROUND(((1 - PERCENT_RANK() OVER (PARTITION BY me.subject_code ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC)) * 100)::numeric, 2) AS percentile,
                COUNT(*) OVER () AS total_count
            FROM marks_enriched me
            WHERE lower(me.subject_code) = lower(:subject_code)
            {section_filter}
            {semester_filter}
        )
        SELECT * FROM ranked_subject
        ORDER BY total_marks DESC, class_rank ASC
        OFFSET :offset LIMIT :limit
        """
    )

    rows = (
        await db.execute(
            query,
            {"subject_code": subject_code, "section": section, "limit": limit, "offset": offset, "semester": semester},
        )
    ).mappings().all()

    if not rows:
        meta_query = text(
            """
            SELECT
                s.course_code AS subject_code,
                s.name AS subject_name
            FROM subjects s
            WHERE lower(s.course_code) = lower(:subject_code)
            LIMIT 1
            """
        )
        meta = (await db.execute(meta_query, {"subject_code": subject_code})).mappings().first()
        if not meta:
            raise HTTPException(status_code=404, detail=f"Subject {subject_code} not found")
        
        return schemas.SubjectLeaderboardResponse(
            subject_code=meta["subject_code"],
            subject_name=meta["subject_name"],
            top_leaderboard=[],
            bottom_leaderboard=[],
            pagination=schemas.PaginationMeta(total=0, limit=limit, offset=offset)
        )

    entries = [schemas.LeaderboardEntry(**{k: v for k, v in dict(row).items() if k != "total_count"}) for row in rows]
    subject_name = entries[0].subject_name
    total = int(rows[0]["total_count"])

    # For bottom performers, we reuse the same ranking logic but sort differently
    bottom_query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        ranked_subject AS (
            SELECT
                me.roll_no,
                me.student_name,
                me.section,
                me.batch,
                me.current_semester,
                me.subject_code,
                me.subject_name,
                me.total_marks,
                me.internal_marks,
                me.grade,
                RANK() OVER (
                    PARTITION BY me.subject_code, me.batch, me.current_semester{section_partition}
                    ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC, me.roll_no ASC
                ) AS class_rank,
                RANK() OVER (
                    PARTITION BY me.subject_code, me.batch{section_partition}
                    ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC, me.roll_no ASC
                ) AS batch_rank,
                ROUND(((1 - PERCENT_RANK() OVER (PARTITION BY me.subject_code ORDER BY me.total_marks DESC, COALESCE(me.effective_internal_marks, 0) DESC)) * 100)::numeric, 2) AS percentile
            FROM marks_enriched me
            WHERE lower(me.subject_code) = lower(:subject_code)
            {section_filter}
            {semester_filter}
        )
        SELECT * FROM ranked_subject
        WHERE total_marks > 0 OR grade IN ('U', 'FAIL', 'F', 'AB', 'ABSENT')
        ORDER BY total_marks ASC, COALESCE(internal_marks, 0) ASC, class_rank ASC
        LIMIT :limit
        """
    )

    bottom_rows = (
        await db.execute(bottom_query, {"subject_code": subject_code, "section": section, "limit": limit, "semester": semester})
    ).mappings().all()
    bottom_entries = [schemas.LeaderboardEntry(**dict(row)) for row in bottom_rows]

    return schemas.SubjectLeaderboardResponse(
        subject_code=subject_code,
        subject_name=subject_name,
        top_leaderboard=entries,
        bottom_leaderboard=bottom_entries,
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset),
    )


async def get_overall_leaderboard(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    section: Optional[str] = None,
    batch: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
    semester: Optional[int] = None,
) -> schemas.SubjectLeaderboardResponse:
    """
    Get a leaderboard based on overall performance (GPA) across all subjects.
    """
    section_part = "AND (CAST(:section AS TEXT) IS NULL OR lower(section) = lower(CAST(:section AS TEXT)))"
    batch_part = "AND (CAST(:batch AS TEXT) IS NULL OR upper(batch) = upper(CAST(:batch AS TEXT)))"
    
    # Define a special student_current for specific semester if needed
    semester_join = "v.semester = COALESCE(CAST(:semester AS INTEGER), st.current_semester)"
    
    query = text(
        _base_ctes(curriculum_credits).replace("v.semester = st.current_semester", semester_join)
        + f"""
        ,
        ranked_overall AS (
            SELECT
                roll_no,
                student_name,
                section,
                batch,
                current_semester,
                'OVERALL' AS subject_code,
                'Overall Performance (GPA)' AS subject_name,
                cgpa_proxy AS total_marks, -- Using GPA as total_marks for schema compatibility
                internal_avg AS internal_marks,
                CASE 
                    WHEN cgpa_proxy >= 9 THEN 'O'
                    WHEN cgpa_proxy >= 8 THEN 'A+'
                    WHEN cgpa_proxy >= 7 THEN 'A'
                    WHEN cgpa_proxy >= 6 THEN 'B+'
                    WHEN cgpa_proxy >= 5 THEN 'B'
                    ELSE 'RA'
                END AS grade,
                RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC) AS class_rank,
                RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC) AS batch_rank,
                ROUND(((1 - PERCENT_RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC)) * 100)::numeric, 2) AS percentile,
                COUNT(*) OVER () AS total_count
            FROM student_current
            WHERE 1=1
            {section_part}
            {batch_part}
        )
        SELECT * FROM ranked_overall
        ORDER BY class_rank ASC
        OFFSET :offset LIMIT :limit
        """
    )

    rows = (
        await db.execute(
            query,
            {"section": section, "batch": batch, "limit": limit, "offset": offset, "semester": semester},
        )
    ).mappings().all()

    if not rows:
        return schemas.SubjectLeaderboardResponse(
            subject_code="OVERALL",
            subject_name="Overall Performance (GPA)",
            top_leaderboard=[],
            bottom_leaderboard=[],
            pagination=schemas.PaginationMeta(total=0, limit=limit, offset=offset)
        )

    entries = [schemas.LeaderboardEntry(**{k: v for k, v in dict(row).items() if k != "total_count"}) for row in rows]
    total = int(rows[0]["total_count"])

    # Bottom performers for overall
    bottom_query = text(
        _base_ctes(curriculum_credits).replace("v.semester = st.current_semester", semester_join)
        + f"""
        ,
        base_performance AS (
            SELECT
                roll_no,
                student_name,
                section,
                batch,
                current_semester,
                'OVERALL' AS subject_code,
                'Overall Performance (GPA)' AS subject_name,
                cgpa_proxy AS total_marks,
                internal_avg AS internal_marks,
                CASE 
                    WHEN cgpa_proxy >= 9 THEN 'O'
                    WHEN cgpa_proxy >= 8 THEN 'A+'
                    WHEN cgpa_proxy >= 7 THEN 'A'
                    WHEN cgpa_proxy >= 6 THEN 'B+'
                    WHEN cgpa_proxy >= 5 THEN 'B'
                    ELSE 'RA'
                END AS grade,
                RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC) AS class_rank,
                RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC) AS batch_rank,
                ROUND(((1 - PERCENT_RANK() OVER (ORDER BY cgpa_proxy DESC, attendance_percentage DESC)) * 100)::numeric, 2) AS percentile
            FROM student_current
            WHERE 1=1
            {section_part}
            {batch_part}
        )
        SELECT * FROM base_performance
        WHERE total_marks > 0 OR grade = 'RA'
        ORDER BY total_marks ASC, percentile ASC
        LIMIT :limit
        """
    )

    bottom_rows = (
        await db.execute(bottom_query, {"section": section, "batch": batch, "limit": limit, "semester": semester})
    ).mappings().all()
    bottom_entries = [schemas.LeaderboardEntry(**dict(row)) for row in bottom_rows]

    return schemas.SubjectLeaderboardResponse(
        subject_code="OVERALL",
        subject_name="Overall Performance (GPA)",
        top_leaderboard=entries,
        bottom_leaderboard=bottom_entries,
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset),
    )


async def get_subject_catalog(db: AsyncSession) -> list[schemas.SubjectCatalogItem]:
    """Get subject catalog with threshold data, backwards compatible with pre-migration schema"""
    
    # Check if threshold columns exist in the database
    try:
        # Try the full query with threshold columns first
        query_with_thresholds = text(
            """
            SELECT
                s.id,
                s.course_code AS subject_code,
                s.name AS subject_name,
                s.semester,
                s.is_active,
                COUNT(sa.id) AS records,
                COALESCE(s.pass_threshold, 50.0) AS pass_threshold,
                s.target_average,
                COALESCE(s.percentile_excellent, 85.0) AS percentile_excellent,
                COALESCE(s.percentile_good, 60.0) AS percentile_good,
                COALESCE(s.percentile_average, 30.0) AS percentile_average
            FROM subjects s
            LEFT JOIN student_assessments sa ON sa.subject_id = s.id AND sa.assessment_type = 'SEMESTER_EXAM'
            GROUP BY s.id, s.course_code, s.name, s.semester, s.is_active, 
                     s.pass_threshold, s.target_average, s.percentile_excellent, 
                     s.percentile_good, s.percentile_average
            ORDER BY COUNT(sa.id) DESC, s.course_code
            """
        )
        rows = (await db.execute(query_with_thresholds)).mappings().all()
        return [schemas.SubjectCatalogItem(**dict(row)) for row in rows]
    
    except Exception as e:
        # Log the migration status
        print(f"INFO: Threshold columns not found in database. Using default values. Error: {str(e)}")
        print("INFO: Run 'alembic upgrade head' to apply threshold management schema updates.")
        
        # Fallback to basic query without threshold columns (for pre-migration compatibility)
        query_basic = text(
            """
            SELECT
                s.id,
                s.course_code AS subject_code,
                s.name AS subject_name,
                s.semester,
                s.is_active,
                COUNT(sa.id) AS records,
                50.0 AS pass_threshold,
                75.0 AS target_average,
                85.0 AS percentile_excellent,
                60.0 AS percentile_good,
                30.0 AS percentile_average
            FROM subjects s
            LEFT JOIN student_assessments sa ON sa.subject_id = s.id AND sa.assessment_type = 'SEMESTER_EXAM'
            GROUP BY s.id, s.course_code, s.name, s.semester, s.is_active
            ORDER BY COUNT(sa.id) DESC, s.course_code
            """
        )
        rows = (await db.execute(query_basic)).mappings().all()
        return [schemas.SubjectCatalogItem(**dict(row)) for row in rows]


async def get_student_360(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    roll_no: str,
) -> schemas.Student360Profile:
    """
    Generate a 360-degree profile of a student with comprehensive analytics.
    
    FIXED CALCULATION ISSUES (2026-04-05):
    - Correlation NULL handling: Distinguishes between no correlation vs insufficient data
    - Internals risk driver: Improved NULL handling and status logic
    - GPA velocity scaling: Uses absolute value instead of biased +2 offset
    - Domain scores: Prioritizes actual marks over standardized grade points
    - Peer ranking percentile: Matches ranking order (CGPA DESC, attendance DESC)
    
    Args:
        db: Database session
        curriculum_credits: Subject credit mapping
        roll_no: Student roll number
        
    Returns:
        Student360Profile with full analytics
        
    Raises:
        HTTPException: If student not found or data generation fails
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Validate student exists
    student_check = await db.execute(
        text("SELECT id, roll_no, name FROM students WHERE roll_no = :roll_no LIMIT 1"),
        {"roll_no": roll_no}
    )
    student = student_check.first()
    if not student:
        raise HTTPException(
            status_code=404, 
            detail=f"Student with roll number '{roll_no}' not found in system"
        )
    
    logger.info(f"Generating Student 360 profile for {roll_no} ({student[2]})")
    
    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        domain_master AS (
            SELECT unnest(ARRAY['Programming', 'Math', 'Systems', 'Practical', 'Professional', 'Core']) AS domain
        ),
        cohort_domain_avg AS (
            SELECT
                COALESCE(me.skill_domain, 'Core') AS domain,
                ROUND(
                    AVG(
                        CASE
                            WHEN me.total_marks > 0 THEN me.total_marks
                            WHEN me.effective_internal_marks IS NOT NULL THEN me.effective_internal_marks
                            WHEN me.grade_point > 0 THEN me.grade_point * 10
                            ELSE 0
                        END
                    )::numeric,
                    2
                ) AS cohort_score
            FROM marks_enriched me
            JOIN students target ON target.roll_no = :roll_no
            WHERE me.batch = target.batch AND me.credit > 0
            GROUP BY COALESCE(me.skill_domain, 'Core')
        ),
        domain_scores AS (
            SELECT
                dm.domain,
                COALESCE(ROUND(
                    AVG(
                        CASE
                            WHEN me.total_marks > 0 THEN me.total_marks
                            WHEN me.effective_internal_marks IS NOT NULL THEN me.effective_internal_marks
                            WHEN me.grade_point > 0 THEN me.grade_point * 10
                            ELSE 0
                        END
                    ) FILTER (WHERE me.roll_no = :roll_no)::numeric,
                    2
                ), 0) AS score,
                COALESCE(MAX(cda.cohort_score), 0) AS cohort_score
            FROM domain_master dm
            LEFT JOIN marks_enriched me ON COALESCE(me.skill_domain, 'Core') = dm.domain AND me.credit > 0
            LEFT JOIN cohort_domain_avg cda ON cda.domain = dm.domain
            GROUP BY dm.domain
            ORDER BY CASE 
                WHEN dm.domain = 'Programming' THEN 1
                WHEN dm.domain = 'Math' THEN 2
                WHEN dm.domain = 'Systems' THEN 3
                WHEN dm.domain = 'Practical' THEN 4
                WHEN dm.domain = 'Professional' THEN 5
                ELSE 6 
            END
        ),
        student_series AS (
            SELECT
                v.semester,
                v.sgpa,
                v.previous_sgpa,
                v.gpa_velocity AS velocity,
                COALESCE(ar.attendance_percentage, 0) AS attendance_pct,
                COALESCE(v.internal_avg, 0) AS internal_avg
            FROM velocity v
            LEFT JOIN attendance_rollup ar ON ar.student_id = v.student_id
            WHERE v.roll_no = :roll_no
            ORDER BY v.semester
        ),
        student_profile AS (
            SELECT
                rs.roll_no,
                rs.reg_no,
                rs.student_name,
                rs.batch,
                rs.section,
                rs.current_semester,
                rs.cgpa_proxy AS overall_gpa,
                rs.attendance_percentage,
                rs.gpa_velocity,
                rs.active_arrears,
                ROUND((
                    SELECT
                        CASE
                            WHEN COUNT(*) >= 2 THEN corr(ar.attendance_percentage::float, v.internal_avg::float)
                            ELSE NULL
                        END
                    FROM velocity v
                    LEFT JOIN attendance_rollup ar ON ar.student_id = v.student_id
                    WHERE v.roll_no = :roll_no
                      AND v.internal_avg IS NOT NULL
                      AND ar.attendance_percentage IS NOT NULL
                )::numeric, 2) AS attendance_marks_correlation,
                rs.risk_score
            FROM risk_scores rs
            WHERE rs.roll_no = :roll_no
        ),
        peer_benchmark AS (
            SELECT
                COUNT(*) OVER () AS cohort_size,
                RANK() OVER (ORDER BY sc.cgpa_proxy DESC, sc.attendance_percentage DESC) AS class_rank,
                ROUND(((1 - PERCENT_RANK() OVER (ORDER BY sc.cgpa_proxy DESC, sc.attendance_percentage DESC)) * 100)::numeric, 2) AS percentile,
                ROUND(AVG(sc.cgpa_proxy) OVER ()::numeric, 2) AS cohort_avg_gpa,
                ROUND((sc.cgpa_proxy - AVG(sc.cgpa_proxy) OVER ())::numeric, 2) AS gap_from_cohort,
                sc.roll_no
            FROM student_current sc
            WHERE sc.current_semester = (SELECT current_semester FROM student_profile)
        ),
        subject_strengths AS (
            SELECT
                me.subject_code,
                me.subject_name,
                me.semester,
                me.grade,
                me.total_marks,
                me.internal_marks,
                ROUND(
                    CASE
                        WHEN me.effective_internal_marks IS NULL THEN COALESCE(NULLIF(me.total_marks, 0), me.grade_point * 10)
                        ELSE me.grade_point * 10 + me.effective_internal_marks
                    END::numeric,
                    2
                ) AS score,
                'Strong academic signal' AS note
            FROM marks_enriched me
            WHERE me.roll_no = :roll_no AND me.credit > 0
            ORDER BY score DESC, me.total_marks DESC
            LIMIT 4
        ),
        subject_support AS (
            SELECT
                me.subject_code,
                me.subject_name,
                me.semester,
                me.grade,
                me.total_marks,
                me.internal_marks,
                ROUND(
                    CASE
                        WHEN me.total_marks > 0 THEN me.total_marks
                        WHEN me.effective_internal_marks IS NOT NULL THEN me.effective_internal_marks
                        WHEN me.grade_point > 0 THEN me.grade_point * 10
                        ELSE 0
                    END::numeric,
                    2
                ) AS score,
                CASE
                    WHEN me.failed = 1 THEN 'Active backlog pressure'
                    WHEN me.effective_internal_marks IS NOT NULL AND me.effective_internal_marks < 60 THEN 'Internal recovery needed'
                    ELSE 'Performance volatility'
                END AS note
            FROM marks_enriched me
            WHERE me.roll_no = :roll_no AND me.credit > 0
            ORDER BY me.failed DESC, COALESCE(me.effective_internal_marks, me.total_marks) ASC, me.total_marks ASC
            LIMIT 4
        )
        SELECT json_build_object(
            'profile', (SELECT row_to_json(sp) FROM student_profile sp),
            'domains', COALESCE((SELECT json_agg(ds) FROM domain_scores ds), '[]'::json),
            'series', COALESCE((SELECT json_agg(ss) FROM student_series ss), '[]'::json),
            'peer', (SELECT row_to_json(pb) FROM peer_benchmark pb WHERE pb.roll_no = :roll_no LIMIT 1),
            'strengths', COALESCE((SELECT json_agg(st) FROM subject_strengths st), '[]'::json),
            'support', COALESCE((SELECT json_agg(su) FROM subject_support su), '[]'::json)
        ) AS payload
        """
    )
    payload = (await db.execute(query, {"roll_no": roll_no})).scalar_one()
    profile = payload["profile"]
    if not profile:
        raise HTTPException(status_code=404, detail="Student 360 profile not found")

    gpa_velocity = float(profile["gpa_velocity"] or 0)
    gpa_trend = "Stable"
    if gpa_velocity > 0.15:
        gpa_trend = "Rising"
    elif gpa_velocity < -0.15:
        gpa_trend = "Falling"

    attendance_percentage = float(profile["attendance_percentage"])
    overall_gpa = float(profile["overall_gpa"])
    active_arrears = int(profile["active_arrears"])
    attendance_band = _attendance_band(attendance_percentage)
    placement_signal = _placement_signal(overall_gpa, active_arrears, attendance_percentage)
    raw_peer = payload.get("peer") or {
        "cohort_size": 1,
        "class_rank": 1,
        "percentile": 100.0,
        "cohort_avg_gpa": overall_gpa,
        "gap_from_cohort": 0.0,
    }
    peer = {key: value for key, value in raw_peer.items() if key != "roll_no"}
    risk_drivers = [
        schemas.StudentRiskDriver(
            label="Attendance",
            value=attendance_percentage,
            status=_tone_from_metric(attendance_percentage, 80, 75, reverse=True),
        ),
        schemas.StudentRiskDriver(
            label="Internals",
            value=float(
                (profile["attendance_marks_correlation"] * 50 + 50) 
                if profile["attendance_marks_correlation"] is not None 
                else 50
            ),
            status=(
                "positive" if profile["attendance_marks_correlation"] is not None and float(profile["attendance_marks_correlation"]) > 0.3
                else "warning" if profile["attendance_marks_correlation"] is not None and float(profile["attendance_marks_correlation"]) < -0.1
                else "neutral"
            ),
        ),
        schemas.StudentRiskDriver(
            label="GPA Velocity",
            value=round(max(min(abs(gpa_velocity) * 100, 100), 0), 2),
            status="positive" if gpa_velocity > 0.15 else "critical" if gpa_velocity < -0.25 else "warning" if gpa_velocity < 0 else "neutral",
        ),
        schemas.StudentRiskDriver(
            label="Backlog Load",
            value=float(active_arrears),
            status="critical" if active_arrears >= 2 else "warning" if active_arrears == 1 else "positive",
        ),
    ]
    recommended_actions: list[str] = []
    if attendance_percentage < 75:
        recommended_actions.append("Trigger counselor intervention for attendance recovery within 7 days.")
    if profile["attendance_marks_correlation"] is not None and float(profile["attendance_marks_correlation"]) < 0:
        recommended_actions.append("Attendance is not translating into marks. Review study strategy and internal preparation.")
    if gpa_velocity < 0:
        recommended_actions.append("GPA velocity is falling. Schedule subject-wise remediation for the next cycle.")
    if active_arrears > 0:
        recommended_actions.append(f"Clear {active_arrears} active arrears before placement readiness review.")
    if not recommended_actions:
        recommended_actions.append("Maintain momentum and shift focus to high-value coding subjects for placement readiness.")

    # Validate critical fields before returning
    if not profile.get("student_name"):
        raise HTTPException(status_code=500, detail="Student profile is incomplete: missing student name")
    
    if profile.get("current_semester") is None:
        logger.warning(f"Student {roll_no} missing current semester data")
    
    # Build and return response
    response = schemas.Student360Profile(
        roll_no=profile["roll_no"],
        reg_no=profile.get("reg_no"),
        student_name=profile["student_name"],
        batch=profile.get("batch"),
        section=profile.get("section"),  # Now properly populated!
        current_semester=profile.get("current_semester"),
        overall_gpa=overall_gpa,
        attendance_percentage=attendance_percentage,
        gpa_trend=gpa_trend,
        gpa_velocity=gpa_velocity,
        attendance_marks_correlation=profile["attendance_marks_correlation"],
        active_arrears=active_arrears,
        risk_level=_risk_level(float(profile["risk_score"])),
        attendance_band=attendance_band,
        placement_signal=placement_signal,
        skill_domains=[schemas.StudentSkillDomainScore(**row) for row in payload["domains"]],
        semester_velocity=[schemas.StudentSemesterVelocity(**row) for row in payload["series"]],
        strongest_subjects=[schemas.StudentSubjectHighlight(**row) for row in payload["strengths"]],
        support_subjects=[schemas.StudentSubjectHighlight(**row) for row in payload["support"]],
        peer_benchmark=schemas.StudentPeerBenchmark(**peer),
        risk_drivers=risk_drivers,
        recommended_actions=recommended_actions,
    )
    
    logger.info(f"Successfully generated Student 360 profile for {roll_no}")
    return response


async def get_subject_bottlenecks(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    subject_code: Optional[str],
    limit: int,
    offset: int,
    sort_by: str,
) -> schemas.SubjectBottleneckResponse:
    order_sql = {
        "failure_rate": "failure_rate DESC",
        "marks_stddev": "marks_stddev DESC",
        "avg_grade": "current_average_marks ASC",
        "student_count": "attempts DESC",
        "drift": "drift_from_history ASC",
    }.get(sort_by, "failure_rate DESC")

    # When the UI is asking for "failure clusters" (sort_by=failure_rate),
    # hide subjects with zero failures so the heatmap doesn't show 0% "High Pressure" tiles.
    having_sql = "HAVING SUM(me.failed) > 0" if sort_by == "failure_rate" and subject_code is None else ""

    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        subject_history AS (
            SELECT
                me.subject_code,
                me.subject_name,
                me.semester,
                COUNT(*) AS attempts,
                ROUND((100.0 * SUM(me.failed) / NULLIF(COUNT(*), 0))::numeric, 2) AS failure_rate,
                ROUND(COALESCE(stddev_pop(me.total_marks), 0)::numeric, 2) AS marks_stddev,
                ROUND(AVG(me.total_marks)::numeric, 2) AS current_average_marks,
                ROUND(AVG(me.total_marks) FILTER (
                    WHERE substring(coalesce(me.batch, ''), 1, 4) ~ '^[0-9]{{4}}$'
                    AND CAST(substring(me.batch, 1, 4) AS INTEGER) >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
                )::numeric, 2) AS historical_five_year_average,
                COUNT(*) OVER () AS total_count
            FROM marks_enriched me
            WHERE ({_cast_text_param('subject_code_val')} IS NULL OR lower(me.subject_code) = lower({_cast_text_param('subject_code_val')}))
              AND me.credit > 0
              AND me.subject_code NOT ILIKE '24AC%'
            GROUP BY me.subject_code, me.subject_name, me.semester
            {having_sql}
        )
        SELECT
            subject_code,
            subject_name,
            semester,
            attempts,
            failure_rate,
            marks_stddev,
            current_average_marks,
            COALESCE(historical_five_year_average, current_average_marks) AS historical_five_year_average,
            ROUND((current_average_marks - COALESCE(historical_five_year_average, current_average_marks))::numeric, 2) AS drift_from_history,
            NULL::text AS faculty_context,
            total_count
        FROM subject_history
        ORDER BY {order_sql}
        OFFSET :offset LIMIT :limit
        """
    )
    rows = (await db.execute(query, {"subject_code_val": subject_code, "limit": limit, "offset": offset})).mappings().all()
    total = int(rows[0]["total_count"]) if rows else 0
    return schemas.SubjectBottleneckResponse(
        items=[schemas.SubjectBottleneckItem(**{k: v for k, v in dict(row).items() if k != "total_count"}) for row in rows],
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset),
    )


async def get_faculty_impact_matrix(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    subject_code: Optional[str],
    faculty_id: Optional[int],
    limit: int,
    offset: int,
) -> schemas.FacultyImpactMatrixResponse:
    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        subject_baseline AS (
            SELECT
                me.subject_code,
                COALESCE(ROUND((100.0 * SUM(me.failed) / NULLIF(COUNT(*), 0))::numeric, 2), 0.0) AS subject_failure_rate
            FROM marks_enriched me
            GROUP BY me.subject_code
        ),
        faculty_matrix AS (
            SELECT
                fsa.faculty_id,
                sf.name AS faculty_name,
                sb.course_code AS subject_code,
                sb.name AS subject_name,
                COUNT(me.student_id) AS student_count,
                COALESCE(ROUND((100.0 * SUM(me.failed) / NULLIF(COUNT(me.student_id), 0))::numeric, 2), 0.0) AS failure_rate,
                sbc.subject_failure_rate,
                COALESCE(ROUND(AVG(COALESCE(me.total_marks, 0))::numeric, 2), 0.0) AS average_marks,
                COUNT(*) OVER () AS total_count
            FROM faculty_subject_assignments fsa
            JOIN staff sf ON sf.id = fsa.faculty_id
            JOIN subjects sb ON sb.id = fsa.subject_id
            LEFT JOIN marks_enriched me ON me.subject_code = sb.course_code AND (me.semester = sb.semester OR sb.semester IS NULL)
            LEFT JOIN subject_baseline sbc ON sbc.subject_code = sb.course_code
            WHERE ({_cast_int_param('faculty_id')} IS NULL OR fsa.faculty_id = {_cast_int_param('faculty_id')})
              AND ({_cast_text_param('subject_code')} IS NULL OR lower(sb.course_code) = lower({_cast_text_param('subject_code')}))
            GROUP BY fsa.faculty_id, sf.name, sb.course_code, sb.name, sbc.subject_failure_rate
        )
        SELECT
            faculty_id,
            faculty_name,
            subject_code,
            subject_name,
            student_count,
            failure_rate,
            COALESCE(subject_failure_rate, failure_rate, 0.0) AS subject_failure_rate,
            COALESCE(ROUND((failure_rate - COALESCE(subject_failure_rate, failure_rate))::numeric, 2), 0.0) AS cohort_delta,
            average_marks,
            CASE
                WHEN failure_rate <= COALESCE(subject_failure_rate, failure_rate) - 8 THEN 'High positive impact'
                WHEN failure_rate >= COALESCE(subject_failure_rate, failure_rate) + 8 THEN 'Needs cohort review'
                ELSE 'Within subject baseline'
            END AS impact_label,
            total_count
        FROM faculty_matrix
        ORDER BY cohort_delta ASC, average_marks DESC
        OFFSET :offset LIMIT :limit
        """
    )
    rows = (await db.execute(query, {"subject_code": subject_code, "faculty_id": faculty_id, "limit": limit, "offset": offset})).mappings().all()
    total = int(rows[0]["total_count"]) if rows else 0
    return schemas.FacultyImpactMatrixResponse(
        items=[schemas.FacultyImpactMatrixItem(**{k: v for k, v in dict(row).items() if k != "total_count"}) for row in rows],
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset),
    )


async def get_placement_readiness(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    cgpa_threshold: float,
    limit: int,
    offset: int,
    sort_by: str,
) -> schemas.PlacementReadinessResponse:
    order_sql = {
        "cgpa": "cgpa DESC",
        "coding_score": "coding_subject_score DESC",
        "attendance": "attendance_percentage DESC",
    }.get(sort_by, "cgpa DESC")

    coding_filter = " OR ".join([f"lower(me.subject_name) LIKE '%{pattern}%'" for pattern in CODING_PATTERNS])
    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        coding_scores AS (
            SELECT
                me.student_id,
                ROUND(AVG(CASE WHEN {coding_filter} THEN COALESCE(me.total_marks, me.internal_marks) ELSE NULL END)::numeric, 2) AS coding_subject_score
            FROM marks_enriched me
            GROUP BY me.student_id
        ),
        placement_candidates AS (
            SELECT
                sc.roll_no,
                sc.student_name,
                sc.batch,
                sc.current_semester,
                sc.cgpa_proxy AS cgpa,
                sc.active_arrears,
                COALESCE(cs.coding_subject_score, 0) AS coding_subject_score,
                sc.attendance_percentage,
                (sc.cgpa_proxy >= :cgpa_threshold AND sc.active_arrears = 0 AND COALESCE(cs.coding_subject_score, 0) >= 65) AS placement_ready,
                COUNT(*) OVER () AS total_count
            FROM student_current sc
            LEFT JOIN coding_scores cs ON cs.student_id = sc.student_id
            WHERE sc.cgpa_proxy >= :cgpa_threshold
        )
        SELECT * FROM placement_candidates
        ORDER BY {order_sql}
        OFFSET :offset LIMIT :limit
        """
    )
    rows = (await db.execute(query, {"cgpa_threshold": cgpa_threshold, "limit": limit, "offset": offset})).mappings().all()
    total = int(rows[0]["total_count"]) if rows else 0
    return schemas.PlacementReadinessResponse(
        items=[schemas.PlacementCandidate(**{k: v for k, v in dict(row).items() if k != "total_count"}) for row in rows],
        pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset),
    )


async def spotlight_search(db: AsyncSession, *, query: str, limit: int = 8) -> schemas.SpotlightSearchResponse:
    sql = text(
        """
        SELECT * FROM (
            SELECT 'student' AS entity_type, st.roll_no AS entity_id, st.name AS label, concat(coalesce(st.batch, 'No batch'), ' | Sem ', coalesce(st.current_semester, 0)) AS sublabel
            FROM students st
            WHERE st.name ILIKE :pattern OR st.roll_no ILIKE :pattern
            UNION ALL
            SELECT 'faculty' AS entity_type, CAST(sf.id AS TEXT) AS entity_id, sf.name AS label, sf.department AS sublabel
            FROM staff sf
            WHERE sf.name ILIKE :pattern
            UNION ALL
            SELECT 'subject' AS entity_type, sb.course_code AS entity_id, sb.name AS label, concat('Sem ', coalesce(sb.semester, 0)) AS sublabel
            FROM subjects sb
            WHERE sb.name ILIKE :pattern OR sb.course_code ILIKE :pattern
        ) q
        LIMIT :limit
        """
    )
    rows = (await db.execute(sql, {"pattern": f"%{query}%", "limit": limit})).mappings().all()
    return schemas.SpotlightSearchResponse(results=[schemas.SpotlightResult(**dict(row)) for row in rows])


async def _get_admin_directory_rollup(db: AsyncSession, curriculum_credits: dict[str, float]) -> list[schemas.AdminDirectoryStudent]:
    query = text(
        _base_ctes(curriculum_credits)
        + """
        SELECT
            sc.roll_no,
            sc.reg_no,
            sc.student_name AS name,
            sc.city,
            sc.email,
            NULL::text AS phone_primary,
            sc.batch,
            sc.section,
            sc.current_semester,
            0 AS marks_count,
            0 AS attendance_count,
            sc.attendance_percentage,
            COALESCE(sc.cgpa_proxy, 0) AS average_grade_points,
            COALESCE(sc.internal_avg, 0) AS average_internal_percentage,
            sc.active_arrears AS backlogs,
            sc.is_initial_password,
            RANK() OVER (ORDER BY COALESCE(sc.cgpa_proxy, 0) DESC, COALESCE(sc.attendance_percentage, 0) DESC) AS rank
        FROM student_current sc
        ORDER BY rank, sc.roll_no
        """
    )
    rows = (await db.execute(query)).mappings().all()
    return [schemas.AdminDirectoryStudent(**dict(row)) for row in rows]


async def _get_batch_health(db: AsyncSession, curriculum_credits: dict[str, float]) -> list[dict]:
    query = text(
        _base_ctes(curriculum_credits)
        + """
        SELECT
            COALESCE(batch, 'Unknown') AS batch,
            ROUND(AVG(cgpa_proxy)::numeric, 2) AS avg_gpa,
            ROUND(AVG(attendance_percentage)::numeric, 2) AS avg_attendance,
            SUM(CASE WHEN active_arrears > 0 THEN 1 ELSE 0 END) AS backlog_students,
            COUNT(*) AS total_students
        FROM student_current
        GROUP BY COALESCE(batch, 'Unknown')
        ORDER BY avg_gpa DESC, avg_attendance DESC
        """
    )
    return [
        {
            "batch": row["batch"],
            "average_gpa": float(row["avg_gpa"] or 0),
            "average_attendance": float(row["avg_attendance"] or 0),
            "at_risk_count": int(row["backlog_students"] or 0),
            "total_students": int(row["total_students"] or 0)
        }
        for row in (await db.execute(query)).mappings().all()
    ]



async def _get_semester_pulse(db: AsyncSession, curriculum_credits: dict[str, float]) -> list[dict]:
    query = text(
        _base_ctes(curriculum_credits)
        + """
        SELECT
            current_semester AS semester,
            COUNT(*) AS students,
            ROUND(AVG(cgpa_proxy)::numeric, 2) AS avg_gpa,
            ROUND(AVG(attendance_percentage)::numeric, 2) AS avg_attendance,
            SUM(CASE WHEN active_arrears > 0 THEN 1 ELSE 0 END) AS backlog_students
        FROM student_current
        GROUP BY current_semester
        ORDER BY current_semester
        """
    )
    return [
        {
            "semester": int(row["semester"]),
            "average_gpa": float(row["avg_gpa"] or 0),
            "average_attendance": float(row["avg_attendance"] or 0),
            "student_count": int(row["students"] or 0),
            "at_risk_count": int(row["backlog_students"] or 0)
        }
        for row in (await db.execute(query)).mappings().all()
    ]



async def _get_risk_summary(db: AsyncSession, curriculum_credits: dict[str, float]) -> schemas.AdminRiskSummary:
    query = text(
        _base_ctes(curriculum_credits)
        + """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS critical,
            SUM(CASE WHEN risk_score >= 55 AND risk_score < 70 THEN 1 ELSE 0 END) AS high,
            SUM(CASE WHEN risk_score >= 35 AND risk_score < 55 THEN 1 ELSE 0 END) AS moderate,
            SUM(CASE WHEN risk_score < 35 THEN 1 ELSE 0 END) AS low
        FROM risk_scores
        """
    )
    row = (await db.execute(query)).mappings().one()
    return schemas.AdminRiskSummary(**{key: int(row[key] or 0) for key in row.keys()})


async def _get_placement_summary(db: AsyncSession, curriculum_credits: dict[str, float]) -> schemas.AdminPlacementSummary:
    coding_patterns = "|".join(CODING_PATTERNS)
    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        ,
        coding_scores AS (
            SELECT
                me.student_id,
                ROUND(AVG(CASE WHEN me.subject_name ~* :coding_patterns THEN COALESCE(me.total_marks, me.internal_marks) ELSE NULL END)::numeric, 2) AS coding_subject_score
            FROM marks_enriched me
            GROUP BY me.student_id
        )
        SELECT
            SUM(CASE WHEN sc.cgpa_proxy >= 7 AND sc.active_arrears = 0 AND COALESCE(cs.coding_subject_score, 0) >= 65 THEN 1 ELSE 0 END) AS ready_count,
            SUM(CASE WHEN sc.cgpa_proxy >= 6 AND sc.active_arrears <= 1 AND COALESCE(cs.coding_subject_score, 0) >= 55 
                     AND NOT (sc.active_arrears > 1 OR sc.cgpa_proxy < 6) THEN 1 ELSE 0 END) AS almost_ready_count,
            SUM(CASE WHEN sc.active_arrears > 1 OR sc.cgpa_proxy < 6 THEN 1 ELSE 0 END) AS blocked_count,
            ROUND(AVG(COALESCE(cs.coding_subject_score, 0))::numeric, 2) AS avg_coding_score
        FROM student_current sc
        LEFT JOIN coding_scores cs ON cs.student_id = sc.student_id
        """
    )
    row = (await db.execute(query, {"coding_patterns": coding_patterns})).mappings().one()
    return schemas.AdminPlacementSummary(
        ready_count=int(row["ready_count"] or 0),
        almost_ready_count=int(row["almost_ready_count"] or 0),
        blocked_count=int(row["blocked_count"] or 0),
        avg_coding_score=float(row["avg_coding_score"] or 0),
    )


async def _get_leaderboard_snapshots(db: AsyncSession, curriculum_credits: dict[str, float]) -> list[schemas.AdminLeaderboardSnapshot]:
    query = text(
        _base_ctes(curriculum_credits)
        + """
        ,
        ranked_subjects AS (
            SELECT
                me.subject_code,
                me.subject_name,
                me.current_semester,
                COUNT(*) AS attempts,
                COALESCE(ROUND(MAX(me.total_marks)::numeric, 2), 0.0) AS top_score,
                COALESCE(ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY me.total_marks)::numeric, 2), 0.0) AS median_score,
                COALESCE(ROUND((MAX(me.total_marks) - MIN(me.total_marks))::numeric, 2), 0.0) AS score_spread
            FROM marks_enriched me
            WHERE me.total_marks IS NOT NULL
            GROUP BY me.subject_code, me.subject_name, me.current_semester
        )
        SELECT
            subject_code,
            subject_name,
            current_semester AS semester,
            attempts,
            top_score,
            median_score,
            score_spread
        FROM ranked_subjects
        ORDER BY attempts DESC, score_spread DESC, top_score DESC
        LIMIT 8
        """
    )
    rows = (await db.execute(query)).mappings().all()
    return [schemas.AdminLeaderboardSnapshot(**dict(row)) for row in rows]


async def _get_subject_coverage(db: AsyncSession) -> list[schemas.AdminSubjectCoverage]:
    query = text(
        """
        SELECT
            s.semester,
            COUNT(*) AS total_subjects,
            SUM(CASE WHEN COALESCE(record_counts.records, 0) > 0 THEN 1 ELSE 0 END) AS ranked_subjects,
            SUM(COALESCE(record_counts.records, 0)) AS total_records
        FROM subjects s
        LEFT JOIN (
            SELECT 
                sa.subject_id, 
                COUNT(*) AS records
            FROM student_assessments sa
            WHERE sa.marks IS NOT NULL
            GROUP BY sa.subject_id
        ) record_counts ON record_counts.subject_id = s.id
        WHERE s.semester IS NOT NULL AND s.semester >= 1
        GROUP BY s.semester
        ORDER BY s.semester
        """
    )
    rows = (await db.execute(query)).mappings().all()
    return [schemas.AdminSubjectCoverage(**{key: int(row[key] or 0) for key in row.keys()}) for row in rows]


async def get_command_center(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    spotlight: str = "",
) -> schemas.AdminCommandCenterResponse:
    from .analytics_service import build_hod_dashboard

    dashboard = await build_hod_dashboard(db, curriculum_credits)
    # Patch: propagate new analytics fields to top-level response for admin dashboard
    # If dashboard.directory or dashboard.risk_students contain per-subject marks, map new fields
    # (Assume downstream consumers expect these fields in marks or subject performance lists)
    for student in getattr(dashboard, 'directory', []):
        if hasattr(student, 'marks'):
            for mark in student.marks:
                mark.percentile = getattr(mark, 'percentile', None)
                mark.normalized_score = getattr(mark, 'normalized_score', None)
                mark.performance_label = getattr(mark, 'performance_label', None)
    for student in getattr(dashboard, 'risk_students', []):
        if hasattr(student, 'marks'):
            for mark in student.marks:
                mark.percentile = getattr(mark, 'percentile', None)
                mark.normalized_score = getattr(mark, 'normalized_score', None)
                mark.performance_label = getattr(mark, 'performance_label', None)
    directory = await _get_admin_directory_rollup(db, curriculum_credits)
    subject_catalog = await get_subject_catalog(db)
    bottlenecks = await get_subject_bottlenecks(db, curriculum_credits, subject_code=None, limit=6, offset=0, sort_by="failure_rate")
    faculty = await get_faculty_impact_matrix(db, curriculum_credits, subject_code=None, faculty_id=None, limit=6, offset=0)
    placements = await get_placement_readiness(db, curriculum_credits, cgpa_threshold=6.5, limit=8, offset=0, sort_by="cgpa")
    watchlist = await get_risk_registry(db, curriculum_credits, risk_level=None, limit=8, offset=0, sort_by="risk_score")
    spotlight_results = await spotlight_search(db, query=spotlight, limit=8) if spotlight else schemas.SpotlightSearchResponse(results=[])
    batch_health = await _get_batch_health(db, curriculum_credits)
    semester_pulse = await _get_semester_pulse(db, curriculum_credits)
    risk_summary = await _get_risk_summary(db, curriculum_credits)
    placement_summary = await _get_placement_summary(db, curriculum_credits)
    leaderboard_snapshots = await _get_leaderboard_snapshots(db, curriculum_credits)
    subject_coverage = await _get_subject_coverage(db)

    alerts: list[str] = []
    for student in dashboard.risk_students[:4]:
        if student.risk_score >= 70 or student.gpa_drop_factor >= 1.5:
            alerts.append(f"{student.name} entered red zone with risk {student.risk_score}.")
    for subject in bottlenecks.items[:2]:
        if subject.drift_from_history is not None:
            alerts.append(f"{subject.subject_code} is trending {abs(subject.drift_from_history)} marks below its five-year baseline.")

    top_performers = sorted(directory, key=lambda item: ((item.average_grade_points or 0), (item.attendance_percentage or 0)), reverse=True)[:8]
    attendance_defaulters = sorted(directory, key=lambda item: (item.attendance_percentage or 0))[:8]
    internal_defaulters = sorted(directory, key=lambda item: (item.average_internal_percentage or 0))[:8]
    backlog_clusters = sorted([item for item in directory if (item.backlogs or 0) > 0], key=lambda item: (-(item.backlogs or 0), (item.average_grade_points or 0)))[:8]
    opportunity_students = sorted(
        [item for item in directory if (item.attendance_percentage or 0) >= 85 and (item.average_grade_points or 0) < 7],
        key=lambda item: ((item.average_grade_points or 0), -(item.attendance_percentage or 0)),
    )[:8]
    quick_actions = [
        "Open Student 360 for every red-zone student before counselor review.",
        "Use the semester-filtered leaderboard to compare top and bottom performers paper-by-paper.",
        "Export the batch summary before placement committee meetings.",
        "Map faculty assignments to unlock cohort-level teaching impact analysis.",
        "Review attendance defaulters and internal defaulters together to prioritize intervention."
    ]
    action_queue = [
        schemas.AdminCohortAction(
            title="Critical-risk sweep",
            detail="Start with red-zone students before moving to moderate watchlist cases.",
            metric=f"{risk_summary.critical} critical students",
            tone="critical" if risk_summary.critical else "info",
        ),
        schemas.AdminCohortAction(
            title="Placement pipeline",
            detail="Students who are close to placement-ready should get coding-subject mentoring first.",
            metric=f"{placement_summary.almost_ready_count} almost ready",
            tone="warning" if placement_summary.almost_ready_count else "positive",
        ),
        schemas.AdminCohortAction(
            title="Attendance rescue",
            detail="Combine attendance defaulters with support-subject cases for the fastest intervention ROI.",
            metric=f"{len(attendance_defaulters)} surfaced students",
            tone="warning" if attendance_defaulters else "info",
        ),
        schemas.AdminCohortAction(
            title="Subject bottlenecks",
            detail="Review the hardest subjects with drift below recent baseline before internal reviews.",
            metric=f"{len([item for item in bottlenecks.items if item.drift_from_history is not None and item.drift_from_history < 0])} drifting subjects",
            tone="critical" if any(item.drift_from_history is not None and item.drift_from_history < -5 for item in bottlenecks.items) else "warning",
        ),
    ]

    return schemas.AdminCommandCenterResponse(
        daily_briefing=dashboard.daily_briefing,
        department_health=dashboard.department_health,
        alerts=alerts,
        bottlenecks=bottlenecks.items,
        faculty_impact=faculty.items,
        placement_ready=placements.items,
        spotlight_results=spotlight_results.results,
        top_performers=top_performers,
        attendance_defaulters=attendance_defaulters,
        internal_defaulters=internal_defaulters,
        backlog_clusters=backlog_clusters,
        opportunity_students=opportunity_students,
        watchlist_students=watchlist.items,
        batch_health=batch_health,
        semester_pulse=semester_pulse,
        risk_summary=risk_summary,
        placement_summary=placement_summary,
        leaderboard_snapshots=leaderboard_snapshots,
        subject_coverage=subject_coverage,
        action_queue=action_queue,
        quick_actions=quick_actions,
        subject_catalog=subject_catalog,
    )


async def get_risk_registry(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
    *,
    risk_level: Optional[str],
    limit: int,
    offset: int,
    sort_by: str,
) -> schemas.RiskRegistryResponse:
    order_sql = {
        "risk_score": "risk_score DESC",
        "attendance": "attendance_percentage ASC",
        "gpa_velocity": "gpa_velocity ASC",
    }.get(sort_by, "risk_score DESC")
    query = text(
        _base_ctes(curriculum_credits)
        + f"""
        SELECT
            roll_no,
            student_name,
            risk_score,
            CASE 
                WHEN risk_score >= 70 THEN 'Critical'
                WHEN risk_score >= 55 THEN 'High'
                WHEN risk_score >= 35 THEN 'Moderate'
                ELSE 'Low'
            END AS risk_level,
            attendance_percentage,
            internal_avg,
            ROUND(GREATEST(0, previous_sgpa - cgpa_proxy)::numeric, 2) AS gpa_drop_factor,
            gpa_velocity,
            COUNT(*) OVER () AS total_count
        FROM risk_scores
        WHERE (CAST(:risk_level AS TEXT) IS NULL)
           OR (CAST(:risk_level AS TEXT) = 'Critical' AND risk_score >= 70)
           OR (CAST(:risk_level AS TEXT) = 'High' AND risk_score >= 55 AND risk_score < 70)
           OR (CAST(:risk_level AS TEXT) = 'Moderate' AND risk_score >= 35 AND risk_score < 55)
           OR (CAST(:risk_level AS TEXT) = 'Low' AND risk_score < 35)
        ORDER BY {order_sql}
        OFFSET :offset LIMIT :limit
        """
    )
    rows = (await db.execute(query, {"risk_level": risk_level, "limit": limit, "offset": offset})).mappings().all()
    total = int(rows[0]["total_count"]) if rows else 0
    items = []
    for row in rows:
        risk_score = float(row["risk_score"])
        alerts = []
        if float(row["attendance_percentage"]) < 75:
            alerts.append(f"Attendance at {row['attendance_percentage']}%")
        if row["internal_avg"] is not None and float(row["internal_avg"]) < 60:
            alerts.append(f"Internals at {row['internal_avg']}%")
        if float(row["gpa_drop_factor"]) > 0.5:
            alerts.append(f"GPA drop {row['gpa_drop_factor']}")
        items.append(
            schemas.StudentRiskScore(
                roll_no=row["roll_no"],
                name=row["student_name"],
                risk_score=risk_score,
                attendance_factor=float(row["attendance_percentage"]),
                internal_marks_factor=float(row["internal_avg"] or 0),
                gpa_drop_factor=float(row["gpa_drop_factor"]),
                is_at_risk=risk_score >= 55,
                risk_level=_risk_level(risk_score),
                alerts=alerts or ["Monitoring recommended"],
            )
        )
    return schemas.RiskRegistryResponse(items=items, pagination=schemas.PaginationMeta(total=total, limit=limit, offset=offset))


async def export_batch_summary_xlsx(db: AsyncSession, curriculum_credits: dict[str, float], *, cgpa_threshold: float) -> StreamingResponse:
    from openpyxl import Workbook

    placements = await get_placement_readiness(db, curriculum_credits, cgpa_threshold=cgpa_threshold, limit=500, offset=0, sort_by="cgpa")
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Placement Readiness"
    sheet.append(["Roll No", "Student Name", "Batch", "Semester", "CGPA", "Arrears", "Coding Score", "Attendance", "Ready"])
    for item in placements.items:
        sheet.append([
            item.roll_no,
            item.student_name,
            item.batch,
            item.current_semester,
            item.cgpa,
            item.active_arrears,
            item.coding_subject_score,
            item.attendance_percentage,
            "Yes" if item.placement_ready else "No",
        ])

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="mca-batch-summary.xlsx"'},
    )


async def export_student_grade_sheet_pdf(db: AsyncSession, curriculum_credits: dict[str, float], *, roll_no: str) -> StreamingResponse:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    profile = await get_student_360(db, curriculum_credits, roll_no=roll_no)
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    pdf.setTitle(f"Grade Sheet {profile.roll_no}")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(20 * mm, height - 20 * mm, "MCA Grade Sheet")
    pdf.setFont("Helvetica", 11)
    lines = [
        f"Student: {profile.student_name}",
        f"Roll No: {profile.roll_no}",
        f"Batch: {profile.batch or '-'}",
        f"Current Semester: {profile.current_semester or '-'}",
        f"Overall GPA: {profile.overall_gpa}",
        f"Attendance: {profile.attendance_percentage}%",
        f"GPA Trend: {profile.gpa_trend}",
        f"Active Arrears: {profile.active_arrears}",
    ]
    y = height - 35 * mm
    for line in lines:
        pdf.drawString(20 * mm, y, line)
        y -= 8 * mm

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(20 * mm, y - 4 * mm, "Semester Velocity")
    y -= 14 * mm
    pdf.setFont("Helvetica", 10)
    for item in profile.semester_velocity:
        pdf.drawString(
            20 * mm,
            y,
            f"Sem {item.semester}: SGPA {item.sgpa} | Velocity {item.velocity if item.velocity is not None else '-'} | Attendance {item.attendance_pct}%",
        )
        y -= 7 * mm
        if y < 20 * mm:
            pdf.showPage()
            y = height - 20 * mm
    pdf.save()
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{roll_no}-grade-sheet.pdf"'})


async def export_student_resume_pdf(db: AsyncSession, curriculum_credits: dict[str, float], *, roll_no: str) -> StreamingResponse:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import Paragraph, Table, TableStyle, Spacer

    # Strip extension if present
    clean_roll_no = roll_no.split(".")[0]
    profile = await get_student_360(db, curriculum_credits, roll_no=clean_roll_no)
    
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    # helper for drawing lines
    def draw_section_line(y_pos):
        pdf.setStrokeColor(colors.lightgrey)
        pdf.setLineWidth(0.5)
        pdf.line(20 * mm, y_pos, width - 20 * mm, y_pos)

    # 1. Header
    pdf.setFont("Helvetica-Bold", 24)
    pdf.setFillColor(colors.HexColor("#1e293b"))  # Slate 800
    pdf.drawString(20 * mm, height - 25 * mm, profile.student_name.upper())
    
    pdf.setFont("Helvetica", 12)
    pdf.setFillColor(colors.HexColor("#64748b"))  # Slate 500
    pdf.drawString(20 * mm, height - 32 * mm, f"Roll No: {profile.roll_no} | Batch: {profile.batch or 'N/A'} | Sem: {profile.current_semester or 'N/A'}")
    
    y = height - 40 * mm
    draw_section_line(y)
    y -= 10 * mm

    # 2. Academic Summary
    pdf.setFont("Helvetica-Bold", 14)
    pdf.setFillColor(colors.HexColor("#0f172a")) # Slate 900
    pdf.drawString(20 * mm, y, "ACADEMIC SUMMARY")
    y -= 8 * mm
    
    summary_data = [
        ["Overall CGPA", f"{profile.overall_gpa:.2f} / 10.0"],
        ["Attendance", f"{profile.attendance_percentage:.1f}% ({profile.attendance_band})"],
        ["Active Arrears", str(profile.active_arrears)],
        ["Placement Signal", profile.placement_signal]
    ]
    
    table = Table(summary_data, colWidths=[50 * mm, 100 * mm])
    table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,-1), 11),
        ('TEXTCOLOR', (0,0), (0,-1), colors.HexColor("#475569")),
        ('TEXTCOLOR', (1,0), (1,-1), colors.HexColor("#1e293b")),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    table.wrapOn(pdf, width, height)
    table.drawOn(pdf, 20 * mm, y - (len(summary_data) * 8 * mm))
    
    y -= (len(summary_data) * 8 * mm) + 10 * mm
    draw_section_line(y)
    y -= 10 * mm

    # 3. Performance Trends
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(20 * mm, y, "SEMESTER-WISE PERFORMANCE")
    y -= 8 * mm
    
    trend_data = [["Semester", "SGPA", "Attendance", "Internal Avg"]]
    for sv in profile.semester_velocity:
        trend_data.append([
            f"Semester {sv.semester}",
            f"{sv.sgpa:.2f}",
            f"{sv.attendance_pct}%",
            f"{sv.internal_avg:.1f}%"
        ])
    
    table = Table(trend_data, colWidths=[35 * mm, 35 * mm, 35 * mm, 35 * mm])
    table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 10),
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#f8fafc")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.HexColor("#475569")),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('FONTNAME', (0,1), (-1,-1), 'Helvetica'),
        ('FONTSIZE', (0,1), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    table.wrapOn(pdf, width, height)
    table.drawOn(pdf, 20 * mm, y - (len(trend_data) * 8 * mm))
    
    y -= (len(trend_data) * 8 * mm) + 15 * mm
    
    # 4. Key Strengths & Skills
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(20 * mm, y, "TECHNICAL STRENGTHS & SKILL DOMAINS")
    y -= 8 * mm
    
    # Skill Domains
    skills_text = ", ".join([f"{sd.domain} ({sd.score:.0f}%)" for sd in profile.skill_domains])
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(20 * mm, y, "Skill Domains:")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(50 * mm, y, skills_text[:80] + ("..." if len(skills_text) > 80 else ""))
    y -= 8 * mm
    
    # Strongest Subjects
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(20 * mm, y, "Top Subjects:")
    pdf.setFont("Helvetica", 11)
    top_subs = ", ".join([s.subject_name for s in profile.strongest_subjects[:3]])
    pdf.drawString(50 * mm, y, top_subs)
    
    y -= 20 * mm
    draw_section_line(y)
    y -= 10 * mm
    
    # 5. Risk Assessment (Brief)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(20 * mm, y, "ACADEMIC STANDING")
    y -= 8 * mm
    pdf.setFont("Helvetica", 11)
    status = "Consistent Performer" if profile.risk_level == "Low" else f"Academic Risk: {profile.risk_level}"
    pdf.drawString(20 * mm, y, f"Current Status: {status}")
    y -= 6 * mm
    pdf.setFont("Helvetica-Oblique", 10)
    pdf.setFillColor(colors.HexColor("#64748b"))
    pdf.drawString(20 * mm, y, f"Generated by SPARK Intelligence on {time.strftime('%Y-%m-%d %H:%M:%S')}")

    pdf.save()
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{clean_roll_no}-resume.pdf"'})
