from __future__ import annotations

from typing import Optional, List, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .. import schemas
from ..utils.academic_calculations import (
    best_2_of_3_cits_null_check_sql,
    best_2_of_3_cits_with_fallback_sql,
    grade_point_from_grade_or_marks_sql,
)
from .admin_service import AdminService


class RankingService:
    """
    Centralized service for all student ranking calculations based on CGPA.
    Provides consistent ranking logic across the application.
    """

    @staticmethod
    async def get_student_rank_by_cgpa(
        db: AsyncSession,
        roll_no: str,
        curriculum_credits: dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific student's overall rank based on CGPA and detailed ranking info.
        
        Returns:
            Dict with keys: rank, cgpa, attendance_percentage, percentile, total_students
            None if student not found
        """
        credits_values = ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())
        
        ranking_query = text(f'''
            WITH directory_with_ranks AS (
                SELECT 
                    roll_no,
                    name,
                    average_grade_points,
                    average_internal_percentage,
                    attendance_percentage,
                    backlogs,
                    DENSE_RANK() OVER (
                        ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                    ) AS rank,
                    COUNT(*) OVER () AS total_students,
                    ROUND(
                        ((1 - PERCENT_RANK() OVER (
                            ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                        )) * 100)::numeric,
                        2
                    ) AS percentile
                FROM (
                    {AdminService._admin_directory_query_text(credits_values)}
                ) directory
            )
            SELECT 
                rank,
                average_grade_points AS cgpa,
                attendance_percentage,
                percentile,
                total_students,
                name,
                backlogs
            FROM directory_with_ranks 
            WHERE roll_no = :roll_no
        ''')
        
        result = await db.execute(ranking_query, {'roll_no': roll_no})
        row = result.mappings().first()
        
        if row:
            return {
                'rank': row['rank'],
                'cgpa': float(row['cgpa']) if row['cgpa'] else 0.0,
                'attendance_percentage': float(row['attendance_percentage']) if row['attendance_percentage'] else 0.0,
                'percentile': float(row['percentile']) if row['percentile'] else 0.0,
                'total_students': row['total_students'],
                'name': row['name'],
                'backlogs': row['backlogs'] or 0
            }
        return None

    @staticmethod
    async def get_batch_rankings(
        db: AsyncSession,
        batch: str,
        curriculum_credits: dict[str, float],
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get all students in a specific batch ranked by CGPA.
        
        Returns:
            Dict with keys: batch, total_students, rankings (list), has_more
        """
        credits_values = ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())
        
        ranking_query = text(f'''
            WITH batch_directory AS (
                SELECT *
                FROM (
                    {AdminService._admin_directory_query_text(credits_values)}
                ) directory
                WHERE batch = :batch
            ),
            batch_rankings AS (
                SELECT 
                    roll_no,
                    reg_no,
                    name,
                    batch,
                    current_semester,
                    section,
                    average_grade_points AS cgpa,
                    attendance_percentage,
                    backlogs,
                    DENSE_RANK() OVER (
                        ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                    ) AS rank,
                    COUNT(*) OVER () AS total_students,
                    ROUND(
                        ((1 - PERCENT_RANK() OVER (
                            ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                        )) * 100)::numeric,
                        2
                    ) AS percentile
                FROM batch_directory
            )
            SELECT *
            FROM batch_rankings
            ORDER BY rank
            LIMIT :limit OFFSET :offset
        ''')
        
        result = await db.execute(ranking_query, {
            'batch': batch,
            'limit': limit,
            'offset': offset
        })
        
        rankings = []
        total_students = 0
        
        for row in result.mappings():
            total_students = row['total_students']
            rankings.append({
                'roll_no': row['roll_no'],
                'reg_no': row['reg_no'],
                'name': row['name'],
                'batch': row['batch'],
                'current_semester': row['current_semester'],
                'section': row['section'],
                'cgpa': float(row['cgpa']) if row['cgpa'] else 0.0,
                'attendance_percentage': float(row['attendance_percentage']) if row['attendance_percentage'] else 0.0,
                'backlogs': row['backlogs'] or 0,
                'rank': row['rank'],
                'percentile': float(row['percentile']) if row['percentile'] else 0.0
            })
        
        return {
            'batch': batch,
            'total_students': total_students,
            'rankings': rankings,
            'has_more': len(rankings) == limit and (offset + limit) < total_students
        }

    @staticmethod
    async def get_semester_rankings(
        db: AsyncSession,
        semester: int,
        curriculum_credits: dict[str, float],
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get all students in a specific semester ranked by SGPA (Semester GPA).
        
        Returns:
            Dict with keys: semester, total_students, rankings (list), has_more
        """
        credits_values = ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())
        
        ranking_query = text(f'''
            WITH curriculum_credits_map AS (
                SELECT * FROM (VALUES {credits_values}) AS t(course_code, credit)
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
                    MAX(v.grade) FILTER (WHERE v.assessment_type = 'SEMESTER_EXAM') AS sem_grade,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'LAB') AS lab,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'PROJECT') AS project
                FROM student_assessments v
                WHERE v.is_final = true AND v.semester = :semester
                GROUP BY v.student_id, v.subject_id, v.semester
            ),
            semester_marks AS (
                SELECT
                    st.id AS student_id,
                    st.roll_no,
                    st.reg_no,
                    st.name,
                    st.batch,
                    st.current_semester,
                    st.section,
                    mp.sem_grade,
                    sb.course_code,
                    sb.name AS subject_name,
                    CASE 
                        WHEN sb.course_code LIKE '24AC%' THEN 0.0
                        ELSE COALESCE(NULLIF(sb.credits, 0), ccm.credit, 0)
                    END AS credit,
                    CASE
                        WHEN COALESCE(mp.sem_exam, mp.lab, mp.project) IS NULL
                             AND ({best_2_of_3_cits_null_check_sql().strip()}) IS NULL
                        THEN NULL
                        ELSE ({best_2_of_3_cits_with_fallback_sql().strip()})
                             + COALESCE(COALESCE(mp.sem_exam, mp.lab, mp.project), 0)
                    END AS total_marks
                FROM marks_pivot mp
                JOIN students st ON st.id = mp.student_id
                JOIN subjects sb ON sb.id = mp.subject_id
                LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = sb.course_code
                WHERE mp.semester = :semester
            ),
            semester_gpa AS (
                SELECT
                    student_id,
                    roll_no,
                    reg_no,
                    name,
                    batch,
                    current_semester,
                    section,
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
                    )::numeric AS sgpa_sort,
                    ROUND(
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
                        )::numeric,
                        2
                    ) AS sgpa,
                    ROUND(AVG(total_marks) FILTER (WHERE total_marks IS NOT NULL)::numeric, 2) AS avg_marks,
                    COUNT(*) FILTER (
                        WHERE ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) IS NOT NULL
                    ) AS subjects_attempted,
                    COUNT(*) FILTER (
                        WHERE course_code NOT LIKE '24AC%'
                          AND (
                            (NULLIF(trim(coalesce(sem_grade, '')), '') IS NOT NULL AND upper(coalesce(sem_grade, '')) IN ('U', 'F', 'FAIL', 'RA', 'AB', 'ABSENT', 'WH'))
                            OR (total_marks < 50 AND total_marks IS NOT NULL)
                          )
                    ) AS failed_subjects
                FROM semester_marks
                GROUP BY student_id, roll_no, reg_no, name, batch, current_semester, section
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
            semester_rankings AS (
                SELECT 
                    sg.roll_no,
                    sg.reg_no,
                    sg.name,
                    sg.batch,
                    sg.current_semester,
                    sg.section,
                    sg.sgpa,
                    sg.avg_marks,
                    sg.subjects_attempted,
                    sg.failed_subjects,
                    COALESCE(ar.attendance_percentage, 0) AS attendance_percentage,
                    DENSE_RANK() OVER (
                        ORDER BY
                            sg.sgpa_sort DESC,
                            sg.failed_subjects ASC,
                            sg.avg_marks DESC,
                            COALESCE(ar.attendance_percentage, 0) DESC,
                            sg.roll_no ASC
                    ) AS rank,
                    COUNT(*) OVER () AS total_students,
                    ROUND(
                        ((1 - PERCENT_RANK() OVER (
                            ORDER BY
                                sg.sgpa_sort DESC,
                                sg.failed_subjects ASC,
                                sg.avg_marks DESC,
                                COALESCE(ar.attendance_percentage, 0) DESC,
                                sg.roll_no ASC
                        )) * 100)::numeric,
                        2
                    ) AS percentile
                FROM semester_gpa sg
                LEFT JOIN attendance_rollup ar ON ar.student_id = sg.student_id
                WHERE sg.subjects_attempted > 0  -- Only include students with actual grades
            )
            SELECT *
            FROM semester_rankings
            ORDER BY rank
            LIMIT :limit OFFSET :offset
        ''')
        
        result = await db.execute(ranking_query, {
            'semester': semester,
            'limit': limit,
            'offset': offset
        })
        
        rankings = []
        total_students = 0
        
        for row in result.mappings():
            total_students = row['total_students']
            rankings.append({
                'roll_no': row['roll_no'],
                'reg_no': row['reg_no'],
                'name': row['name'],
                'batch': row['batch'],
                'current_semester': row['current_semester'],
                'section': row['section'],
                'cgpa': float(row['sgpa']) if row['sgpa'] else 0.0,  # Using SGPA for semester ranking
                'attendance_percentage': float(row['attendance_percentage']) if row['attendance_percentage'] else 0.0,
                'backlogs': row['failed_subjects'] or 0,  # Using failed subjects in this semester
                'rank': row['rank'],
                'percentile': float(row['percentile']) if row['percentile'] else 0.0,
                'avg_marks': float(row['avg_marks']) if row['avg_marks'] else 0.0,
                'subjects_attempted': row['subjects_attempted'] or 0
            })
        
        return {
            'semester': semester,
            'total_students': total_students,
            'rankings': rankings,
            'has_more': len(rankings) == limit and (offset + limit) < total_students
        }

    @staticmethod
    async def get_semester_batch_rankings(
        db: AsyncSession,
        semester: int,
        batch: str,
        curriculum_credits: dict[str, float],
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get students in a specific semester and batch ranked by SGPA.
        This is often more useful than semester-only rankings.
        
        Returns:
            Dict with keys: semester, batch, total_students, rankings (list), has_more
        """
        credits_values = ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())
        
        ranking_query = text(f'''
            WITH curriculum_credits_map AS (
                SELECT * FROM (VALUES {credits_values}) AS t(course_code, credit)
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
                    MAX(v.grade) FILTER (WHERE v.assessment_type = 'SEMESTER_EXAM') AS sem_grade,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'LAB') AS lab,
                    MAX(v.marks) FILTER (WHERE v.assessment_type = 'PROJECT') AS project
                FROM student_assessments v
                WHERE v.is_final = true AND v.semester = :semester
                GROUP BY v.student_id, v.subject_id, v.semester
            ),
            semester_marks AS (
                SELECT
                    st.id AS student_id,
                    st.roll_no,
                    st.reg_no,
                    st.name,
                    st.batch,
                    st.current_semester,
                    st.section,
                    mp.sem_grade,
                    sb.course_code,
                    sb.name AS subject_name,
                    CASE 
                        WHEN sb.course_code LIKE '24AC%' THEN 0.0
                        ELSE COALESCE(NULLIF(sb.credits, 0), ccm.credit, 0)
                    END AS credit,
                    CASE
                        WHEN COALESCE(mp.sem_exam, mp.lab, mp.project) IS NULL
                             AND ({best_2_of_3_cits_null_check_sql().strip()}) IS NULL
                        THEN NULL
                        ELSE ({best_2_of_3_cits_with_fallback_sql().strip()})
                             + COALESCE(COALESCE(mp.sem_exam, mp.lab, mp.project), 0)
                    END AS total_marks
                FROM marks_pivot mp
                JOIN students st ON st.id = mp.student_id
                JOIN subjects sb ON sb.id = mp.subject_id
                LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = sb.course_code
                WHERE mp.semester = :semester AND st.batch = :batch
            ),
            semester_gpa AS (
                SELECT
                    student_id,
                    roll_no,
                    reg_no,
                    name,
                    batch,
                    current_semester,
                    section,
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
                    )::numeric AS sgpa_sort,
                    ROUND(
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
                        )::numeric,
                        2
                    ) AS sgpa,
                    ROUND(AVG(total_marks) FILTER (WHERE total_marks IS NOT NULL)::numeric, 2) AS avg_marks,
                    COUNT(*) FILTER (
                        WHERE ({grade_point_from_grade_or_marks_sql('sem_grade', 'total_marks').strip()}) IS NOT NULL
                    ) AS subjects_attempted,
                    COUNT(*) FILTER (
                        WHERE course_code NOT LIKE '24AC%'
                          AND (
                            (NULLIF(trim(coalesce(sem_grade, '')), '') IS NOT NULL AND upper(coalesce(sem_grade, '')) IN ('U', 'F', 'FAIL', 'RA', 'AB', 'ABSENT', 'WH'))
                            OR (total_marks < 50 AND total_marks IS NOT NULL)
                          )
                    ) AS failed_subjects
                FROM semester_marks
                GROUP BY student_id, roll_no, reg_no, name, batch, current_semester, section
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
            semester_rankings AS (
                SELECT 
                    sg.roll_no,
                    sg.reg_no,
                    sg.name,
                    sg.batch,
                    sg.current_semester,
                    sg.section,
                    sg.sgpa,
                    sg.avg_marks,
                    sg.subjects_attempted,
                    sg.failed_subjects,
                    COALESCE(ar.attendance_percentage, 0) AS attendance_percentage,
                    DENSE_RANK() OVER (
                        ORDER BY
                            sg.sgpa_sort DESC,
                            sg.failed_subjects ASC,
                            sg.avg_marks DESC,
                            COALESCE(ar.attendance_percentage, 0) DESC,
                            sg.roll_no ASC
                    ) AS rank,
                    COUNT(*) OVER () AS total_students,
                    ROUND(
                        ((1 - PERCENT_RANK() OVER (
                            ORDER BY
                                sg.sgpa_sort DESC,
                                sg.failed_subjects ASC,
                                sg.avg_marks DESC,
                                COALESCE(ar.attendance_percentage, 0) DESC,
                                sg.roll_no ASC
                        )) * 100)::numeric,
                        2
                    ) AS percentile
                FROM semester_gpa sg
                LEFT JOIN attendance_rollup ar ON ar.student_id = sg.student_id
                WHERE sg.subjects_attempted > 0  -- Only include students with actual grades
            )
            SELECT *
            FROM semester_rankings
            ORDER BY rank
            LIMIT :limit OFFSET :offset
        ''')
        
        result = await db.execute(ranking_query, {
            'semester': semester,
            'batch': batch,
            'limit': limit,
            'offset': offset
        })
        
        rankings = []
        total_students = 0
        
        for row in result.mappings():
            total_students = row['total_students']
            rankings.append({
                'roll_no': row['roll_no'],
                'reg_no': row['reg_no'],
                'name': row['name'],
                'batch': row['batch'],
                'current_semester': row['current_semester'],
                'section': row['section'],
                'cgpa': float(row['sgpa']) if row['sgpa'] else 0.0,  # Using SGPA for semester ranking
                'attendance_percentage': float(row['attendance_percentage']) if row['attendance_percentage'] else 0.0,
                'backlogs': row['failed_subjects'] or 0,  # Using failed subjects in this semester
                'rank': row['rank'],
                'percentile': float(row['percentile']) if row['percentile'] else 0.0,
                'avg_marks': float(row['avg_marks']) if row['avg_marks'] else 0.0,
                'subjects_attempted': row['subjects_attempted'] or 0
            })
        
        return {
            'semester': semester,
            'batch': batch,
            'total_students': total_students,
            'rankings': rankings,
            'has_more': len(rankings) == limit and (offset + limit) < total_students
        }

    @staticmethod
    async def get_top_performers(
        db: AsyncSession,
        curriculum_credits: dict[str, float],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top N performers based on CGPA.
        
        Returns:
            List of student records with ranking information
        """
        result = await RankingService.get_overall_rankings(
            db=db,
            curriculum_credits=curriculum_credits,
            limit=limit,
            offset=0
        )
        return result['rankings']

    @staticmethod
    async def get_overall_rankings(
        db: AsyncSession,
        curriculum_credits: dict[str, float],
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get overall rankings across all students regardless of batch/semester.
        
        Returns:
            Dict with keys: total_students, rankings (list), has_more
        """
        credits_values = ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())
        
        ranking_query = text(f'''
            WITH overall_rankings AS (
                SELECT 
                    roll_no,
                    reg_no,
                    name,
                    batch,
                    current_semester,
                    section,
                    average_grade_points AS cgpa,
                    attendance_percentage,
                    backlogs,
                    DENSE_RANK() OVER (
                        ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                    ) AS rank,
                    COUNT(*) OVER () AS total_students,
                    ROUND(
                        ((1 - PERCENT_RANK() OVER (
                            ORDER BY average_grade_points_sort DESC, backlogs ASC, attendance_percentage DESC, roll_no ASC
                        )) * 100)::numeric,
                        2
                    ) AS percentile
                FROM (
                    {AdminService._admin_directory_query_text(credits_values)}
                ) directory
            )
            SELECT *
            FROM overall_rankings
            ORDER BY rank
            LIMIT :limit OFFSET :offset
        ''')
        
        result = await db.execute(ranking_query, {
            'limit': limit,
            'offset': offset
        })
        
        rankings = []
        total_students = 0
        
        for row in result.mappings():
            total_students = row['total_students']
            rankings.append({
                'roll_no': row['roll_no'],
                'reg_no': row['reg_no'],
                'name': row['name'],
                'batch': row['batch'],
                'current_semester': row['current_semester'],
                'section': row['section'],
                'cgpa': float(row['cgpa']) if row['cgpa'] else 0.0,
                'attendance_percentage': float(row['attendance_percentage']) if row['attendance_percentage'] else 0.0,
                'backlogs': row['backlogs'] or 0,
                'rank': row['rank'],
                'percentile': float(row['percentile']) if row['percentile'] else 0.0
            })
        
        return {
            'total_students': total_students,
            'rankings': rankings,
            'has_more': len(rankings) == limit and (offset + limit) < total_students
        }

    @staticmethod
    def calculate_rank_change(current_rank: int, previous_rank: Optional[int]) -> Optional[int]:
        """
        Calculate rank change from previous period.
        
        Returns:
            Positive number if rank improved (went down in number)
            Negative number if rank worsened (went up in number)
            None if no previous rank data
        """
        if previous_rank is None:
            return None
        return previous_rank - current_rank

    @staticmethod
    def get_rank_category(rank: int, total_students: int) -> str:
        """
        Categorize rank performance.
        
        Returns:
            "Excellent", "Good", "Average", or "Needs Improvement"
        """
        percentile = (rank / total_students) * 100
        
        if percentile <= 10:
            return "Excellent"
        elif percentile <= 25:
            return "Good"
        elif percentile <= 75:
            return "Average"
        else:
            return "Needs Improvement"