from __future__ import annotations

from typing import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .. import schemas
from ..utils.academic_calculations import (
    best_2_of_3_cits_null_check_sql, 
    best_2_of_3_cits_with_fallback_sql, 
    total_marks_calculation_sql, 
    grade_point_calculation_sql,
    failed_calculation_sql
)

GRADE_POINTS_SQL = """
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


def _credits_values(curriculum_credits: dict[str, float]) -> str:
    return ", ".join(f"('{code}', {credit})" for code, credit in curriculum_credits.items())


def _risk_level(score: float) -> str:
    if score >= 70:
        return "Critical"
    if score >= 55:
        return "High"
    if score >= 35:
        return "Moderate"
    return "Low"


def _lab_or_audit_case(subject_name_expr: str, subject_code_expr: str) -> str:
    return f"""
    CASE
        WHEN lower(coalesce({subject_name_expr}, '')) SIMILAR TO '%(lab|project|practic|workshop|audit|value added|mandatory|non credit)%' THEN FALSE
        WHEN coalesce({subject_code_expr}, '') ILIKE '24AC%' THEN FALSE
        ELSE TRUE
    END
    """


async def build_hod_dashboard(
    db: AsyncSession,
    curriculum_credits: dict[str, float],
) -> schemas.HODDashboardResponse:
    credits_values = _credits_values(curriculum_credits)
    # In these queries we only join `subject_catalog sc` / `subjects sb`, so we must not
    # reference unrelated aliases (e.g. `sg`) inside the CASE expressions.
    sg_internal_case = _lab_or_audit_case("sc.name", "sc.course_code")
    directory_internal_case = _lab_or_audit_case("sb.name", "sb.course_code")

    dashboard_query = text(
        f"""
        WITH curriculum_credits_map AS (
            SELECT * FROM (VALUES {credits_values}) AS t(course_code, credit)
        ),
        subject_catalog AS (
            SELECT s.id, s.course_code, s.name, s.semester, 
                CASE 
                    WHEN s.course_code LIKE '24AC%' THEN 0.0
                    ELSE COALESCE(NULLIF(s.credits, 0), ccm.credit, 0)
                END AS credit
            FROM subjects s
            LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = s.course_code
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
                MAX(marks) FILTER (WHERE assessment_type = 'LAB') AS lab_marks,
                MAX(marks) FILTER (WHERE assessment_type = 'PROJECT') AS project_marks
            FROM v_final_assessments
            GROUP BY student_id, subject_id, semester
        ),
        marks_enriched AS (
            SELECT
                st.id AS student_id,
                st.roll_no,
                st.name,
                st.batch,
                st.current_semester,
                mp.semester,
                sc.course_code AS subject_code,
                sc.name AS subject_name,
                COALESCE(sc.credit, 0) AS credit,
                ({best_2_of_3_cits_null_check_sql()}) AS internal_marks,
                CASE
                    WHEN {sg_internal_case} THEN ({best_2_of_3_cits_null_check_sql()})
                    ELSE NULL
                END AS effective_internal_marks,
                COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks) AS exam_marks,
                ({total_marks_calculation_sql(sg_internal_case)}) AS total_marks,
                ({grade_point_calculation_sql(sg_internal_case)}) AS grade_point,
                ({failed_calculation_sql(sg_internal_case)}) AS failed
            FROM marks_pivot mp
            JOIN students st ON st.id = mp.student_id
            JOIN subject_catalog sc ON sc.id = mp.subject_id
        ),
        semester_gpa AS (
            SELECT
                student_id,
                roll_no,
                name,
                semester,
                CASE
                    WHEN SUM(credit) FILTER (WHERE credit > 0) > 0
                    THEN ROUND((SUM(grade_point * credit) / SUM(credit) FILTER (WHERE credit > 0))::numeric, 2)
                    ELSE ROUND(AVG(grade_point) FILTER (WHERE subject_code NOT ILIKE '24AC%')::numeric, 2)
                END AS sgpa,
                ROUND(AVG(effective_internal_marks)::numeric, 2) AS avg_internal,
                ROUND((100.0 * SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0))::numeric, 2) AS pass_rate
            FROM marks_enriched
            WHERE total_marks IS NOT NULL
            GROUP BY student_id, roll_no, name, semester
        ),
        gpa_velocity AS (
            SELECT
                student_id,
                roll_no,
                name,
                semester,
                sgpa,
                LAG(sgpa) OVER (PARTITION BY student_id ORDER BY semester) AS previous_sgpa,
                ROUND((sgpa - LAG(sgpa) OVER (PARTITION BY student_id ORDER BY semester))::numeric, 2) AS velocity
            FROM semester_gpa
        ),
        attendance_rollup AS (
            SELECT
                student_id,
                ROUND(
                    (100.0 * SUM(present + on_duty) / NULLIF(SUM(total_periods), 0))::numeric,
                    2
                ) AS attendance_pct
            FROM v_attendance_summary
            GROUP BY student_id
        ),
        cumulative_gpa AS (
            SELECT
                student_id,
                ROUND(
                    CASE
                        WHEN SUM(credit) FILTER (WHERE credit > 0) > 0
                        THEN (SUM(grade_point * credit) / SUM(credit) FILTER (WHERE credit > 0))
                        ELSE AVG(grade_point) FILTER (WHERE subject_code NOT ILIKE '24AC%')
                    END::numeric, 2
                ) AS cgpa
            FROM marks_enriched
            WHERE total_marks IS NOT NULL
            GROUP BY student_id
        ),
        student_current AS (
            SELECT DISTINCT ON (st.id)
                st.id AS student_id,
                st.roll_no,
                st.name,
                st.batch,
                st.current_semester,
                COALESCE(ar.attendance_pct, 0) AS attendance_pct,
                COALESCE(sg.sgpa, 0) AS current_sgpa,
                COALESCE(cg.cgpa, sg.sgpa, 0) AS cgpa,
                sg.avg_internal AS internal_pct,
                COALESCE(gv.velocity, 0) AS gpa_velocity,
                COALESCE(gv.previous_sgpa, sg.sgpa, 0) AS previous_sgpa
            FROM students st
            LEFT JOIN attendance_rollup ar ON ar.student_id = st.id
            LEFT JOIN cumulative_gpa cg ON cg.student_id = st.id
            LEFT JOIN semester_gpa sg ON sg.student_id = st.id AND sg.semester = st.current_semester
            LEFT JOIN gpa_velocity gv ON gv.student_id = st.id AND gv.semester = st.current_semester
            ORDER BY st.id, st.current_semester DESC
        ),
        risk_scores AS (
            SELECT
                sc.*,
                ROUND(LEAST(
                    100,
                    GREATEST(0, (75 - sc.attendance_pct) / 75.0 * 100) * 0.30 +
                    CASE
                        WHEN sc.internal_pct IS NULL THEN 0
                        ELSE GREATEST(0, (60 - sc.internal_pct) / 60.0 * 100) * 0.30
                    END +
                    GREATEST(0, (COALESCE(sc.previous_sgpa, sc.current_sgpa) - sc.current_sgpa) * 20) * 0.40
                )::numeric, 2) AS risk_score
            FROM student_current sc
        ),
        trend_points AS (
            SELECT
                semester,
                ROUND(AVG(sgpa)::numeric, 2) AS average_gpa,
                ROUND(AVG(pass_rate)::numeric, 2) AS average_attendance,
                COUNT(*) AS student_count,
                SUM(CASE WHEN sgpa < 5 THEN 1 ELSE 0 END) AS at_risk_count
            FROM semester_gpa
            GROUP BY semester
            ORDER BY semester
        ),
        failure_density AS (
            SELECT
                me.subject_code,
                me.subject_name,
                me.semester,
                COUNT(*) AS attempts,
                SUM(me.failed) AS red_zone_count,
                ROUND((100.0 * SUM(me.failed) / NULLIF(COUNT(*), 0))::numeric, 2) AS fail_rate
            FROM marks_enriched me
            GROUP BY me.subject_code, me.subject_name, me.semester
            HAVING COUNT(*) >= 3
        ),
        faculty_impact AS (
            SELECT
                fsa.faculty_id,
                sf.name AS faculty_name,
                sb.course_code AS subject_code,
                sb.name AS subject_name,
                COUNT(sa.id) AS student_count,
                ROUND(AVG(
                    CASE 
                        WHEN sa.marks >= 90 THEN 10 WHEN sa.marks >= 80 THEN 9
                        WHEN sa.marks >= 70 THEN 8 WHEN sa.marks >= 60 THEN 7
                        WHEN sa.marks >= 50 THEN 6 WHEN sa.marks >= 40 THEN 5
                        ELSE 0 
                    END
                )::numeric, 2) AS average_gpa,
                ROUND((100.0 * SUM(CASE WHEN sa.marks >= 40 THEN 1 ELSE 0 END) / NULLIF(COUNT(sa.id), 0))::numeric, 2) AS pass_rate
            FROM faculty_subject_assignments fsa
            JOIN staff sf ON sf.id = fsa.faculty_id
            JOIN subjects sb ON sb.id = fsa.subject_id
            LEFT JOIN student_assessments sa ON sa.subject_id = sb.id AND sa.assessment_type = 'SEMESTER_EXAM'
            GROUP BY fsa.faculty_id, sf.name, sb.course_code, sb.name
        )
        SELECT json_build_object(
            'metrics', json_build_object(
                'active_students', (SELECT COUNT(*) FROM students),
                'avg_attendance', COALESCE((SELECT ROUND(AVG(attendance_pct)::numeric, 2) FROM student_current), 0),
                'avg_gpa', COALESCE((SELECT ROUND(AVG(current_sgpa)::numeric, 2) FROM student_current), 0),
                'risk_count', COALESCE((SELECT COUNT(*) FROM risk_scores WHERE risk_score >= 70), 0)
            ),
            'risk_students', COALESCE((
                SELECT json_agg(x ORDER BY x.risk_score DESC)
                FROM (
                    SELECT roll_no, name, risk_score, attendance_pct, internal_pct, previous_sgpa, current_sgpa, gpa_velocity
                    FROM risk_scores
                    WHERE risk_score >= 35
                    ORDER BY risk_score DESC
                    LIMIT 8
                ) x
            ), '[]'::json),
            'trend_points', COALESCE((SELECT json_agg(tp ORDER BY tp.semester) FROM trend_points tp), '[]'::json),
            'failure_heatmap', COALESCE((
                SELECT json_agg(fd ORDER BY fd.fail_rate DESC, fd.red_zone_count DESC)
                FROM (
                    SELECT * FROM failure_density
                    ORDER BY fail_rate DESC, red_zone_count DESC
                    LIMIT 8
                ) fd
            ), '[]'::json),
            'faculty_impact', COALESCE((
                SELECT json_agg(fi ORDER BY fi.pass_rate DESC, fi.average_gpa DESC)
                FROM (
                    SELECT *, ROUND((pass_rate * 0.65 + average_gpa * 3.5)::numeric, 2) AS impact_score
                    FROM faculty_impact
                    WHERE student_count > 0
                    ORDER BY impact_score DESC
                    LIMIT 6
                ) fi
            ), '[]'::json),
            'strength_radar', COALESCE((
                SELECT json_agg(sr ORDER BY sr.gpa DESC, sr.consistency DESC)
                FROM (
                    SELECT
                        rs.roll_no,
                        rs.name,
                        rs.attendance_pct AS attendance,
                        rs.internal_pct AS internals,
                        rs.current_sgpa AS gpa,
                        rs.cgpa,
                        ROUND(GREATEST(0, 100 - ABS(COALESCE(rs.gpa_velocity, 0)) * 40)::numeric, 2) AS consistency
                    FROM risk_scores rs
                    ORDER BY rs.cgpa DESC, rs.current_sgpa DESC, rs.internal_pct DESC
                    LIMIT 5
                ) sr
            ), '[]'::json)
        ) AS payload
        """
    )

    result = await db.execute(dashboard_query)
    payload = result.scalar_one()

    directory_query = text(
        f"""
        WITH curriculum_credits_map AS (
            SELECT * FROM (VALUES {credits_values}) AS t(course_code, credit)
        ),
        attendance_rollup AS (
            SELECT
                student_id,
                SUM(total_periods) AS attendance_count,
                ROUND(
                    (100.0 * SUM(present + on_duty) / NULLIF(SUM(total_periods), 0))::numeric,
                    2
                ) AS attendance_percentage
            FROM v_attendance_summary
            GROUP BY student_id
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
                MAX(v.marks) FILTER (WHERE v.assessment_type = 'LAB') AS lab_marks,
                MAX(v.marks) FILTER (WHERE v.assessment_type = 'PROJECT') AS project_marks
            FROM v_final_assessments v
            GROUP BY v.student_id, v.subject_id, v.semester
        ),
        marks_scored AS (
            SELECT
                mp.student_id,
                sb.course_code,
                ({best_2_of_3_cits_null_check_sql()}) AS internal_max,
                COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks) AS exam_component,
                ({total_marks_calculation_sql(directory_internal_case)}) AS total_marks,
                CASE 
                    WHEN sb.course_code LIKE '24AC%' THEN 0.0
                    ELSE COALESCE(NULLIF(sb.credits, 0), ccm.credit, 0)
                END AS credit
            FROM marks_pivot mp
            JOIN subjects sb ON sb.id = mp.subject_id
            LEFT JOIN curriculum_credits_map ccm ON ccm.course_code = sb.course_code
        ),
        grade_rollup_pivot AS (
            SELECT
                student_id,
                COUNT(*) FILTER (WHERE total_marks IS NOT NULL) AS marks_count,
                ROUND(AVG(internal_max) FILTER (WHERE internal_max IS NOT NULL)::numeric, 2) AS average_internal_percentage,
                ROUND(AVG(
                    CASE
                        WHEN total_marks >= 90 THEN 10
                        WHEN total_marks >= 80 THEN 9
                        WHEN total_marks >= 70 THEN 8
                        WHEN total_marks >= 60 THEN 7
                        WHEN total_marks >= 50 THEN 6
                        WHEN total_marks >= 45 THEN 5
                        ELSE 0
                    END
                ) FILTER (WHERE total_marks IS NOT NULL)::numeric, 2) AS average_grade_points,
                SUM(CASE WHEN total_marks < 50 AND total_marks IS NOT NULL AND credit > 0.0 AND course_code NOT LIKE '24AC%' THEN 1 ELSE 0 END) AS backlogs
            FROM marks_scored
            GROUP BY student_id
        )
        SELECT
            st.roll_no,
            st.reg_no,
            st.name,
            st.batch,
            st.current_semester,
            NULL::text AS city,
            st.email,
            NULL::text AS phone_primary,
            COALESCE(gr.marks_count, 0) AS marks_count,
            COALESCE(ar.attendance_count, 0) AS attendance_count,
            COALESCE(ar.attendance_percentage, 0) AS attendance_percentage,
            COALESCE(gr.average_grade_points, 0) AS average_grade_points,
            COALESCE(gr.average_internal_percentage, 0) AS average_internal_percentage,
            COALESCE(gr.backlogs, 0) AS backlogs,
            DENSE_RANK() OVER (
                ORDER BY
                    COALESCE(gr.average_grade_points, 0) DESC,
                    COALESCE(gr.backlogs, 0) ASC,
                    COALESCE(ar.attendance_percentage, 0) DESC,
                    st.roll_no ASC
            ) AS rank
        FROM students st
        LEFT JOIN grade_rollup_pivot gr ON gr.student_id = st.id
        LEFT JOIN attendance_rollup ar ON ar.student_id = st.id
        ORDER BY rank, st.roll_no
        LIMIT 12
        """
    )
    directory_rows = (await db.execute(directory_query)).mappings().all()
    directory = [schemas.AdminDirectoryStudent(**dict(row)) for row in directory_rows]

    metrics_blob = payload["metrics"]
    risk_students = [_build_risk_student(row) for row in payload["risk_students"]]
    trend_points = [schemas.TrendPoint(label=f"Sem {row['semester']}", **row) for row in payload["trend_points"]]
    failure_heatmap = [schemas.FailureHeatmapCell(**row) for row in payload["failure_heatmap"]]
    faculty_impact = [schemas.FacultyImpactView(**row) for row in payload["faculty_impact"]]
    strength_radar = [schemas.StudentStrengthRadar(**{k: v for k, v in row.items() if k in schemas.StudentStrengthRadar.model_fields}) for row in payload["strength_radar"]]

    critical_risk_count = int(metrics_blob["risk_count"])

    overall_health_score = round(
        max(
            0.0,
            min(
                100.0,
                100
                - (critical_risk_count * 4)
                + (metrics_blob["avg_attendance"] - 75) * 0.2
                + (metrics_blob["avg_gpa"] - 6) * 6,
            ),
        ),
        2,
    )

    daily_briefing = _build_daily_briefing(
        overall_health_score=overall_health_score,
        failure_heatmap=failure_heatmap,
        trend_points=trend_points,
        risk_students=risk_students,
        critical_risk_count=critical_risk_count,
    )

    department_health = schemas.DepartmentHealth(
        overall_health_score=overall_health_score,
        active_students=metrics_blob["active_students"],
        at_risk_count=critical_risk_count,
        average_attendance=metrics_blob["avg_attendance"],
        average_gpa=metrics_blob["avg_gpa"],
        department_name="MCA",
        daily_briefing=daily_briefing,
        semester_trends=[point.model_dump() for point in trend_points],
        top_critical_subjects=[
            schemas.SubjectDifficultyItem(
                code=item.subject_code,
                subject=item.subject_name,
                semester=item.semester,
                fail_rate=item.fail_rate,
                average_grade_point=0,
                average_internal=0,
                variance=0,
                difficulty_index=round(item.fail_rate / 10, 2),
                pass_rate=round(100 - item.fail_rate, 2),
            )
            for item in failure_heatmap[:5]
        ],
    )

    metrics = {
        "activeStudents": schemas.DashboardMetric(value=metrics_blob["active_students"], label="Active Students"),
        "averageAttendance": schemas.DashboardMetric(value=metrics_blob["avg_attendance"], label="Average Attendance"),
        "averageGpa": schemas.DashboardMetric(value=metrics_blob["avg_gpa"], label="Average GPA"),
        "criticalRisk": schemas.DashboardMetric(value=department_health.at_risk_count, label="Critical Risk"),
        "healthScore": schemas.DashboardMetric(value=department_health.overall_health_score, label="Health Score"),
    }

    return schemas.HODDashboardResponse(
        department_health=department_health,
        metrics=metrics,
        daily_briefing=daily_briefing,
        risk_students=risk_students,
        trend_points=trend_points,
        failure_heatmap=failure_heatmap,
        faculty_impact=faculty_impact,
        strength_radar=strength_radar,
        directory=directory,
    )


def _build_risk_student(row: dict) -> schemas.StudentRiskScore:
    alerts = []
    if row["attendance_pct"] < 75:
        alerts.append(f"Attendance at {row['attendance_pct']}%")
    if row["internal_pct"] is not None and row["internal_pct"] < 60:
        alerts.append(f"Internals at {row['internal_pct']}%")
    if row["current_sgpa"] < row["previous_sgpa"]:
        alerts.append(f"GPA velocity {row['gpa_velocity']}")

    risk_score = float(row["risk_score"])
    return schemas.StudentRiskScore(
        roll_no=row["roll_no"],
        name=row["name"],
        risk_score=risk_score,
        attendance_factor=float(row["attendance_pct"]),
        internal_marks_factor=float(row["internal_pct"] or 0),
        gpa_drop_factor=max(0.0, float(row["previous_sgpa"]) - float(row["current_sgpa"])),
        is_at_risk=risk_score >= 55,
        risk_level=_risk_level(risk_score),
        alerts=alerts or ["Monitoring recommended"],
    )


def _build_daily_briefing(
    *,
    overall_health_score: float,
    failure_heatmap: Iterable[schemas.FailureHeatmapCell],
    trend_points: Iterable[schemas.TrendPoint],
    risk_students: Iterable[schemas.StudentRiskScore],
    critical_risk_count: int,
) -> str:
    failure_heatmap = list(failure_heatmap)
    trend_points = list(trend_points)
    risk_students = list(risk_students)

    lead = f"Overall Dept. Health is {round(overall_health_score)}%."
    if failure_heatmap:
        hottest = failure_heatmap[0]
        lead += f" Alert: {hottest.subject_name} in Sem {hottest.semester} is showing a {round(hottest.fail_rate, 1)}% red-zone cluster."

    if trend_points:
        latest = trend_points[-1]
        lead += f" Current semester GPA is {latest.average_gpa} with {latest.average_attendance}% pass momentum."

    if risk_students:
        lead += f" {critical_risk_count} students need immediate intervention."

    return lead[:400]
