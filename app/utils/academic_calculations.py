"""
Utility functions for academic calculations.
"""

def best_2_of_3_cits_sql() -> str:
    """Returns SQL expression to calculate the **average** of the best 2 out of 3 CIT marks.

    Policy:
    - If 3 CITs exist: drop the lowest, average the remaining 2
    - If 2 CITs exist: average those 2
    - If only 1 CIT exists: use that mark as-is
    - If none: NULL

    Returning an *average* (not a sum) prevents total marks from being inflated beyond the grading scale.
    """
    return """
    CASE
        WHEN COALESCE(mp.cit1, -1) >= 0 AND COALESCE(mp.cit2, -1) >= 0 AND COALESCE(mp.cit3, -1) >= 0 THEN
            -- All 3 CITs available: average of best 2
            (
                COALESCE(mp.cit1, 0) + COALESCE(mp.cit2, 0) + COALESCE(mp.cit3, 0)
                - LEAST(COALESCE(mp.cit1, 0), COALESCE(mp.cit2, 0), COALESCE(mp.cit3, 0))
            ) / 2.0
        WHEN (COALESCE(mp.cit1, -1) >= 0 AND COALESCE(mp.cit2, -1) >= 0) THEN
            -- Only CIT1 and CIT2 available
            (COALESCE(mp.cit1, 0) + COALESCE(mp.cit2, 0)) / 2.0
        WHEN (COALESCE(mp.cit1, -1) >= 0 AND COALESCE(mp.cit3, -1) >= 0) THEN
            -- Only CIT1 and CIT3 available
            (COALESCE(mp.cit1, 0) + COALESCE(mp.cit3, 0)) / 2.0
        WHEN (COALESCE(mp.cit2, -1) >= 0 AND COALESCE(mp.cit3, -1) >= 0) THEN
            -- Only CIT2 and CIT3 available
            (COALESCE(mp.cit2, 0) + COALESCE(mp.cit3, 0)) / 2.0
        WHEN COALESCE(mp.cit1, -1) >= 0 THEN
            -- Only CIT1 available
            COALESCE(mp.cit1, 0)
        WHEN COALESCE(mp.cit2, -1) >= 0 THEN
            -- Only CIT2 available
            COALESCE(mp.cit2, 0)
        WHEN COALESCE(mp.cit3, -1) >= 0 THEN
            -- Only CIT3 available
            COALESCE(mp.cit3, 0)
        ELSE
            NULL
    END
    """


def best_2_of_3_cits_null_check_sql() -> str:
    """
    Returns SQL expression to check if we have valid CIT marks for best 2 calculation.
    
    Returns:
        SQL expression that returns the best-2 CIT *average* or NULL if insufficient data
    """
    return f"""
    NULLIF(
        ({best_2_of_3_cits_sql().strip()}),
        NULL
    )
    """


def best_2_of_3_cits_with_fallback_sql() -> str:
    """
    Returns SQL expression for best 2 of 3 CITs with 0 fallback.
    
    Returns:
        SQL expression that returns best-2 CIT *average* or 0 if no marks available
    """
    return f"""
    COALESCE(
        ({best_2_of_3_cits_sql().strip()}),
        0
    )
    """


def total_marks_calculation_sql(sg_internal_case: str) -> str:
    """
    Returns SQL expression for calculating total marks using best 2 of 3 CITs.
    
    Args:
        sg_internal_case: SQL expression that determines if internal marks should be included
        
    Returns:
        SQL expression for total marks calculation
    """
    return f"""
    CASE
        WHEN COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks) IS NULL
             AND ({best_2_of_3_cits_null_check_sql()}) IS NULL
        THEN NULL
        WHEN {sg_internal_case}
        THEN ({best_2_of_3_cits_with_fallback_sql()})
             + COALESCE(COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks), 0)
        ELSE COALESCE(COALESCE(mp.sem_exam, mp.lab_marks, mp.project_marks), 0)
    END
    """


def grade_point_calculation_sql(sg_internal_case: str) -> str:
    """
    Returns complete SQL CASE statement for grade point calculation using best 2 of 3 CITs.

    Args:
        sg_internal_case: SQL expression that determines if internal marks should be included

    Returns:
        Complete SQL CASE statement for grade point calculation
    """
    total_marks_expr = total_marks_calculation_sql(sg_internal_case)
    return f"""
    CASE
        WHEN ({total_marks_expr}) >= 90 THEN 10
        WHEN ({total_marks_expr}) >= 80 THEN 9
        WHEN ({total_marks_expr}) >= 70 THEN 8
        WHEN ({total_marks_expr}) >= 60 THEN 7
        WHEN ({total_marks_expr}) >= 50 THEN 6
        WHEN ({total_marks_expr}) >= 45 THEN 5
        ELSE 0
    END
    """


def grade_point_from_grade_sql(grade_expr: str) -> str:
    """SQL CASE: convert letter grade expression -> grade points."""
    return f"""
    CASE upper(coalesce({grade_expr}, ''))
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


def grade_point_from_marks_sql(marks_expr: str) -> str:
    """SQL CASE: convert numeric marks expression -> grade points."""
    return f"""
    CASE
        WHEN ({marks_expr}) >= 90 THEN 10
        WHEN ({marks_expr}) >= 80 THEN 9
        WHEN ({marks_expr}) >= 70 THEN 8
        WHEN ({marks_expr}) >= 60 THEN 7
        WHEN ({marks_expr}) >= 55 THEN 6
        WHEN ({marks_expr}) >= 50 THEN 5
        ELSE 0
    END
    """


def grade_point_from_grade_or_marks_sql(grade_expr: str, marks_expr: str) -> str:
    """
    Prefer grade->points when a grade is present; otherwise fall back to marks thresholds.

    Returns NULL only when both grade is blank and marks are NULL.
    """
    grade_points_expr = grade_point_from_grade_sql(grade_expr).strip()
    marks_points_expr = grade_point_from_marks_sql(marks_expr).strip()
    return f"""
    CASE
        WHEN NULLIF(trim(coalesce({grade_expr}, '')), '') IS NOT NULL THEN ({grade_points_expr})
        WHEN ({marks_expr}) IS NULL THEN NULL
        ELSE ({marks_points_expr})
    END
    """


def failed_calculation_sql(sg_internal_case: str) -> str:
    """
    Returns SQL expression for failed subject calculation using best 2 of 3 CITs.
    
    Args:
        sg_internal_case: SQL expression that determines if internal marks should be included
        
    Returns:
        SQL expression for failed calculation
    """
    total_marks_expr = total_marks_calculation_sql(sg_internal_case)
    return f"""
    CASE
        WHEN ({total_marks_expr}) < 50
             AND ({total_marks_expr}) IS NOT NULL
             AND COALESCE(sc.course_code, '') NOT ILIKE '24AC%'
        THEN 1
        ELSE 0
    END
    """