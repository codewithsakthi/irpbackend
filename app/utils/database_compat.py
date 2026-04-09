"""
Database compatibility utilities for threshold management
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import ProgrammingError


async def check_threshold_columns_exist(db: AsyncSession) -> bool:
    """Check if threshold columns exist in the subjects table"""
    try:
        # Simple query to check if columns exist
        test_query = text("SELECT pass_threshold FROM subjects LIMIT 1")
        await db.execute(test_query)
        return True
    except (ProgrammingError, Exception):
        return False


def get_subject_catalog_query(has_threshold_columns: bool = False) -> str:
    """Get the appropriate subject catalog query based on schema version"""
    
    if has_threshold_columns:
        # Query with threshold columns (post-migration)
        return """
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
        """
    else:
        # Query without threshold columns (pre-migration)
        return """
            SELECT
                s.id AS subject_id,
                s.course_code,
                s.name AS subject_name,
                COALESCE(s.credits, 0) AS credits,
                s.semester,
                50.0 AS pass_threshold,
                75.0 AS target_average,
                85.0 AS percentile_excellent,
                60.0 AS percentile_good,
                30.0 AS percentile_average
            FROM subjects s
        """


def get_performance_label_case(has_threshold_columns: bool = False) -> str:
    """Get the appropriate CASE statement for performance labels"""
    
    if has_threshold_columns:
        # Use actual threshold columns
        return """
            CASE
                WHEN sp.percentile_rank < sc.percentile_average OR mwt.computed_total_marks < sc.pass_threshold THEN 'At Risk'
                WHEN sp.percentile_rank <= sc.percentile_good THEN 'Average'
                WHEN sp.percentile_rank <= sc.percentile_excellent THEN 'Good'
                ELSE 'Excellent'
            END
        """
    else:
        # Use hardcoded defaults
        return """
            CASE
                WHEN sp.percentile_rank < 30.0 OR mwt.computed_total_marks < 50.0 THEN 'At Risk'
                WHEN sp.percentile_rank <= 60.0 THEN 'Average'
                WHEN sp.percentile_rank <= 85.0 THEN 'Good'
                ELSE 'Excellent'
            END
        """