"""refine_grade_calc_audit_courses_only

Revision ID: g8h9i0j1k2l3
Revises: f7g8h9i0j1k2
Create Date: 2026-03-31 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'g8h9i0j1k2l3'
down_revision: Union[str, Sequence[str], None] = 'f7g8h9i0j1k2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Refine grade calculation: Only audit courses (24AC*) show 'P' when no marks.
    
    Uses a trigger to check subject code and apply audit-specific logic.
    """
    # Drop the old computed columns
    op.execute("ALTER TABLE student_marks DROP COLUMN grade")
    op.execute("ALTER TABLE student_marks DROP COLUMN result_status")
    
    # Recreate as regular columns (no longer computed)
    op.add_column('student_marks', sa.Column(
        'grade',
        sa.String(2),
        nullable=True
    ))
    
    op.add_column('student_marks', sa.Column(
        'result_status',
        sa.String(10),
        nullable=True
    ))
    
    # Create trigger function to set grades based on marks and subject type
    op.execute("""
    CREATE OR REPLACE FUNCTION update_student_grades()
    RETURNS TRIGGER AS $$
    DECLARE
        v_course_code VARCHAR(20);
        v_total_marks NUMERIC;
        v_internal_marks NUMERIC;
    BEGIN
        -- Get the course code
        SELECT course_code INTO v_course_code FROM subjects WHERE id = NEW.subject_id;
        
        -- Calculate internal marks (max of three CITs)
        v_internal_marks := GREATEST(
            COALESCE(NEW.cit1_marks, 0),
            COALESCE(NEW.cit2_marks, 0),
            COALESCE(NEW.cit3_marks, 0)
        );
        
        -- Calculate total marks
        v_total_marks := v_internal_marks + COALESCE(NEW.semester_exam_marks, 0);
        
        -- Store computed marks
        NEW.internal_marks := v_internal_marks;
        NEW.total_marks := v_total_marks;
        
        -- Determine grade and status for audit courses (only show 'P' if no marks)
        IF v_course_code LIKE '24AC%' THEN
            IF NEW.cit1_marks IS NULL AND NEW.cit2_marks IS NULL AND NEW.cit3_marks IS NULL AND NEW.semester_exam_marks IS NULL THEN
                NEW.grade := 'P';
                NEW.result_status := 'Pass';
            ELSE
                -- Audit courses with marks should use normal grading
                IF v_total_marks >= 90 THEN NEW.grade := 'O';
                ELSIF v_total_marks >= 80 THEN NEW.grade := 'A+';
                ELSIF v_total_marks >= 70 THEN NEW.grade := 'A';
                ELSIF v_total_marks >= 60 THEN NEW.grade := 'B+';
                ELSIF v_total_marks >= 50 THEN NEW.grade := 'B';
                ELSIF v_total_marks >= 45 THEN NEW.grade := 'C';
                ELSE NEW.grade := 'F';
                END IF;
                
                NEW.result_status := CASE WHEN v_total_marks >= 50 THEN 'Pass' ELSE 'Fail' END;
            END IF;
        ELSE
            -- For labs and other subjects, use normal grading (no special handling for NULL)
            IF v_total_marks >= 90 THEN NEW.grade := 'O';
            ELSIF v_total_marks >= 80 THEN NEW.grade := 'A+';
            ELSIF v_total_marks >= 70 THEN NEW.grade := 'A';
            ELSIF v_total_marks >= 60 THEN NEW.grade := 'B+';
            ELSIF v_total_marks >= 50 THEN NEW.grade := 'B';
            ELSIF v_total_marks >= 45 THEN NEW.grade := 'C';
            ELSE NEW.grade := 'F';
            END IF;
            
            NEW.result_status := CASE WHEN v_total_marks >= 50 THEN 'Pass' ELSE 'Fail' END;
        END IF;
        
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)
    
    # Drop existing trigger if it exists
    op.execute("DROP TRIGGER IF EXISTS update_grades_trigger ON student_marks")
    
    # Create trigger for INSERT and UPDATE
    op.execute("""
    CREATE TRIGGER update_grades_trigger
    BEFORE INSERT OR UPDATE ON student_marks
    FOR EACH ROW
    EXECUTE FUNCTION update_student_grades();
    """)
    
    # Backfill existing records with new logic
    op.execute("""
    UPDATE student_marks sm
    SET grade = CASE 
        WHEN s.course_code LIKE '24AC%' AND sm.cit1_marks IS NULL AND sm.cit2_marks IS NULL AND sm.cit3_marks IS NULL AND sm.semester_exam_marks IS NULL THEN 'P'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 90 THEN 'O'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 80 THEN 'A+'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 70 THEN 'A'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 60 THEN 'B+'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 50 THEN 'B'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 45 THEN 'C'
        ELSE 'F' END,
        result_status = CASE 
        WHEN s.course_code LIKE '24AC%' AND sm.cit1_marks IS NULL AND sm.cit2_marks IS NULL AND sm.cit3_marks IS NULL AND sm.semester_exam_marks IS NULL THEN 'Pass'
        WHEN (GREATEST(COALESCE(sm.cit1_marks, 0), COALESCE(sm.cit2_marks, 0), COALESCE(sm.cit3_marks, 0)) + COALESCE(sm.semester_exam_marks, 0)) >= 50 THEN 'Pass'
        ELSE 'Fail' END
    FROM subjects s
    WHERE sm.subject_id = s.id;
    """)


def downgrade() -> None:
    """Revert to previous grade calculation logic."""
    # Drop the trigger
    op.execute("DROP TRIGGER IF EXISTS update_grades_trigger ON student_marks")
    op.execute("DROP FUNCTION IF EXISTS update_student_grades()")
    
    # Drop the regular columns
    op.execute("ALTER TABLE student_marks DROP COLUMN grade")
    op.execute("ALTER TABLE student_marks DROP COLUMN result_status")
    
    # Recreate as computed columns (previous version: all subjects with NULL marks show 'P')
    op.add_column('student_marks', sa.Column(
        'grade',
        sa.String(2),
        sa.Computed(
            """CASE 
                WHEN (cit1_marks IS NULL AND cit2_marks IS NULL AND cit3_marks IS NULL AND semester_exam_marks IS NULL) THEN 'P'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 90 THEN 'O'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 80 THEN 'A+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 70 THEN 'A'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 60 THEN 'B+'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'B'
                WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 45 THEN 'C'
                ELSE 'F' 
            END""",
            persisted=True
        )
    ))
    
    op.add_column('student_marks', sa.Column(
        'result_status',
        sa.String(10),
        sa.Computed(
            "CASE WHEN (cit1_marks IS NULL AND cit2_marks IS NULL AND cit3_marks IS NULL AND semester_exam_marks IS NULL) THEN 'Pass' WHEN (GREATEST(COALESCE(cit1_marks, 0), COALESCE(cit2_marks, 0), COALESCE(cit3_marks, 0)) + COALESCE(semester_exam_marks, 0)) >= 50 THEN 'Pass' ELSE 'Fail' END",
            persisted=True
        )
    ))

