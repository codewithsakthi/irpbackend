"""fix_assessment_grades_and_schema_duplicates

Revision ID: 9b3c1d2e4f6a
Revises: 7e1f78e38cf8
Create Date: 2026-05-26 15:45:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "9b3c1d2e4f6a"
down_revision: Union[str, Sequence[str], None] = "7e1f78e38cf8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Keep one canonical unique constraint for each natural key before dropping
    # duplicate constraints that may have been introduced by older migration layers.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'programs'::regclass
                  AND conname = 'uq_program_code'
            ) THEN
                ALTER TABLE programs ADD CONSTRAINT uq_program_code UNIQUE (code);
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'roles'::regclass
                  AND conname = 'uq_role_name'
            ) THEN
                ALTER TABLE roles ADD CONSTRAINT uq_role_name UNIQUE (name);
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'refresh_tokens'::regclass
                  AND conname = 'uq_refresh_tokens_token_id'
            ) THEN
                ALTER TABLE refresh_tokens ADD CONSTRAINT uq_refresh_tokens_token_id UNIQUE (token_id);
            END IF;

            ALTER TABLE programs DROP CONSTRAINT IF EXISTS programs_code_key;
            ALTER TABLE programs DROP CONSTRAINT IF EXISTS uniq_program_code;
            ALTER TABLE roles DROP CONSTRAINT IF EXISTS roles_name_key;
            ALTER TABLE roles DROP CONSTRAINT IF EXISTS uniq_role_name;
            ALTER TABLE student_assessments DROP CONSTRAINT IF EXISTS chk_sa_assessment_type;
            ALTER TABLE subjects DROP CONSTRAINT IF EXISTS subjects_course_code_key;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'counselor_diary'::regclass
                  AND conname = 'fk_counselor_diary_counselor_id'
            ) AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'counselor_diary'
                  AND column_name = 'counselor_id'
            ) THEN
                ALTER TABLE counselor_diary
                ADD CONSTRAINT fk_counselor_diary_counselor_id
                FOREIGN KEY (counselor_id) REFERENCES staff(id) NOT VALID;
            END IF;
        END$$;
        """
    )

    # Drop duplicate/plain indexes that overlap with PKs, unique constraints, or
    # an already-retained composite index.
    for index_name in (
        "idx_refresh_token_id",
        "idx_refresh_tokens_token_id",
        "ix_refresh_tokens_token_id",
        "idx_counselor_student",
        "idx_sa_student_semester",
        "idx_sa_student_semester_hardened",
        "idx_sa_final_student_subject",
        "uniq_final_assessment",
        "ix_programs_id",
        "ix_roles_id",
        "ix_refresh_tokens_id",
        "ix_faculty_subject_assignments_id",
        "ix_student_growth_history_id",
    ):
        op.execute(f"DROP INDEX IF EXISTS {index_name};")

    op.execute("ALTER TABLE students DROP CONSTRAINT IF EXISTS chk_valid_semester;")
    op.execute(
        "ALTER TABLE students ADD CONSTRAINT chk_valid_semester CHECK (current_semester >= 1);"
    )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sa_final
        ON student_assessments (student_id, subject_id, assessment_type)
        WHERE is_final = true;
        """
    )

    # Replace the unwired/broken grade trigger with one that matches the current
    # row-shaped table: one marks column plus assessment_type.
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.student_marks') IS NOT NULL THEN
                DROP TRIGGER IF EXISTS update_grades_trigger ON student_marks;
            END IF;
        END$$;
        """
    )
    op.execute("DROP TRIGGER IF EXISTS update_grades_trigger ON student_assessments;")
    op.execute("DROP TRIGGER IF EXISTS trg_update_grades ON student_assessments;")
    op.execute("DROP TRIGGER IF EXISTS trg_update_student_assessment_grades ON student_assessments;")
    op.execute("DROP FUNCTION IF EXISTS update_student_assessment_grades();")
    op.execute(
        """
        CREATE OR REPLACE FUNCTION update_student_grades()
        RETURNS TRIGGER AS $$
        DECLARE
            v_assessment_type TEXT;
            v_marks NUMERIC;
        BEGIN
            v_assessment_type := upper(coalesce(NEW.assessment_type, ''));
            v_marks := NEW.marks;

            IF v_marks IS NULL THEN
                NEW.grade := NULL;
                NEW.result_status := NULL;
            ELSIF v_assessment_type IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') THEN
                IF v_marks >= 90 THEN NEW.grade := 'O';
                ELSIF v_marks >= 80 THEN NEW.grade := 'A+';
                ELSIF v_marks >= 70 THEN NEW.grade := 'A';
                ELSIF v_marks >= 60 THEN NEW.grade := 'B+';
                ELSIF v_marks >= 50 THEN NEW.grade := 'B';
                ELSIF v_marks >= 45 THEN NEW.grade := 'C';
                ELSE NEW.grade := 'F';
                END IF;

                NEW.result_status := CASE WHEN v_marks >= 50 THEN 'Pass' ELSE 'Fail' END;
            ELSE
                NEW.grade := NULL;
                NEW.result_status := NULL;
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_update_grades
        BEFORE INSERT OR UPDATE OF marks, assessment_type ON student_assessments
        FOR EACH ROW
        EXECUTE FUNCTION update_student_grades();
        """
    )
    op.execute(
        """
        UPDATE student_assessments
        SET
            grade = CASE
                WHEN marks IS NULL THEN NULL
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 90 THEN 'O'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 80 THEN 'A+'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 70 THEN 'A'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 60 THEN 'B+'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 50 THEN 'B'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 45 THEN 'C'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') THEN 'F'
                ELSE NULL
            END,
            result_status = CASE
                WHEN marks IS NULL THEN NULL
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') AND marks >= 50 THEN 'Pass'
                WHEN upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT') THEN 'Fail'
                ELSE NULL
            END;
        """
    )

    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_student_semester_summary CASCADE;")
    op.execute(
        """
        CREATE MATERIALIZED VIEW mv_student_semester_summary AS
        SELECT
            student_id,
            semester,
            ROUND(AVG(marks), 2) as average_marks,
            COUNT(*) FILTER (
                WHERE is_final = true
                  AND upper(assessment_type) IN ('SEMESTER_EXAM', 'LAB', 'PROJECT')
                  AND upper(coalesce(result_status, '')) IN ('FAIL', 'F')
            ) as backlog_count,
            ROUND(AVG(CASE WHEN assessment_type = 'SEMESTER_EXAM' THEN marks END), 2) as exam_average
        FROM student_assessments
        GROUP BY student_id, semester;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW v_attendance_summary AS
        SELECT
            student_id,
            subject_id,
            semester,
            COUNT(*) AS total_periods,
            SUM(CASE WHEN status = 'P' THEN 1 ELSE 0 END) AS present,
            SUM(CASE WHEN status = 'A' THEN 1 ELSE 0 END) AS absent,
            SUM(CASE WHEN status = 'L' THEN 1 ELSE 0 END) AS leave,
            SUM(CASE WHEN status = 'O' THEN 1 ELSE 0 END) AS on_duty,
            ROUND(
                (
                    100.0
                    * SUM(CASE WHEN status IN ('P', 'O') THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0)
                )::numeric,
                2
            ) AS attendance_pct
        FROM period_attendance
        GROUP BY student_id, subject_id, semester;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron'
            ) THEN
                EXECUTE 'CREATE EXTENSION IF NOT EXISTS pg_cron';

                PERFORM cron.unschedule('create-next-year-audit-partition');
                PERFORM cron.schedule(
                    'create-next-year-audit-partition',
                    '0 0 1 12 *',
                    'SELECT create_next_year_audit_partition()'
                );
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not register audit partition cron job gracefully: %', SQLERRM;
        END$$;
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX idx_mv_student_semester_summary_pk
        ON mv_student_semester_summary (student_id, semester);
        """
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_student_semester_summary CASCADE;")
    op.execute(
        """
        CREATE MATERIALIZED VIEW mv_student_semester_summary AS
        SELECT
            student_id,
            semester,
            ROUND(AVG(marks), 2) as average_marks,
            COUNT(CASE WHEN result_status = 'F' THEN 1 END) as backlog_count,
            ROUND(AVG(CASE WHEN assessment_type = 'SEMESTER_EXAM' THEN marks END), 2) as exam_average
        FROM student_assessments
        GROUP BY student_id, semester;
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX idx_mv_student_semester_summary_pk
        ON mv_student_semester_summary (student_id, semester);
        """
    )

    op.execute("DROP TRIGGER IF EXISTS trg_update_student_assessment_grades ON student_assessments;")
    op.execute("DROP TRIGGER IF EXISTS trg_update_grades ON student_assessments;")

    # Do not recreate duplicate constraints or redundant indexes on downgrade.
