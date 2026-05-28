"""production_hardening_indexes_views_and_partition_maintenance

Revision ID: 7e1f78e38cf8
Revises: 5a24061f304b
Create Date: 2026-05-26 12:22:00.226589

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7e1f78e38cf8'
down_revision: Union[str, Sequence[str], None] = '5a24061f304b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. COMPOSITE PERFORMANCE INDEXES
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_period_attendance_student_date_sem 
        ON period_attendance (student_id, date, semester);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_sa_student_semester_hardened 
        ON student_assessments (student_id, semester);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_audit_logs_table_record_changed 
        ON audit_logs (table_name, record_id, changed_at);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_student_subject_enrollment_student_year 
        ON student_subject_enrollment (student_id, academic_year);
    """)


    # 2. DECLARATIVE MATERIALIZED VIEWS WITH UNIQUE INDEXES
    # 2.1 Student Attendance Aggregate per Subject/Semester
    op.execute("""
        CREATE MATERIALIZED VIEW mv_student_attendance_aggregate AS
        SELECT 
            student_id,
            subject_id,
            semester,
            COUNT(*) as total_periods,
            SUM(CASE WHEN status IN ('P', 'O') THEN 1 ELSE 0 END) as present_periods,
            ROUND(
                (SUM(CASE WHEN status IN ('P', 'O') THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100), 
                2
            ) as attendance_percentage
        FROM period_attendance
        GROUP BY student_id, subject_id, semester;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_mv_student_attendance_aggregate_pk 
        ON mv_student_attendance_aggregate (student_id, subject_id, semester);
    """)

    # 2.2 Student Semester Assessment Summaries
    op.execute("""
        CREATE MATERIALIZED VIEW mv_student_semester_summary AS
        SELECT 
            student_id,
            semester,
            ROUND(AVG(marks), 2) as average_marks,
            COUNT(CASE WHEN result_status = 'F' THEN 1 END) as backlog_count,
            ROUND(AVG(CASE WHEN assessment_type = 'SEMESTER_EXAM' THEN marks END), 2) as exam_average
        FROM student_assessments
        GROUP BY student_id, semester;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_mv_student_semester_summary_pk 
        ON mv_student_semester_summary (student_id, semester);
    """)

    # 2.3 Student Placement Readiness Aggregates
    op.execute("""
        CREATE MATERIALIZED VIEW mv_placement_readiness_summary AS
        SELECT 
            sc.student_id,
            s.roll_no,
            s.name,
            s.batch,
            s.program_id,
            sc.placement_score,
            sc.placement_probability,
            sc.academic_score,
            sc.technical_score,
            sc.communication_score,
            sc.computed_at
        FROM student_capability_scores sc
        JOIN students s ON sc.student_id = s.id;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_mv_placement_readiness_summary_pk 
        ON mv_placement_readiness_summary (student_id);
    """)

    # 2.4 SPI Rankings
    op.execute("""
        CREATE MATERIALIZED VIEW mv_spi_rankings AS
        SELECT 
            sc.student_id,
            s.roll_no,
            s.name,
            s.program_id,
            s.batch,
            sc.spi_score,
            sc.profile_type,
            RANK() OVER (ORDER BY sc.spi_score DESC) as global_rank,
            RANK() OVER (PARTITION BY s.program_id ORDER BY sc.spi_score DESC) as program_rank,
            RANK() OVER (PARTITION BY s.program_id, s.batch ORDER BY sc.spi_score DESC) as cohort_rank,
            sc.computed_at
        FROM student_capability_scores sc
        JOIN students s ON sc.student_id = s.id;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_mv_spi_rankings_pk 
        ON mv_spi_rankings (student_id);
    """)

    # 2.5 Risk Tier Distributions
    op.execute("""
        CREATE MATERIALIZED VIEW mv_risk_distribution AS
        WITH risk_calc AS (
            SELECT 
                sc.student_id,
                s.roll_no,
                s.name,
                s.program_id,
                s.batch,
                ROUND(
                    (100.0 - sc.consistency_score) * 0.30 +
                    (100.0 - sc.academic_score) * 0.30 +
                    (100.0 - sc.placement_probability) * 0.40,
                    2
                ) as risk_score
            FROM student_capability_scores sc
            JOIN students s ON sc.student_id = s.id
        )
        SELECT 
            student_id,
            roll_no,
            name,
            program_id,
            batch,
            risk_score,
            CASE 
                WHEN risk_score >= 70 THEN 'Critical'
                WHEN risk_score >= 55 THEN 'High'
                WHEN risk_score >= 35 THEN 'Moderate'
                ELSE 'Low'
            END as risk_level
        FROM risk_calc;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_mv_risk_distribution_pk 
        ON mv_risk_distribution (student_id);
    """)


    # 3. HELPER MAINTENANCE PL/pgSQL FUNCTIONS
    # 3.1 Next Year Audit Log Partition Automation
    op.execute("""
        CREATE OR REPLACE FUNCTION create_next_year_audit_partition()
        RETURNS VOID AS $$
        DECLARE
            next_year INT;
            part_table TEXT;
            start_date TEXT;
            end_date TEXT;
        BEGIN
            next_year := EXTRACT(YEAR FROM CURRENT_DATE)::INT + 1;
            part_table := 'audit_logs_' || next_year;
            start_date := next_year || '-01-01';
            end_date := (next_year + 1) || '-01-01';
            
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name = part_table
            ) THEN
                EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF audit_logs FOR VALUES FROM (%L) TO (%L)', part_table, start_date, end_date);
                RAISE NOTICE 'Partition % created successfully.', part_table;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # 3.2 Materialized Views Concurrent Refresh Helper
    op.execute("""
        CREATE OR REPLACE FUNCTION refresh_all_materialized_views(concurrent BOOLEAN DEFAULT TRUE)
        RETURNS VOID AS $$
        BEGIN
            IF concurrent THEN
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_student_attendance_aggregate;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_student_semester_summary;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_placement_readiness_summary;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_spi_rankings;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_risk_distribution;
            ELSE
                REFRESH MATERIALIZED VIEW mv_student_attendance_aggregate;
                REFRESH MATERIALIZED VIEW mv_student_semester_summary;
                REFRESH MATERIALIZED VIEW mv_placement_readiness_summary;
                REFRESH MATERIALIZED VIEW mv_spi_rankings;
                REFRESH MATERIALIZED VIEW mv_risk_distribution;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """)


    # 4. pg_cron SCHEDULER REGISTRATION
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron'
            ) THEN
                EXECUTE 'CREATE EXTENSION IF NOT EXISTS pg_cron';
                
                -- Unschedule if pre-existing to avoid duplicates
                PERFORM cron.unschedule('create-next-year-audit-partition');
                PERFORM cron.unschedule('refresh-materialized-views');
                
                -- Schedule dynamic partition creation every December 1st at midnight
                PERFORM cron.schedule('create-next-year-audit-partition', '0 0 1 12 *', 'SELECT create_next_year_audit_partition()');
                
                -- Schedule concurrent materialized view refresh every hour on the hour
                PERFORM cron.schedule('refresh-materialized-views', '0 * * * *', 'SELECT refresh_all_materialized_views(true)');
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not register pg_cron jobs gracefully: %', SQLERRM;
        END$$;
    """)


def downgrade() -> None:
    # 4. Remove cron jobs if pg_cron is loaded
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'
            ) THEN
                PERFORM cron.unschedule('create-next-year-audit-partition');
                PERFORM cron.unschedule('refresh-materialized-views');
            END IF;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END$$;
    """)

    # 3. Drop maintenance functions
    op.execute("DROP FUNCTION IF EXISTS refresh_all_materialized_views(BOOLEAN);")
    op.execute("DROP FUNCTION IF EXISTS create_next_year_audit_partition();")

    # 2. Drop Materialized Views
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_risk_distribution CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_spi_rankings CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_placement_readiness_summary CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_student_semester_summary CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_student_attendance_aggregate CASCADE;")

    # 1. Drop Composite Indexes
    op.execute("DROP INDEX IF EXISTS idx_period_attendance_student_date_sem;")
    op.execute("DROP INDEX IF EXISTS idx_sa_student_semester_hardened;")
    op.execute("DROP INDEX IF EXISTS idx_audit_logs_table_record_changed;")
    op.execute("DROP INDEX IF EXISTS idx_student_subject_enrollment_student_year;")
