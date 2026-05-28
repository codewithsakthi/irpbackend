"""add_rbac_partitioning_calendar_and_semester_trigger

Revision ID: 5a24061f304b
Revises: d56e2be324c1
Create Date: 2026-05-26 12:09:33.431132

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5a24061f304b'
down_revision: Union[str, Sequence[str], None] = 'd56e2be324c1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop any pre-existing empty/leftover regular tables to prevent partition naming conflicts
    op.execute("DROP TABLE IF EXISTS audit_logs_default, audit_logs_assessments, audit_logs_attendance CASCADE;")

    # 1. RANGE PARTITIONING FOR AUDIT LOGS
    # Rename existing table
    op.rename_table('audit_logs', 'audit_logs_old')

    # Create new partitioned table
    op.execute("""
        CREATE TABLE audit_logs (
            id          BIGSERIAL,
            table_name  VARCHAR(50)  NOT NULL,
            record_id   BIGINT       NOT NULL,
            action      VARCHAR(10)  NOT NULL,
            old_values  JSONB,
            new_values  JSONB,
            changed_by  INTEGER      REFERENCES users(id),
            changed_at  TIMESTAMP    NOT NULL DEFAULT now(),
            PRIMARY KEY (id, changed_at)
        ) PARTITION BY RANGE (changed_at);
    """)

    # Create partitions
    op.execute("""
        CREATE TABLE audit_logs_2024 PARTITION OF audit_logs
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    """)
    op.execute("""
        CREATE TABLE audit_logs_2025 PARTITION OF audit_logs
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    """)
    op.execute("""
        CREATE TABLE audit_logs_2026 PARTITION OF audit_logs
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
    """)
    op.execute("""
        CREATE TABLE audit_logs_default PARTITION OF audit_logs DEFAULT;
    """)

    # Copy old data
    op.execute("""
        INSERT INTO audit_logs (id, table_name, record_id, action, old_values, new_values, changed_by, changed_at)
        SELECT id, table_name, record_id, action, old_values, new_values, changed_by, changed_at
        FROM audit_logs_old;
    """)

    # Reset sequence
    op.execute("""
        SELECT setval(pg_get_serial_sequence('audit_logs', 'id'), COALESCE(MAX(id), 1)) FROM audit_logs;
    """)

    # Drop old table
    op.drop_table('audit_logs_old')

    # Recreate index on parent (which propagates to partitions)
    op.execute("CREATE INDEX ON audit_logs (changed_at);")
    op.execute("CREATE INDEX ON audit_logs (table_name, record_id);")


    # 2. GRANULAR RBAC PERMISSIONS MODEL
    # Create tables
    op.execute("""
        CREATE TABLE permissions (
            id          SERIAL PRIMARY KEY,
            resource    VARCHAR(50)  NOT NULL,
            action      VARCHAR(20)  NOT NULL,
            description TEXT,
            UNIQUE (resource, action)
        );
    """)
    op.execute("""
        CREATE TABLE role_permissions (
            role_id       INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
            permission_id INTEGER NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
            PRIMARY KEY (role_id, permission_id)
        );
    """)

    # Seed permissions
    op.execute("""
        INSERT INTO permissions (resource, action) VALUES
            ('students',    'read'),
            ('students',    'write'),
            ('attendance',  'read'),
            ('attendance',  'write'),
            ('assessments', 'read'),
            ('assessments', 'write'),
            ('reports',     'read'),
            ('staff',       'read'),
            ('staff',       'write');
    """)

    # Seed permission-role mappings
    op.execute("""
        INSERT INTO role_permissions (role_id, permission_id)
        SELECT r.id, p.id FROM roles r, permissions p WHERE r.name = 'admin';
    """)
    op.execute("""
        INSERT INTO role_permissions (role_id, permission_id)
        SELECT r.id, p.id FROM roles r, permissions p
        WHERE r.name = 'staff' AND (
            (p.resource = 'students' AND p.action IN ('read', 'write')) OR
            (p.resource = 'attendance' AND p.action IN ('read', 'write')) OR
            (p.resource = 'assessments' AND p.action IN ('read', 'write')) OR
            (p.resource = 'reports' AND p.action = 'read') OR
            (p.resource = 'staff' AND p.action = 'read')
        );
    """)
    op.execute("""
        INSERT INTO role_permissions (role_id, permission_id)
        SELECT r.id, p.id FROM roles r, permissions p
        WHERE r.name = 'student' AND (
            (p.resource = 'students' AND p.action = 'read') OR
            (p.resource = 'attendance' AND p.action = 'read') OR
            (p.resource = 'assessments' AND p.action = 'read')
        );
    """)


    # 3. PROGRAM COLUMNS AND TRIGGER SEMESTER VALIDATION
    # Add columns
    op.execute("""
        ALTER TABLE programs
            ADD COLUMN degree_type      VARCHAR(20) NOT NULL DEFAULT 'undergraduate',
            ADD COLUMN duration_years   SMALLINT    NOT NULL DEFAULT 4,
            ADD COLUMN total_semesters  SMALLINT    NOT NULL DEFAULT 8;
    """)

    # Specialize MCA postgraduate duration/semester defaults
    op.execute("UPDATE programs SET degree_type = 'postgraduate', duration_years = 2, total_semesters = 4 WHERE code = 'MCA';")

    # Add check constraint
    op.execute("""
        ALTER TABLE students
            ADD CONSTRAINT chk_valid_semester
            CHECK (current_semester >= 1 AND current_semester <= 12);
    """)

    # Create plpgsql trigger function
    op.execute("""
        CREATE OR REPLACE FUNCTION check_student_semester()
        RETURNS TRIGGER AS $$
        DECLARE
            max_sem INTEGER;
        BEGIN
            SELECT total_semesters INTO max_sem
            FROM programs WHERE id = NEW.program_id;

            IF max_sem IS NOT NULL THEN
                IF NEW.current_semester < 1 OR NEW.current_semester > max_sem THEN
                    RAISE EXCEPTION 'current_semester % exceeds program max % for program_id %',
                        NEW.current_semester, max_sem, NEW.program_id;
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Bind trigger
    op.execute("""
        CREATE TRIGGER trg_validate_semester
        BEFORE INSERT OR UPDATE ON students
        FOR EACH ROW EXECUTE FUNCTION check_student_semester();
    """)


    # 4. ACADEMIC CALENDAR TABLE
    op.execute("""
        CREATE TABLE academic_calendar (
            id              SERIAL PRIMARY KEY,
            program_id      INTEGER      REFERENCES programs(id) ON DELETE CASCADE,
            academic_year   VARCHAR(20)  NOT NULL,
            semester        SMALLINT     NOT NULL,
            event_type      VARCHAR(30)  NOT NULL,
            title           VARCHAR(255) NOT NULL,
            start_date      DATE         NOT NULL,
            end_date        DATE         NOT NULL,
            description     TEXT,
            created_at      TIMESTAMP    DEFAULT now(),
            CONSTRAINT chk_date_order CHECK (end_date >= start_date)
        );
    """)

    op.execute("CREATE INDEX idx_acad_cal_year_sem ON academic_calendar (academic_year, semester);")
    op.execute("CREATE INDEX idx_acad_cal_type_start ON academic_calendar (event_type, start_date);")

    # Seed academic calendar data (fixed user's IA1 end date typo)
    op.execute("""
        INSERT INTO academic_calendar 
            (academic_year, semester, event_type, title, start_date, end_date)
        VALUES
            ('2025-26', 1, 'semester_start',    'Odd Semester Begins',   '2025-07-01', '2025-07-01'),
            ('2025-26', 1, 'exam_window',       'Internal Assessment 1', '2025-08-15', '2025-08-22'),
            ('2025-26', 1, 'exam_window',       'End Semester Exams',    '2025-11-10', '2025-11-30'),
            ('2025-26', 1, 'semester_end',      'Odd Semester Ends',     '2025-11-30', '2025-11-30'),
            ('2025-26', 1, 'holiday',           'Diwali Break',          '2025-10-20', '2025-10-24');
    """)


def downgrade() -> None:
    # 4. Drop academic calendar
    op.execute("DROP TABLE IF EXISTS academic_calendar CASCADE;")

    # 3. Drop trigger and programs columns
    op.execute("DROP TRIGGER IF EXISTS trg_validate_semester ON students;")
    op.execute("DROP FUNCTION IF EXISTS check_student_semester();")
    op.execute("ALTER TABLE students DROP CONSTRAINT IF EXISTS chk_valid_semester;")
    op.execute("""
        ALTER TABLE programs
            DROP COLUMN degree_type,
            DROP COLUMN duration_years,
            DROP COLUMN total_semesters;
    """)

    # 2. Drop RBAC permissions
    op.execute("DROP TABLE IF EXISTS role_permissions CASCADE;")
    op.execute("DROP TABLE IF EXISTS permissions CASCADE;")

    # 1. Restore non-partitioned audit_logs
    op.rename_table('audit_logs', 'audit_logs_partitioned')

    op.execute("""
        CREATE TABLE audit_logs (
            id          BIGSERIAL,
            table_name  VARCHAR(50)  NOT NULL,
            record_id   BIGINT       NOT NULL,
            action      VARCHAR(10)  NOT NULL,
            old_values  JSONB,
            new_values  JSONB,
            changed_by  INTEGER      REFERENCES users(id),
            changed_at  TIMESTAMP    NOT NULL DEFAULT now(),
            PRIMARY KEY (id, table_name)
        );
    """)

    op.execute("""
        INSERT INTO audit_logs (id, table_name, record_id, action, old_values, new_values, changed_by, changed_at)
        SELECT id, table_name, record_id, action, old_values, new_values, changed_by, changed_at
        FROM audit_logs_partitioned;
    """)

    op.execute("""
        SELECT setval(pg_get_serial_sequence('audit_logs', 'id'), COALESCE(MAX(id), 1)) FROM audit_logs;
    """)

    op.execute("DROP TABLE IF EXISTS audit_logs_2024 CASCADE;")
    op.execute("DROP TABLE IF EXISTS audit_logs_2025 CASCADE;")
    op.execute("DROP TABLE IF EXISTS audit_logs_2026 CASCADE;")
    op.execute("DROP TABLE IF EXISTS audit_logs_default CASCADE;")
    op.execute("DROP TABLE IF EXISTS audit_logs_partitioned CASCADE;")

    op.execute("CREATE INDEX idx_audit_table_record ON audit_logs (table_name, record_id);")
