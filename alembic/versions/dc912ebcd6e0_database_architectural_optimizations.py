"""database_architectural_optimizations

Revision ID: dc912ebcd6e0
Revises: cae86a740304
Create Date: 2026-05-26 11:18:09.527986

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'dc912ebcd6e0'
down_revision: Union[str, Sequence[str], None] = 'cae86a740304'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Phase 1: Add New Tables and Columns ────────────────────────────────────
    
    # Create notifications table
    op.create_table(
        'notifications',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('recipient_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('channel', sa.String(length=50), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False),
        sa.Column('priority', sa.String(length=20), nullable=False),
        sa.Column('sender_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('read_at', sa.TIMESTAMP(), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(['recipient_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['sender_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_recipient_status_channel', 'notifications', ['recipient_id', 'status', 'channel'], unique=False)
    op.create_index(op.f('ix_notifications_recipient_id'), 'notifications', ['recipient_id'], unique=False)

    # Create student_enrollments table
    op.create_table(
        'student_enrollments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('student_id', sa.Integer(), nullable=False),
        sa.Column('subject_id', sa.Integer(), nullable=False),
        sa.Column('semester', sa.Integer(), nullable=False),
        sa.Column('academic_year', sa.String(length=20), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('enrolled_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.CheckConstraint("status IN ('active', 'dropped', 'completed')", name='chk_enrollment_status'),
        sa.ForeignKeyConstraint(['student_id'], ['students.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('student_id', 'subject_id', 'academic_year', name='uq_student_enrollment')
    )
    op.create_index(op.f('ix_student_enrollments_student_id'), 'student_enrollments', ['student_id'], unique=False)
    op.create_index(op.f('ix_student_enrollments_subject_id'), 'student_enrollments', ['subject_id'], unique=False)

    # Add soft-delete support to Students, Staff, Subjects
    op.add_column('students', sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('students', sa.Column('deleted_at', sa.TIMESTAMP(), nullable=True))
    op.create_index(op.f('ix_students_is_deleted'), 'students', ['is_deleted'], unique=False)

    op.add_column('staff', sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('staff', sa.Column('deleted_at', sa.TIMESTAMP(), nullable=True))
    op.create_index(op.f('ix_staff_is_deleted'), 'staff', ['is_deleted'], unique=False)

    op.add_column('subjects', sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('subjects', sa.Column('deleted_at', sa.TIMESTAMP(), nullable=True))
    op.create_index(op.f('ix_subjects_is_deleted'), 'subjects', ['is_deleted'], unique=False)

    # Add consolidated profile fields to Students
    op.add_column('students', sa.Column('address', sa.String(), nullable=True))
    op.add_column('students', sa.Column('pincode', sa.String(), nullable=True))
    op.add_column('students', sa.Column('phone_primary', sa.String(), nullable=True))
    op.add_column('students', sa.Column('phone_secondary', sa.String(), nullable=True))
    op.add_column('students', sa.Column('phone_tertiary', sa.String(), nullable=True))
    op.add_column('students', sa.Column('city', sa.String(), nullable=True))
    op.add_column('students', sa.Column('parent_guardian_name', sa.String(), nullable=True))
    op.add_column('students', sa.Column('occupation', sa.String(), nullable=True))
    op.add_column('students', sa.Column('parent_phone', sa.String(), nullable=True))
    op.add_column('students', sa.Column('father_name', sa.String(), nullable=True))
    op.add_column('students', sa.Column('mother_name', sa.String(), nullable=True))
    op.add_column('students', sa.Column('parent_occupation', sa.String(), nullable=True))
    op.add_column('students', sa.Column('parent_address', sa.String(), nullable=True))
    op.add_column('students', sa.Column('parent_email', sa.String(), nullable=True))
    op.add_column('students', sa.Column('emergency_contact_name', sa.String(), nullable=True))
    op.add_column('students', sa.Column('emergency_contact_phone', sa.String(), nullable=True))
    op.add_column('students', sa.Column('emergency_contact_relation', sa.String(), nullable=True))
    op.add_column('students', sa.Column('emergency_contact_address', sa.String(), nullable=True))
    op.add_column('students', sa.Column('emergency_contact_email', sa.String(), nullable=True))

    # Add start/end times to Timetable
    op.add_column('timetable', sa.Column('start_time', sa.Time(), nullable=True))
    op.add_column('timetable', sa.Column('end_time', sa.Time(), nullable=True))
    op.create_check_constraint('chk_timetable_times', 'timetable', 'start_time < end_time')

    # ── Phase 2: Transactional Data Migration ──────────────────────────────────
    
    # 1. Backfill Contact Info into Students
    op.execute("""
        UPDATE students s
        SET address = c.address,
            pincode = c.pincode,
            phone_primary = c.phone_primary,
            phone_secondary = c.phone_secondary,
            phone_tertiary = c.phone_tertiary,
            city = c.city
        FROM contact_info c
        WHERE s.id = c.student_id
    """)

    # 2. Backfill Family Details into Students
    op.execute("""
        UPDATE students s
        SET parent_guardian_name = f.parent_guardian_name,
            occupation = f.occupation,
            parent_phone = f.parent_phone,
            father_name = f.father_name,
            mother_name = f.mother_name,
            parent_occupation = f.parent_occupation,
            parent_address = f.parent_address,
            parent_email = f.parent_email,
            emergency_contact_name = f.emergency_contact_name,
            emergency_contact_phone = f.emergency_contact_phone,
            emergency_contact_relation = f.emergency_contact_relation,
            emergency_contact_address = f.emergency_contact_address,
            emergency_contact_email = f.emergency_contact_email
        FROM family_details f
        WHERE s.id = f.student_id
    """)

    # 3. Resolve Previous Academics Redundancy
    op.execute("""
        UPDATE previous_academics
        SET institution = school_name
        WHERE institution IS NULL AND school_name IS NOT NULL
    """)

    # 4. Backfill Timetable start_time and end_time
    op.execute("""
        UPDATE timetable
        SET start_time = CASE period
            WHEN 1 THEN '09:00:00'::time
            WHEN 2 THEN '10:00:00'::time
            WHEN 3 THEN '11:00:00'::time
            WHEN 4 THEN '12:00:00'::time
            WHEN 5 THEN '14:00:00'::time
            WHEN 6 THEN '15:00:00'::time
            WHEN 7 THEN '16:00:00'::time
            WHEN 8 THEN '17:00:00'::time
            ELSE '09:00:00'::time
        END,
        end_time = CASE period
            WHEN 1 THEN '10:00:00'::time
            WHEN 2 THEN '11:00:00'::time
            WHEN 3 THEN '12:00:00'::time
            WHEN 4 THEN '13:00:00'::time
            WHEN 5 THEN '15:00:00'::time
            WHEN 6 THEN '16:00:00'::time
            WHEN 7 THEN '17:00:00'::time
            WHEN 8 THEN '18:00:00'::time
            ELSE '10:00:00'::time
        END
    """)

    # 5. Populate Student Enrollments from historical marks and attendance
    op.execute("""
        INSERT INTO student_enrollments (student_id, subject_id, semester, academic_year, status)
        SELECT DISTINCT student_id, subject_id, semester, '2024-2025', 'active'
        FROM student_assessments
        UNION
        SELECT DISTINCT student_id, subject_id, semester, '2024-2025', 'active'
        FROM period_attendance
        ON CONFLICT DO NOTHING
    """)

    # 6. Declarative Table Partitioning for Audit Logs
    op.execute("ALTER TABLE audit_logs RENAME TO audit_logs_old")
    op.execute("""
        CREATE TABLE audit_logs (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            table_name VARCHAR(50) NOT NULL,
            record_id BIGINT NOT NULL,
            action VARCHAR(10) NOT NULL,
            old_values JSONB,
            new_values JSONB,
            changed_by INTEGER,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, table_name)
        ) PARTITION BY LIST (table_name)
    """)
    op.execute("CREATE TABLE audit_logs_attendance PARTITION OF audit_logs FOR VALUES IN ('period_attendance')")
    op.execute("CREATE TABLE audit_logs_assessments PARTITION OF audit_logs FOR VALUES IN ('student_assessments')")
    op.execute("CREATE TABLE audit_logs_default PARTITION OF audit_logs DEFAULT")
    op.execute("""
        INSERT INTO audit_logs (table_name, record_id, action, old_values, new_values, changed_by, changed_at)
        SELECT table_name, record_id, action, old_values::jsonb, new_values::jsonb, changed_by, changed_at
        FROM audit_logs_old
    """)
    op.drop_table("audit_logs_old")

    # ── Phase 3: Cleanup Obsolete Columns and Tables ───────────────────────────
    op.drop_table('contact_info')
    op.drop_table('family_details')
    op.drop_column('previous_academics', 'school_name')


def downgrade() -> None:
    # Recreate family_details table
    op.create_table(
        'family_details',
        sa.Column('student_id', sa.Integer(), primary_key=True),
        sa.Column('parent_guardian_name', sa.String(), nullable=True),
        sa.Column('occupation', sa.String(), nullable=True),
        sa.Column('parent_phone', sa.String(), nullable=True),
        sa.Column('father_name', sa.String(), nullable=True),
        sa.Column('mother_name', sa.String(), nullable=True),
        sa.Column('parent_occupation', sa.String(), nullable=True),
        sa.Column('parent_address', sa.String(), nullable=True),
        sa.Column('parent_email', sa.String(), nullable=True),
        sa.Column('emergency_contact_name', sa.String(), nullable=True),
        sa.Column('emergency_contact_phone', sa.String(), nullable=True),
        sa.Column('emergency_contact_relation', sa.String(), nullable=True),
        sa.Column('emergency_contact_address', sa.String(), nullable=True),
        sa.Column('emergency_contact_email', sa.String(), nullable=True)
    )

    # Recreate contact_info table
    op.create_table(
        'contact_info',
        sa.Column('student_id', sa.Integer(), primary_key=True),
        sa.Column('address', sa.String(), nullable=True),
        sa.Column('pincode', sa.String(), nullable=True),
        sa.Column('phone_primary', sa.String(), nullable=True),
        sa.Column('phone_secondary', sa.String(), nullable=True),
        sa.Column('phone_tertiary', sa.String(), nullable=True),
        sa.Column('email', sa.String(), nullable=True),
        sa.Column('city', sa.String(), nullable=True)
    )

    # Restore previous_academics school_name column
    op.add_column('previous_academics', sa.Column('school_name', sa.String(), nullable=True))

    # Restore data back to separate tables
    op.execute("""
        INSERT INTO contact_info (student_id, address, pincode, phone_primary, phone_secondary, phone_tertiary, email, city)
        SELECT id, address, pincode, phone_primary, phone_secondary, phone_tertiary, email, city
        FROM students
    """)

    op.execute("""
        INSERT INTO family_details (student_id, parent_guardian_name, occupation, parent_phone, father_name, mother_name,
                                    parent_occupation, parent_address, parent_email, emergency_contact_name, emergency_contact_phone,
                                    emergency_contact_relation, emergency_contact_address, emergency_contact_email)
        SELECT id, parent_guardian_name, occupation, parent_phone, father_name, mother_name,
               parent_occupation, parent_address, parent_email, emergency_contact_name, emergency_contact_phone,
               emergency_contact_relation, emergency_contact_address, emergency_contact_email
        FROM students
    """)

    op.execute("""
        UPDATE previous_academics
        SET school_name = institution
    """)

    # Drop partitioning and return audit_logs to a simple table
    op.execute("ALTER TABLE audit_logs RENAME TO audit_logs_partitioned")
    op.execute("""
        CREATE TABLE audit_logs (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(50) NOT NULL,
            record_id BIGINT NOT NULL,
            action VARCHAR(10) NOT NULL,
            old_values JSON,
            new_values JSON,
            changed_by INTEGER,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("""
        INSERT INTO audit_logs (table_name, record_id, action, old_values, new_values, changed_by, changed_at)
        SELECT table_name, record_id, action, old_values::json, new_values::json, changed_by, changed_at
        FROM audit_logs_partitioned
    """)
    op.execute("DROP TABLE audit_logs_attendance")
    op.execute("DROP TABLE audit_logs_assessments")
    op.execute("DROP TABLE audit_logs_default")
    op.execute("DROP TABLE audit_logs_partitioned")

    # Drop optimization columns/tables
    op.drop_column('timetable', 'end_time')
    op.drop_column('timetable', 'start_time')

    op.drop_column('subjects', 'deleted_at')
    op.drop_column('subjects', 'is_deleted')

    op.drop_column('staff', 'deleted_at')
    op.drop_column('staff', 'is_deleted')

    # Drop consolidated columns from students
    for col in [
        'address', 'pincode', 'phone_primary', 'phone_secondary', 'phone_tertiary', 'city',
        'parent_guardian_name', 'occupation', 'parent_phone', 'father_name', 'mother_name',
        'parent_occupation', 'parent_address', 'parent_email', 'emergency_contact_name',
        'emergency_contact_phone', 'emergency_contact_relation', 'emergency_contact_address',
        'emergency_contact_email', 'is_deleted', 'deleted_at'
    ]:
        op.drop_column('students', col)

    op.drop_table('student_enrollments')
    op.drop_table('notifications')
