"""normalize_student_family_details

Revision ID: b35b34fcc30e
Revises: dc912ebcd6e0
Create Date: 2026-05-26 11:24:39.686565

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b35b34fcc30e'
down_revision: Union[str, Sequence[str], None] = 'dc912ebcd6e0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create family_details table
    op.create_table(
        'family_details',
        sa.Column('student_id', sa.Integer(), nullable=False),
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
        sa.Column('emergency_contact_email', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['student_id'], ['students.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('student_id')
    )

    # 2. Copy the data from students table to family_details table
    op.execute("""
        INSERT INTO family_details (
            student_id, parent_guardian_name, occupation, parent_phone, father_name, mother_name,
            parent_occupation, parent_address, parent_email, emergency_contact_name, emergency_contact_phone,
            emergency_contact_relation, emergency_contact_address, emergency_contact_email
        )
        SELECT 
            id, parent_guardian_name, occupation, parent_phone, father_name, mother_name,
            parent_occupation, parent_address, parent_email, emergency_contact_name, emergency_contact_phone,
            emergency_contact_relation, emergency_contact_address, emergency_contact_email
        FROM students
    """)

    # 3. Drop the family details columns from students table
    for col in [
        'parent_guardian_name', 'occupation', 'parent_phone', 'father_name', 'mother_name',
        'parent_occupation', 'parent_address', 'parent_email', 'emergency_contact_name',
        'emergency_contact_phone', 'emergency_contact_relation', 'emergency_contact_address',
        'emergency_contact_email'
    ]:
        op.drop_column('students', col)


def downgrade() -> None:
    # 1. Add columns back to students table
    for col in [
        'parent_guardian_name', 'occupation', 'parent_phone', 'father_name', 'mother_name',
        'parent_occupation', 'parent_address', 'parent_email', 'emergency_contact_name',
        'emergency_contact_phone', 'emergency_contact_relation', 'emergency_contact_address',
        'emergency_contact_email'
    ]:
        op.add_column('students', sa.Column(col, sa.String(), nullable=True))

    # 2. Copy the data from family_details table to students table
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

    # 3. Drop family_details table
    op.drop_table('family_details')
