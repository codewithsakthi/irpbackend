"""drop_duplicate_user_fks

Revision ID: b7d4e9a1c2f3
Revises: a4f8c2d9e1b0
Create Date: 2026-05-26 16:20:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b7d4e9a1c2f3"
down_revision: Union[str, Sequence[str], None] = "a4f8c2d9e1b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE staff DROP CONSTRAINT IF EXISTS staff_id_fkey;")
    op.execute("ALTER TABLE students DROP CONSTRAINT IF EXISTS students_id_fkey;")


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'staff'::regclass
                  AND conname = 'staff_id_fkey'
            ) THEN
                ALTER TABLE staff
                ADD CONSTRAINT staff_id_fkey
                FOREIGN KEY (id) REFERENCES users(id);
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'students'::regclass
                  AND conname = 'students_id_fkey'
            ) THEN
                ALTER TABLE students
                ADD CONSTRAINT students_id_fkey
                FOREIGN KEY (id) REFERENCES users(id);
            END IF;
        END$$;
        """
    )
