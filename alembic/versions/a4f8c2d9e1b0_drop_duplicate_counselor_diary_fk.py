"""drop_duplicate_counselor_diary_fk

Revision ID: a4f8c2d9e1b0
Revises: 9b3c1d2e4f6a
Create Date: 2026-05-26 16:10:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a4f8c2d9e1b0"
down_revision: Union[str, Sequence[str], None] = "9b3c1d2e4f6a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE counselor_diary "
        "DROP CONSTRAINT IF EXISTS fk_counselor_diary_counselor_id;"
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'counselor_diary'::regclass
                  AND conname = 'fk_counselor_diary_counselor_id'
            ) THEN
                ALTER TABLE counselor_diary
                ADD CONSTRAINT fk_counselor_diary_counselor_id
                FOREIGN KEY (counselor_id) REFERENCES staff(id) NOT VALID;
            END IF;
        END$$;
        """
    )
