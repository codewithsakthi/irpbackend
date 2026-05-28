import logging
from sqlalchemy import text
from app.core.database import engine


logger = logging.getLogger(__name__)

async def upgrade_db():
    logger.info("SPICS schema compatibility check started")
    async with engine.begin() as conn:
        # Ensure legacy/missed columns exist without requiring a full Alembic run.
        result = await conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='student_professional_profiles' 
              AND column_name IN (
                'github_access_token',
                'leetcode_cache_data',
                'leetcode_cache_expires_at'
              );
        """))
        existing_columns = [row[0] for row in result.fetchall()]

        if "github_access_token" not in existing_columns:
            logger.info("Adding column github_access_token")
            await conn.execute(
                text(
                    "ALTER TABLE student_professional_profiles "
                    "ADD COLUMN IF NOT EXISTS github_access_token TEXT;"
                )
            )
        else:
            logger.info("github_access_token already exists")
        
        if "leetcode_cache_data" not in existing_columns:
            logger.info("Adding column leetcode_cache_data")
            await conn.execute(
                text(
                    "ALTER TABLE student_professional_profiles "
                    "ADD COLUMN IF NOT EXISTS leetcode_cache_data JSONB;"
                )
            )
        else:
            logger.info("leetcode_cache_data already exists")

        if "leetcode_cache_expires_at" not in existing_columns:
            logger.info("Adding column leetcode_cache_expires_at")
            await conn.execute(
                text(
                    "ALTER TABLE student_professional_profiles "
                    "ADD COLUMN IF NOT EXISTS leetcode_cache_expires_at TIMESTAMP;"
                )
            )
        else:
            logger.info("leetcode_cache_expires_at already exists")
            
        # Ensure is_github_imported column exists in student_projects
        result = await conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='student_projects' 
              AND column_name = 'is_github_imported';
        """))
        if not result.fetchone():
            logger.info("Adding column is_github_imported to student_projects")
            await conn.execute(
                text(
                    "ALTER TABLE student_projects "
                    "ADD COLUMN IF NOT EXISTS is_github_imported "
                    "BOOLEAN NOT NULL DEFAULT FALSE;"
                )
            )
        else:
            logger.info("is_github_imported already exists")

        logger.info("SPICS schema compatibility check completed")
