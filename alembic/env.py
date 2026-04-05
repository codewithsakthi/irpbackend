import asyncio
from logging.config import fileConfig
import os
import sys

from sqlalchemy import pool

from alembic import context

# Add the app directory to the python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import engine and settings
from app.core.database import engine, Base, settings
from app.models import base # Ensure models are registered

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target_metadata
target_metadata = Base.metadata

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = settings.DATABASE_URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    # We use the existing engine from our database module
    # as it already handles SSL, connection arguments, and asyncpg correctly.
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    # We don't dispose the global engine here to avoid closing it for the app
    # but for alembic cli, it's fine.


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
