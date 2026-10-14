"""Construct storage adapters for the local Sandbox manager."""

from .config import ManagerConfig
from .repository_port import SessionRepository
from .sqlite_repository import SQLiteSessionRepository


def create_local_repository(config: ManagerConfig) -> SessionRepository:
    return SQLiteSessionRepository(
        config.repository_database_path,
        require_mount=config.repository_require_mount,
        max_pending_events=config.max_pending_events,
        lease_duration_seconds=config.session_lease_duration_seconds,
    )
