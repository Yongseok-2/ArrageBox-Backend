import asyncpg

from app.core.settings import settings

_db_pool: asyncpg.Pool | None = None


async def init_db_pool() -> asyncpg.Pool:
    """애플리케이션 전역 DB 커넥션 풀을 초기화합니다."""
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    return _db_pool


def get_db_pool() -> asyncpg.Pool:
    """초기화된 전역 DB 커넥션 풀을 반환합니다."""
    if _db_pool is None:
        raise RuntimeError("DB pool is not initialized")
    return _db_pool


async def close_db_pool() -> None:
    """전역 DB 커넥션 풀을 종료합니다."""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
