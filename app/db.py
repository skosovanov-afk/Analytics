from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.settings import get_database_url


def make_engine() -> AsyncEngine:
    return create_async_engine(get_database_url(), echo=False, future=True)


engine: AsyncEngine = make_engine()
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    async with SessionLocal() as session:
        yield session

