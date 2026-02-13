from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import User


async def get_current_user(request: Request, db: Annotated[AsyncSession, Depends(get_db)]) -> User:
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user = await db.get(User, int(user_id))
    if not user:
        request.session.clear()
        raise HTTPException(status_code=401, detail="Invalid session")
    return user


async def get_or_create_user_by_email(db: AsyncSession, email: str, role: str) -> User:
    email = email.strip().lower()
    role = (role or "bizdev").strip().lower()
    if role not in {"admin", "bizdev", "marketing", "outreach"}:
        role = "bizdev"

    existing = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if existing:
        # Allow role upgrade/downgrade for demo convenience.
        if existing.role != role:
            existing.role = role
            await db.commit()
            await db.refresh(existing)
        return existing

    user = User(email=email, role=role)
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user

