from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Document


EXCLUDED_DIRS = {
    ".venv",
    "__pycache__",
    ".git",
}

EXCLUDED_FILES = {
    # local artifacts
    "product.db-wal",
    "product.db-shm",
}


def guess_kind(path: Path) -> str:
    ext = path.suffix.lower().lstrip(".")
    if ext in {"csv"}:
        return "csv"
    if ext in {"html", "htm"}:
        return "html"
    if ext in {"md"}:
        return "md"
    if ext in {"py"}:
        return "py"
    if ext in {"jpg", "jpeg", "png", "gif", "webp"}:
        return "image"
    if ext in {"db", "sqlite"}:
        return "db"
    return "other"


@dataclass(frozen=True)
class FileMeta:
    rel_path: str
    ext: str
    kind: str
    size_bytes: int
    mtime_unix: int


def iter_repo_files(repo_root: Path) -> list[FileMeta]:
    repo_root = repo_root.resolve()
    out: list[FileMeta] = []

    for root, dirs, files in os.walk(repo_root):
        # prune excluded dirs
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]

        root_path = Path(root)
        for name in files:
            if name in EXCLUDED_FILES:
                continue
            p = root_path / name
            # skip pyc
            if p.suffix.lower() in {".pyc"}:
                continue

            try:
                st = p.stat()
            except OSError:
                continue

            rel = p.resolve().relative_to(repo_root).as_posix()
            ext = p.suffix.lower().lstrip(".")
            out.append(
                FileMeta(
                    rel_path=rel,
                    ext=ext,
                    kind=guess_kind(p),
                    size_bytes=int(st.st_size),
                    mtime_unix=int(st.st_mtime),
                )
            )

    return out


async def reindex_documents(db: AsyncSession, repo_root: Path) -> dict:
    metas = iter_repo_files(repo_root)
    scanned_paths = {m.rel_path for m in metas}

    # Load existing docs into dict for upsert by rel_path
    existing = (await db.execute(select(Document))).scalars().all()
    by_path = {d.rel_path: d for d in existing}

    created = 0
    updated = 0
    deleted = 0

    for m in metas:
        d = by_path.get(m.rel_path)
        if not d:
            db.add(
                Document(
                    rel_path=m.rel_path,
                    kind=m.kind,
                    ext=m.ext,
                    size_bytes=m.size_bytes,
                    mtime_unix=m.mtime_unix,
                )
            )
            created += 1
            continue

        if d.size_bytes != m.size_bytes or d.mtime_unix != m.mtime_unix or d.kind != m.kind or d.ext != m.ext:
            d.size_bytes = m.size_bytes
            d.mtime_unix = m.mtime_unix
            d.kind = m.kind
            d.ext = m.ext
            updated += 1

    # Remove docs that no longer exist on disk
    for d in existing:
        if d.rel_path not in scanned_paths:
            await db.delete(d)
            deleted += 1

    await db.commit()
    return {"total_scanned": len(metas), "created": created, "updated": updated, "deleted": deleted}

