from __future__ import annotations

import csv
import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Company


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _norm_website(s: str | None) -> str:
    s = _norm(s).lower()
    s = s.removeprefix("http://").removeprefix("https://")
    return s.strip().strip("/")


async def import_companies_csv(db: AsyncSession, csv_path: Path, limit: int | None = None) -> dict:
    """
    Imports rows from Companies.csv into Company table.
    Dedup strategy:
      - website normalized is unique key (if present)
      - if website empty: insert (no uniqueness guarantee)
    """
    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))

    # Build set of already present websites (normalized).
    existing_websites: set[str] = set()
    for w in (await db.execute(select(Company.website).where(Company.website.is_not(None)))).scalars().all():
        if w:
            existing_websites.add(_norm_website(w))

    inserted = 0
    skipped = 0
    total = 0

    # Try utf-8-sig first (common for Excel), fallback to utf-8.
    text = None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            text = csv_path.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        # Last resort
        text = csv_path.read_text(encoding="cp1251", errors="replace")

    reader = csv.DictReader(text.splitlines())
    to_add: list[Company] = []
    for row in reader:
        total += 1
        if limit and total > limit:
            break

        icp = _norm(row.get("ICP"))
        name = _norm(row.get("Company"))
        website_norm = _norm_website(row.get("Website"))

        score = _norm(row.get("Score"))
        reasoning = _norm(row.get("Reasoning"))
        notes = _norm(row.get("Notes"))

        # Skip totally empty rows
        if not name and not website_norm:
            skipped += 1
            continue

        if website_norm and website_norm in existing_websites:
            skipped += 1
            continue

        c = Company(
            icp=icp,
            name=name,
            website=website_norm or None,
            score=score,
            reasoning=reasoning,
            notes=notes,
            raw_json=json.dumps(row, ensure_ascii=False),
        )
        to_add.append(c)
        inserted += 1
        if website_norm:
            existing_websites.add(website_norm)

        # Batch commits for speed/memory.
        if len(to_add) >= 1000:
            db.add_all(to_add)
            await db.commit()
            to_add.clear()

    if to_add:
        db.add_all(to_add)
        await db.commit()

    return {"total_rows": total, "inserted": inserted, "skipped": skipped, "path": str(csv_path)}

