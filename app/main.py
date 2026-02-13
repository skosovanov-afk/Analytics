from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.responses import Response
from starlette.middleware.sessions import SessionMiddleware

from app.auth import get_current_user, get_or_create_user_by_email
from app.db import engine, get_db
from app.file_indexer import reindex_documents
from app.importers import import_companies_csv
from app.knowledge import enrich_and_write_hypothesis_card, write_hypothesis_card
from app.models import Base, Company, Document, Hypothesis, ICP, SubVertical, TAL, TALAccount, VPPoint, Vertical, Call, User
from app.settings import get_secret_key
from app.supabase_client import insert_row


REPO_ROOT = Path(__file__).resolve().parents[1]
templates = Jinja2Templates(directory=str(REPO_ROOT / "templates"))


def parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


async def init_db() -> None:
    """Инициализация схемы БД.

    - Для SQLite выполняем PRAGMA/ALTER TABLE-миграции.
    - Для Postgres (Supabase) просто вызываем create_all без SQLite-специфики.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # SQLite-специфичные миграции – выполняем только когда драйвер sqlite
        sync_engine = conn.sync_engine
        if sync_engine.dialect.name != "sqlite":
            return

        # One-time safety migration: early schema used UNIQUE(website) which breaks on empty websites.
        # If a unique index exists on companies, drop & recreate that table.
        try:
            rows = (await conn.exec_driver_sql("PRAGMA index_list('companies')")).fetchall()
            has_unique = any(int(r[2]) == 1 for r in rows)  # (seq, name, unique, origin, partial)
            if has_unique:
                await conn.exec_driver_sql("DROP TABLE IF EXISTS companies")
                await conn.run_sync(Base.metadata.create_all)
        except Exception:
            # If anything goes wrong, don't block startup.
            pass

        # Lightweight migrations for existing tables (SQLite doesn't auto-add columns on create_all)
        async def ensure_columns(table: str, columns_sql: dict[str, str]) -> None:
            try:
                info = (await conn.exec_driver_sql(f"PRAGMA table_info('{table}')")).fetchall()
                existing_cols = {row[1] for row in info}  # (cid, name, type, notnull, dflt_value, pk)
                for col, ddl in columns_sql.items():
                    if col not in existing_cols:
                        await conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            except Exception:
                # Don't block startup; worst case features are unavailable until db reset.
                return

        await ensure_columns(
            "hypotheses",
            {
                "vp_point_id": "vp_point_id INTEGER",
                "icp_id": "icp_id INTEGER",
                "sub_vertical_id": "sub_vertical_id INTEGER",
                "pain": "pain TEXT DEFAULT ''",
                "expected_signal": "expected_signal TEXT DEFAULT ''",
                "disqualifiers": "disqualifiers TEXT DEFAULT ''",
                "decision": "decision VARCHAR(32) DEFAULT 'open'",
                "decision_notes": "decision_notes TEXT DEFAULT ''",
            },
        )

        await ensure_columns(
            "calls",
            {
                "hypothesis_id": "hypothesis_id INTEGER",
                "tal_account_id": "tal_account_id INTEGER",
                "company_id": "company_id INTEGER",
                # legacy columns (existing product.db may have different schema)
                "call_date": "call_date DATE",
                "company": "company VARCHAR(200) DEFAULT ''",
                "contact": "contact VARCHAR(200) DEFAULT ''",
                "source": "source VARCHAR(100) DEFAULT ''",
                "summary": "summary TEXT DEFAULT ''",
                "transcript_url": "transcript_url VARCHAR(500) DEFAULT ''",
                "pain_confirmed": "pain_confirmed BOOLEAN DEFAULT 0",
                "severity": "severity INTEGER DEFAULT 0",
                "interest": "interest INTEGER DEFAULT 0",
                "follow_up": "follow_up INTEGER DEFAULT 0",
                "disqualifier": "disqualifier VARCHAR(200) DEFAULT ''",
                "created_at": "created_at DATETIME",
            },
        )


def safe_resolve_under_repo(rel_posix_path: str) -> Path | None:
    """
    Prevent path traversal: only allow files under repo root.
    rel_posix_path stored with forward slashes.
    """
    try:
        rel = Path(rel_posix_path)
        target = (REPO_ROOT / rel).resolve()
        if REPO_ROOT.resolve() in target.parents or target == REPO_ROOT.resolve():
            return target
        return None
    except Exception:
        return None


def read_text_preview(path: Path, max_bytes: int = 200_000) -> str | None:
    # Only for reasonably "text-like" files
    if path.suffix.lower() not in {".md", ".txt", ".csv", ".html", ".htm", ".py", ".json"}:
        return None
    try:
        data = path.read_bytes()[:max_bytes]
    except OSError:
        return None
    # Try utf-8, fallback to cp1251 with replacement (common on RU Windows exports)
    for enc in ("utf-8", "utf-8-sig"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("cp1251", errors="replace")


app = FastAPI(title="Product — Hypotheses & Calls MVP")
app.add_middleware(SessionMiddleware, secret_key=get_secret_key())


@app.on_event("startup")
async def _startup() -> None:
    await init_db()


def wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept or "*/*" in accept or not accept


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user_id = request.session.get("user_id")
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "user_id": user_id},
    )


@app.get("/favicon.ico")
async def favicon():
    # Avoid noisy 404s in logs; MVP doesn't ship a favicon yet.
    return Response(status_code=204)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    email: str = Form(...),
    role: str = Form("bizdev"),
):
    user = await get_or_create_user_by_email(db, email=email, role=role)
    request.session["user_id"] = user.id
    return RedirectResponse(url="/hypotheses", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@app.get("/hypotheses", response_class=HTMLResponse)
async def hypotheses_list(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    q = (
        select(Hypothesis)
        .options(selectinload(Hypothesis.vp_point), selectinload(Hypothesis.icp), selectinload(Hypothesis.sub_vertical))
        .order_by(Hypothesis.id.desc())
    )
    if user.role != "admin":
        q = q.where(Hypothesis.owner_user_id == user.id)
    rows = (await db.execute(q)).scalars().all()
    return templates.TemplateResponse(
        "hypotheses_list.html",
        {"request": request, "user": user, "hypotheses": rows},
    )


@app.get("/hypotheses/new", response_class=HTMLResponse)
async def hypotheses_new_page(request: Request, db: Annotated[AsyncSession, Depends(get_db)]):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    vp_points = (await db.execute(select(VPPoint).order_by(VPPoint.name))).scalars().all()
    icps = (await db.execute(select(ICP).order_by(ICP.name))).scalars().all()
    sub_verticals = (
        await db.execute(select(SubVertical).order_by(SubVertical.id.desc()))
    ).scalars().all()
    return templates.TemplateResponse(
        "hypothesis_new.html",
        {"request": request, "user": user, "error": None, "vp_points": vp_points, "icps": icps, "sub_verticals": sub_verticals},
    )


@app.post("/hypotheses/new")
async def hypotheses_new(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    title: str = Form(...),
    vp_point_id: str = Form(""),
    icp_id: str = Form(""),
    sub_vertical_id: str = Form(""),
    pain: str = Form(""),
    expected_signal: str = Form(""),
    disqualifiers: str = Form(""),
    # legacy fields remain optional
    segment: str = Form(""),
    problem: str = Form(""),
    assumption: str = Form(""),
    channel: str = Form(""),
    success_metric: str = Form(""),
    minimal_signal: str = Form(""),
    status: str = Form("draft"),
    start_date: str = Form(""),
    end_date: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = Hypothesis(
        owner_user_id=user.id,
        title=title.strip(),
        vp_point_id=int(vp_point_id) if vp_point_id.strip().isdigit() else None,
        icp_id=int(icp_id) if icp_id.strip().isdigit() else None,
        sub_vertical_id=int(sub_vertical_id) if sub_vertical_id.strip().isdigit() else None,
        pain=pain.strip(),
        expected_signal=expected_signal.strip(),
        disqualifiers=disqualifiers.strip(),
        segment=segment.strip(),
        problem=problem.strip(),
        assumption=assumption.strip(),
        channel=channel.strip(),
        success_metric=success_metric.strip(),
        minimal_signal=minimal_signal.strip(),
        status=status.strip() or "draft",
        start_date=parse_date(start_date),
        end_date=parse_date(end_date),
    )
    db.add(h)
    await db.commit()
    await db.refresh(h)
    # use enriched card if possible (includes TAL/call facts), fallback to base
    try:
        await enrich_and_write_hypothesis_card(REPO_ROOT, db, h)
    except Exception:
        write_hypothesis_card(REPO_ROOT, h)
    return RedirectResponse(url=f"/hypotheses/{h.id}", status_code=303)


@app.get("/vp", response_class=HTMLResponse)
async def vp_points_list(request: Request, db: Annotated[AsyncSession, Depends(get_db)]):
    try:
        await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    items = (await db.execute(select(VPPoint).order_by(VPPoint.id.desc()))).scalars().all()
    return templates.TemplateResponse("vp_points_list.html", {"request": request, "items": items})


@app.post("/vp/new")
async def vp_points_new(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    name: str = Form(...),
    job_to_be_done: str = Form(""),
    pain_friction: str = Form(""),
    outcome_metric: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    if user.role != "admin":
        return RedirectResponse(url="/vp", status_code=303)

    db.add(
        VPPoint(
            name=name.strip(),
            job_to_be_done=job_to_be_done.strip(),
            pain_friction=pain_friction.strip(),
            outcome_metric=outcome_metric.strip(),
        )
    )
    await db.commit()
    return RedirectResponse(url="/vp", status_code=303)


@app.get("/icp", response_class=HTMLResponse)
async def icp_list(request: Request, db: Annotated[AsyncSession, Depends(get_db)]):
    try:
        await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    items = (await db.execute(select(ICP).order_by(ICP.id.desc()))).scalars().all()
    return templates.TemplateResponse("icp_list.html", {"request": request, "items": items})


@app.post("/icp/new")
async def icp_new(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    name: str = Form(...),
    role: str = Form(""),
    scale: str = Form(""),
    decision_context: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    if user.role != "admin":
        return RedirectResponse(url="/icp", status_code=303)

    db.add(ICP(name=name.strip(), role=role.strip(), scale=scale.strip(), decision_context=decision_context.strip()))
    await db.commit()
    return RedirectResponse(url="/icp", status_code=303)


@app.get("/verticals", response_class=HTMLResponse)
async def verticals_list(request: Request, db: Annotated[AsyncSession, Depends(get_db)]):
    try:
        await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    verticals = (
        await db.execute(select(Vertical).options(selectinload(Vertical.subs)).order_by(Vertical.id.desc()))
    ).scalars().unique().all()
    return templates.TemplateResponse("verticals_list.html", {"request": request, "verticals": verticals})


@app.post("/verticals/new")
async def verticals_new(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    name: str = Form(...),
    sub_name: str = Form(""),
    description: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    if user.role != "admin":
        return RedirectResponse(url="/verticals", status_code=303)

    v = Vertical(name=name.strip(), description=description.strip())
    db.add(v)
    await db.commit()
    await db.refresh(v)
    if sub_name.strip():
        db.add(SubVertical(vertical_id=v.id, name=sub_name.strip(), description=""))
        await db.commit()
    return RedirectResponse(url="/verticals", status_code=303)


@app.post("/verticals/{vertical_id}/sub/new")
async def subvertical_new(
    request: Request,
    vertical_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    name: str = Form(""),
    description: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    if user.role != "admin":
        return RedirectResponse(url="/verticals", status_code=303)
    if not name.strip():
        return RedirectResponse(url="/verticals", status_code=303)
    db.add(SubVertical(vertical_id=vertical_id, name=name.strip(), description=description.strip()))
    await db.commit()
    return RedirectResponse(url="/verticals", status_code=303)


async def load_hypothesis_for_user(db: AsyncSession, user: User, hypothesis_id: int) -> Hypothesis | None:
    q = (
        select(Hypothesis)
        .where(Hypothesis.id == hypothesis_id)
        .options(selectinload(Hypothesis.vp_point), selectinload(Hypothesis.icp), selectinload(Hypothesis.sub_vertical))
    )
    h = (await db.execute(q)).scalar_one_or_none()
    if not h:
        return None
    if user.role != "admin" and h.owner_user_id != user.id:
        return None
    return h


@app.get("/hypotheses/{hypothesis_id}", response_class=HTMLResponse)
async def hypothesis_detail(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    # Find existing card by prefix "<id>-"
    out_dir = REPO_ROOT / "knowledge" / "hypotheses"
    card = None
    if out_dir.exists():
        for p in out_dir.glob(f"{h.id}-*.md"):
            card = p.name
            break

    return templates.TemplateResponse(
        "hypothesis_detail.html",
        {"request": request, "user": user, "h": h, "card_filename": card},
    )


@app.post("/hypotheses/{hypothesis_id}/refresh_card")
async def hypothesis_refresh_card(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    try:
        await enrich_and_write_hypothesis_card(REPO_ROOT, db, h)
    except Exception:
        write_hypothesis_card(REPO_ROOT, h)
    return RedirectResponse(url=f"/hypotheses/{hypothesis_id}", status_code=303)


@app.get("/hypotheses/{hypothesis_id}/tal", response_class=HTMLResponse)
async def tal_detail(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    tal = (await db.execute(select(TAL).where(TAL.hypothesis_id == h.id))).scalar_one_or_none()
    if not tal:
        tal = TAL(hypothesis_id=h.id, owner_user_id=h.owner_user_id, name=f"TAL-H-{h.id}")
        db.add(tal)
        await db.commit()
        await db.refresh(tal)

    accounts = (
        await db.execute(
            select(TALAccount)
            .where(TALAccount.tal_id == tal.id)
            .options(selectinload(TALAccount.company))
            .order_by(TALAccount.id.desc())
        )
    ).scalars().all()

    return templates.TemplateResponse(
        "tal_detail.html",
        {"request": request, "user": user, "h": h, "tal": tal, "accounts": accounts},
    )


@app.post("/hypotheses/{hypothesis_id}/tal/add")
async def tal_add(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    company_id: str = Form(""),
    fit_reason: str = Form(""),
    pain_hint: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    tal = (await db.execute(select(TAL).where(TAL.hypothesis_id == h.id))).scalar_one_or_none()
    if not tal:
        tal = TAL(hypothesis_id=h.id, owner_user_id=h.owner_user_id, name=f"TAL-H-{h.id}")
        db.add(tal)
        await db.commit()
        await db.refresh(tal)

    if not company_id.strip().isdigit():
        return RedirectResponse(url=f"/hypotheses/{h.id}/tal", status_code=303)
    cid = int(company_id.strip())
    company = await db.get(Company, cid)
    if not company:
        return RedirectResponse(url=f"/hypotheses/{h.id}/tal", status_code=303)

    # dedupe by unique constraint; ignore if exists
    existing = (
        await db.execute(select(TALAccount).where(TALAccount.tal_id == tal.id, TALAccount.company_id == cid))
    ).scalar_one_or_none()
    if not existing:
        db.add(
            TALAccount(
                tal_id=tal.id,
                company_id=cid,
                fit_reason=fit_reason.strip(),
                pain_hint=pain_hint.strip(),
            )
        )
        await db.commit()
    return RedirectResponse(url=f"/hypotheses/{h.id}/tal", status_code=303)


@app.get("/hypotheses/{hypothesis_id}/calls", response_class=HTMLResponse)
async def calls_list(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    tal = (await db.execute(select(TAL).where(TAL.hypothesis_id == h.id))).scalar_one_or_none()
    tal_accounts = []
    if tal:
        tal_accounts = (
            await db.execute(
                select(TALAccount)
                .where(TALAccount.tal_id == tal.id)
                .options(selectinload(TALAccount.company))
                .order_by(TALAccount.id.desc())
            )
        ).scalars().all()

    calls = (
        await db.execute(
            select(Call)
            .where(Call.hypothesis_id == h.id)
            .options(selectinload(Call.company_ref))
            .order_by(Call.id.desc())
        )
    ).scalars().all()
    return templates.TemplateResponse(
        "calls_list.html",
        {"request": request, "user": user, "h": h, "calls": calls, "tal_accounts": tal_accounts},
    )


@app.post("/hypotheses/{hypothesis_id}/calls/new")
async def calls_new(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    call_date: str = Form(""),
    tal_account_id: str = Form(""),
    summary: str = Form(""),
    transcript_url: str = Form(""),
    pain_confirmed: str = Form(""),
    severity: str = Form("0"),
    interest: str = Form(""),
    follow_up: str = Form(""),
    disqualifier: str = Form(""),
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    ta = None
    company_id = None
    if tal_account_id.strip().isdigit():
        ta = await db.get(TALAccount, int(tal_account_id.strip()))
        if ta:
            company_id = ta.company_id

    c = Call(
        owner_user_id=user.id,
        hypothesis_id=h.id,
        tal_account_id=ta.id if ta else None,
        company_id=company_id,
        call_date=parse_date(call_date),
        summary=summary.strip(),
        transcript_url=transcript_url.strip(),
        pain_confirmed=bool(pain_confirmed),
        interest=bool(interest),
        follow_up=bool(follow_up),
        severity=int(severity) if severity.strip().isdigit() else 0,
        disqualifier=disqualifier.strip(),
    )
    db.add(c)
    await db.commit()
    return RedirectResponse(url=f"/hypotheses/{h.id}/calls", status_code=303)


@app.get("/hypotheses/{hypothesis_id}/metrics", response_class=HTMLResponse)
async def hypothesis_metrics(
    request: Request,
    hypothesis_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    h = await load_hypothesis_for_user(db, user, hypothesis_id)
    if not h:
        return RedirectResponse(url="/hypotheses", status_code=303)

    calls = (await db.execute(select(Call).where(Call.hypothesis_id == h.id).order_by(Call.id.asc()))).scalars().all()
    total = len(calls)
    pain_n = sum(1 for c in calls if c.pain_confirmed)
    interest_n = sum(1 for c in calls if c.interest)
    follow_n = sum(1 for c in calls if c.follow_up)

    def pct(x: int) -> int:
        return int(round((x / total) * 100)) if total else 0

    first_pain = next((idx + 1 for idx, c in enumerate(calls) if c.pain_confirmed), None)
    first_follow = next((idx + 1 for idx, c in enumerate(calls) if c.follow_up), None)

    m = {
        "total_calls": total,
        "pain_confirmed": pain_n,
        "interest": interest_n,
        "follow_up": follow_n,
        "pain_rate": pct(pain_n),
        "interest_rate": pct(interest_n),
        "follow_rate": pct(follow_n),
        "first_pain_call": first_pain,
        "first_follow_call": first_follow,
    }

    decision_hint = (
        "Rule of thumb:\n"
        "- If pain confirmed < 30% → likely invalid (or TAL dirty).\n"
        "- If pain is high but interest low → value wording / owning role.\n"
        "- If pain+interest high but follow-up low → urgency/ownership/trust.\n"
        "- If signals appear only after 25–30 calls → likely rationalizing."
    )

    return templates.TemplateResponse("metrics.html", {"request": request, "user": user, "h": h, "m": m, "decision_hint": decision_hint})


@app.get("/companies", response_class=HTMLResponse)
async def companies_list(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    icp: str = "",
):
    # Companies are visible to any logged-in user; import is admin-only.
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    total = (await db.execute(select(func.count()).select_from(Company))).scalar_one()

    query = select(Company).order_by(Company.id.desc())
    qn = q.strip()
    if qn:
        like = f"%{qn.lower()}%"
        query = query.where(or_(func.lower(Company.name).like(like), func.lower(Company.website).like(like)))
    icpn = icp.strip()
    if icpn:
        query = query.where(Company.icp == icpn)

    rows = (await db.execute(query.limit(200))).scalars().all()

    last_import = request.session.get("last_company_import")
    can_import = user.role == "admin"
    return templates.TemplateResponse(
        "companies_list.html",
        {
            "request": request,
            "user": user,
            "companies": rows,
            "total": total,
            "q": qn,
            "icp": icpn,
            "can_import": can_import,
            "last_import": last_import,
        },
    )


@app.post("/companies/import")
async def companies_import(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    if user.role != "admin":
        return RedirectResponse(url="/companies", status_code=303)

    result = await import_companies_csv(db, REPO_ROOT / "Companies.csv")
    request.session["last_company_import"] = {"inserted": result["inserted"], "skipped": result["skipped"]}
    return RedirectResponse(url="/companies", status_code=303)


@app.get("/files", response_class=HTMLResponse)
async def files_list(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    kind: str = "",
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    total = (await db.execute(select(func.count()).select_from(Document))).scalar_one()
    kinds = (await db.execute(select(Document.kind).distinct().order_by(Document.kind))).scalars().all()

    query = select(Document).order_by(Document.id.desc())
    qn = q.strip()
    if qn:
        like = f"%{qn.lower()}%"
        query = query.where(func.lower(Document.rel_path).like(like))
    kn = kind.strip()
    if kn:
        query = query.where(Document.kind == kn)

    docs = (await db.execute(query.limit(200))).scalars().all()
    can_reindex = user.role == "admin"
    last_reindex = request.session.get("last_files_reindex")
    return templates.TemplateResponse(
        "files_list.html",
        {
            "request": request,
            "user": user,
            "docs": docs,
            "total": total,
            "q": qn,
            "kind": kn,
            "kinds": kinds,
            "can_reindex": can_reindex,
            "last_reindex": last_reindex,
        },
    )


@app.post("/files/reindex")
async def files_reindex(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)
    if user.role != "admin":
        return RedirectResponse(url="/files", status_code=303)

    result = await reindex_documents(db, REPO_ROOT)
    request.session["last_files_reindex"] = result
    return RedirectResponse(url="/files", status_code=303)


@app.get("/files/{doc_id}", response_class=HTMLResponse)
async def file_detail(
    request: Request,
    doc_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    doc = await db.get(Document, doc_id)
    if not doc:
        return RedirectResponse(url="/files", status_code=303)

    path = safe_resolve_under_repo(doc.rel_path)
    preview = None
    preview_bytes = 200_000
    if path and path.exists():
        preview = read_text_preview(path, max_bytes=preview_bytes)

    return templates.TemplateResponse(
        "file_detail.html",
        {"request": request, "doc": doc, "preview": preview, "preview_bytes": preview_bytes},
    )


@app.get("/files/{doc_id}/download")
async def file_download(
    request: Request,
    doc_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        await get_current_user(request, db)
    except Exception:
        return RedirectResponse(url="/login", status_code=303)

    doc = await db.get(Document, doc_id)
    if not doc:
        return RedirectResponse(url="/files", status_code=303)

    path = safe_resolve_under_repo(doc.rel_path)
    if not path or not path.exists() or not path.is_file():
        return RedirectResponse(url="/files", status_code=303)

    return FileResponse(path, filename=path.name)


@app.post("/debug/supabase-insert")
async def debug_supabase_insert(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        user = await get_current_user(request, db)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Unauthorized") from exc

    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    table = str(body.get("table", "smartlead_events_raw")).strip() or "smartlead_events_raw"
    record = body.get("record")
    if not isinstance(record, dict):
        record = {
            "source": "manual_debug",
            "payload": body if body else {"message": "supabase ping"},
            "created_at": dt.datetime.utcnow().isoformat(),
        }

    try:
        rows = insert_row(table, record)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Supabase insert failed: {exc}") from exc

    return {"ok": True, "table": table, "inserted_rows": len(rows), "data": rows}

