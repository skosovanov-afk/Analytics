from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.github_sync import upsert_markdown_file
from app.models import Call, Hypothesis, ICP, SubVertical, TAL, TALAccount, VPPoint


def hypothesis_md_filename(h: Hypothesis) -> str:
    # Keep existing repo convention (they already have `1-LowFXfees.md`)
    safe_title = "".join(ch for ch in h.title if ch.isalnum() or ch in ("-", "_")).strip()
    safe_title = safe_title or "hypothesis"
    return f"{h.id}-{safe_title}.md"


def render_hypothesis_markdown(
    h: Hypothesis,
    *,
    vp_point_name: str | None = None,
    icp_name: str | None = None,
    sub_vertical_name: str | None = None,
) -> str:
    lines: list[str] = []
    lines.append(f"# Hypothesis #{h.id}: {h.title}")
    lines.append("")
    lines.append(f"- **Status**: {h.status}")
    lines.append(f"- **Channel**: {h.channel}")
    lines.append(f"- **Owner user id**: {h.owner_user_id}")
    lines.append(f"- **Decision**: {getattr(h, 'decision', 'open')}")
    lines.append("")
    lines.append("## Framework")
    lines.append(f"- **VP Point**: {vp_point_name or ''} (id={getattr(h, 'vp_point_id', '') or ''})")
    lines.append(f"- **ICP**: {icp_name or ''} (id={getattr(h, 'icp_id', '') or ''})")
    lines.append(f"- **Vertical/Sub**: {sub_vertical_name or ''} (id={getattr(h, 'sub_vertical_id', '') or ''})")
    lines.append("")
    lines.append("### Pain")
    lines.append(getattr(h, "pain", "") or "")
    lines.append("")
    lines.append("### Expected signal")
    lines.append(getattr(h, "expected_signal", "") or "")
    lines.append("")
    lines.append("### Disqualifiers")
    lines.append(getattr(h, "disqualifiers", "") or "")
    lines.append("")
    lines.append("## Segment / ICP")
    lines.append(h.segment or "")
    lines.append("")
    lines.append("## Problem")
    lines.append(h.problem or "")
    lines.append("")
    lines.append("## Assumption")
    lines.append(h.assumption or "")
    lines.append("")
    lines.append("## Success metric")
    lines.append(h.success_metric or "")
    lines.append("")
    lines.append("## Minimal signal")
    lines.append(h.minimal_signal or "")
    lines.append("")
    if h.start_date or h.end_date:
        lines.append("## Dates")
        lines.append(f"- Start: {h.start_date or ''}")
        lines.append(f"- End: {h.end_date or ''}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


async def enrich_and_write_hypothesis_card(repo_root: Path, db: AsyncSession, h: Hypothesis) -> Path:
    """
    Writes hypothesis card and appends lightweight facts (TAL size + call metrics).
    """
    # Load TAL size
    tal = (await db.execute(select(TAL).where(TAL.hypothesis_id == h.id))).scalar_one_or_none()
    tal_count = 0
    if tal:
        tal_count = (
            await db.execute(select(TALAccount.id).where(TALAccount.tal_id == tal.id))
        ).scalars().all().__len__()

    vp_name = None
    icp_name = None
    sub_name = None
    try:
        if getattr(h, "vp_point_id", None):
            vp = await db.get(VPPoint, int(h.vp_point_id))
            vp_name = vp.name if vp else None
        if getattr(h, "icp_id", None):
            icp = await db.get(ICP, int(h.icp_id))
            icp_name = icp.name if icp else None
        if getattr(h, "sub_vertical_id", None):
            sub = await db.get(SubVertical, int(h.sub_vertical_id))
            sub_name = sub.name if sub else None
    except Exception:
        # keep names empty if lookup fails
        pass

    calls = (await db.execute(select(Call).where(Call.hypothesis_id == h.id))).scalars().all()
    total = len(calls)
    pain_n = sum(1 for c in calls if c.pain_confirmed)
    interest_n = sum(1 for c in calls if c.interest)
    follow_n = sum(1 for c in calls if c.follow_up)

    base = (
        render_hypothesis_markdown(h, vp_point_name=vp_name, icp_name=icp_name, sub_vertical_name=sub_name).rstrip()
        + "\n\n"
    )
    base += "## Facts\n"
    base += f"- **TAL size**: {tal_count}\n"
    base += f"- **Calls**: {total}\n"
    if total:
        base += f"- **Pain confirmed rate**: {round((pain_n/total)*100)}%\n"
        base += f"- **Interest rate**: {round((interest_n/total)*100)}%\n"
        base += f"- **Follow-up rate**: {round((follow_n/total)*100)}%\n"

    out_dir = repo_root / "knowledge" / "hypotheses"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / hypothesis_md_filename(h)
    out_path.write_text(base, encoding="utf-8")

    # Опционально синхронизируем карточку в GitHub-репозиторий
    rel_path = f"knowledge/hypotheses/{out_path.name}"
    await upsert_markdown_file(
        path=rel_path,
        content=base,
        message=f"Update hypothesis card #{h.id}",
    )

    return out_path


def write_hypothesis_card(repo_root: Path, h: Hypothesis) -> Path:
    out_dir = repo_root / "knowledge" / "hypotheses"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / hypothesis_md_filename(h)
    # Base writer must never trigger DB IO (no relationship access).
    out_path.write_text(render_hypothesis_markdown(h), encoding="utf-8")
    return out_path

