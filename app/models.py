from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    role: Mapped[str] = mapped_column(String(32), default="bizdev")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    hypotheses: Mapped[list["Hypothesis"]] = relationship(back_populates="owner")


class Hypothesis(Base):
    __tablename__ = "hypotheses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)

    title: Mapped[str] = mapped_column(String(200))
    # Legacy freeform fields (kept for backward compatibility)
    segment: Mapped[str] = mapped_column(String(200), default="")
    problem: Mapped[str] = mapped_column(Text, default="")
    assumption: Mapped[str] = mapped_column(Text, default="")
    channel: Mapped[str] = mapped_column(String(100), default="")
    success_metric: Mapped[str] = mapped_column(String(200), default="")
    minimal_signal: Mapped[str] = mapped_column(String(200), default="")

    # Framework fields (VP → ICP → Vertical/Sub → Hypothesis)
    vp_point_id: Mapped[Optional[int]] = mapped_column(ForeignKey("vp_points.id"), nullable=True, index=True)
    icp_id: Mapped[Optional[int]] = mapped_column(ForeignKey("icps.id"), nullable=True, index=True)
    sub_vertical_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sub_verticals.id"), nullable=True, index=True)

    pain: Mapped[str] = mapped_column(Text, default="")
    expected_signal: Mapped[str] = mapped_column(Text, default="")
    disqualifiers: Mapped[str] = mapped_column(Text, default="")

    decision: Mapped[str] = mapped_column(String(32), default="open")  # open/validated/invalidated/inconclusive
    decision_notes: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="draft")
    start_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    owner: Mapped["User"] = relationship(back_populates="hypotheses")
    calls: Mapped[list["CallLink"]] = relationship(back_populates="hypothesis", cascade="all, delete-orphan")
    metrics: Mapped[list["WeeklyMetric"]] = relationship(back_populates="hypothesis", cascade="all, delete-orphan")

    vp_point: Mapped[Optional["VPPoint"]] = relationship()
    icp: Mapped[Optional["ICP"]] = relationship()
    sub_vertical: Mapped[Optional["SubVertical"]] = relationship()
    tal: Mapped[Optional["TAL"]] = relationship(back_populates="hypothesis", uselist=False)
    script: Mapped[Optional["Script"]] = relationship(back_populates="hypothesis", uselist=False)


class Call(Base):
    __tablename__ = "calls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)

    # Framework linkage (preferred)
    hypothesis_id: Mapped[Optional[int]] = mapped_column(ForeignKey("hypotheses.id"), nullable=True, index=True)
    tal_account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("tal_accounts.id"), nullable=True, index=True)
    company_id: Mapped[Optional[int]] = mapped_column(ForeignKey("companies.id"), nullable=True, index=True)

    call_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)
    company: Mapped[str] = mapped_column(String(200), default="")
    contact: Mapped[str] = mapped_column(String(200), default="")
    source: Mapped[str] = mapped_column(String(100), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    transcript_url: Mapped[str] = mapped_column(String(500), default="")

    # Call observations used for hypothesis validation
    pain_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    severity: Mapped[int] = mapped_column(Integer, default=0)  # 1-5
    interest: Mapped[bool] = mapped_column(Boolean, default=False)
    follow_up: Mapped[bool] = mapped_column(Boolean, default=False)
    disqualifier: Mapped[str] = mapped_column(String(200), default="")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    links: Mapped[list["CallLink"]] = relationship(back_populates="call", cascade="all, delete-orphan")
    hypothesis: Mapped["Hypothesis | None"] = relationship()
    tal_account: Mapped["TALAccount | None"] = relationship(back_populates="calls")
    company_ref: Mapped["Company | None"] = relationship()


class CallLink(Base):
    __tablename__ = "call_links"
    __table_args__ = (UniqueConstraint("hypothesis_id", "call_id", name="uq_call_link"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hypothesis_id: Mapped[int] = mapped_column(ForeignKey("hypotheses.id"), index=True)
    call_id: Mapped[int] = mapped_column(ForeignKey("calls.id"), index=True)

    hypothesis: Mapped["Hypothesis"] = relationship(back_populates="calls")
    call: Mapped["Call"] = relationship(back_populates="links")


class WeeklyMetric(Base):
    __tablename__ = "weekly_metrics"
    __table_args__ = (
        UniqueConstraint("hypothesis_id", "owner_user_id", "week_start", name="uq_week_metric"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hypothesis_id: Mapped[int] = mapped_column(ForeignKey("hypotheses.id"), index=True)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)

    week_start: Mapped[dt.date] = mapped_column(Date)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    hypothesis: Mapped["Hypothesis"] = relationship(back_populates="metrics")


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    icp: Mapped[str] = mapped_column(String(100), default="", index=True)
    name: Mapped[str] = mapped_column(String(300), default="", index=True)
    website: Mapped[Optional[str]] = mapped_column(String(500), nullable=True, index=True)
    score: Mapped[str] = mapped_column(String(50), default="")
    reasoning: Mapped[str] = mapped_column(Text, default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Stored as repo-relative POSIX path (forward slashes)
    rel_path: Mapped[str] = mapped_column(String(800), unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(50), default="other", index=True)  # csv/html/md/image/py/db/other
    ext: Mapped[str] = mapped_column(String(20), default="", index=True)

    size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    mtime_unix: Mapped[int] = mapped_column(Integer, default=0, index=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )


class VPPoint(Base):
    __tablename__ = "vp_points"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    job_to_be_done: Mapped[str] = mapped_column(Text, default="")
    pain_friction: Mapped[str] = mapped_column(Text, default="")
    outcome_metric: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)


class ICP(Base):
    __tablename__ = "icps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    role: Mapped[str] = mapped_column(String(200), default="")
    scale: Mapped[str] = mapped_column(String(200), default="")
    decision_context: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)


class Vertical(Base):
    __tablename__ = "verticals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    subs: Mapped[list["SubVertical"]] = relationship(back_populates="vertical", cascade="all, delete-orphan")


class SubVertical(Base):
    __tablename__ = "sub_verticals"
    __table_args__ = (UniqueConstraint("vertical_id", "name", name="uq_subvertical"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vertical_id: Mapped[int] = mapped_column(ForeignKey("verticals.id"), index=True)
    name: Mapped[str] = mapped_column(String(200), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    vertical: Mapped["Vertical"] = relationship(back_populates="subs")


class TAL(Base):
    __tablename__ = "tals"
    __table_args__ = (UniqueConstraint("hypothesis_id", name="uq_tal_hypothesis"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hypothesis_id: Mapped[int] = mapped_column(ForeignKey("hypotheses.id"), index=True)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    hypothesis: Mapped["Hypothesis"] = relationship(back_populates="tal")
    accounts: Mapped[list["TALAccount"]] = relationship(back_populates="tal", cascade="all, delete-orphan")


class TALAccount(Base):
    __tablename__ = "tal_accounts"
    __table_args__ = (UniqueConstraint("tal_id", "company_id", name="uq_tal_company"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tal_id: Mapped[int] = mapped_column(ForeignKey("tals.id"), index=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)

    fit_reason: Mapped[str] = mapped_column(Text, default="")
    pain_hint: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(50), default="not_contacted", index=True)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    tal: Mapped["TAL"] = relationship(back_populates="accounts")
    company: Mapped["Company"] = relationship()
    calls: Mapped[list["Call"]] = relationship(back_populates="tal_account")


class Script(Base):
    __tablename__ = "scripts"
    __table_args__ = (UniqueConstraint("hypothesis_id", name="uq_script_hypothesis"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hypothesis_id: Mapped[int] = mapped_column(ForeignKey("hypotheses.id"), index=True)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    content: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow
    )

    hypothesis: Mapped["Hypothesis"] = relationship(back_populates="script")

