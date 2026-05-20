import hashlib
import secrets
from datetime import datetime, timedelta
from functools import wraps
from zoneinfo import ZoneInfo

from flask import g, request, redirect, url_for
from flask_login import current_user

from app_odp.models import db, User, BrowserTabSession
from app_odp.policy.policy import RbacPolicy

ROME_TZ = ZoneInfo("Europe/Rome")
OPERATOR_SESSION_TIMEOUT = timedelta(hours=12)


def _now():
    return datetime.now(ROME_TZ)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token() -> str:
    return secrets.token_urlsafe(48)


def get_operator_token() -> str:
    return (
        request.args.get("tab_session")
        or request.form.get("tab_session")
        or request.headers.get("X-Tab-Session")
        or ""
    ).strip()


def operator_or_login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if resolve_operator_session() is not None:
            return fn(*args, **kwargs)

        if getattr(current_user, "is_authenticated", False):
            return fn(*args, **kwargs)

        return redirect(url_for("auth.operator_login"))

    return wrapper


def create_operator_session(user: User) -> str:
    token = new_token()
    now = _now()

    row = BrowserTabSession(
        token_hash=hash_token(token),
        user_id=user.id,
        created_at=now,
        last_seen_at=now,
        ip_address=request.headers.get("X-Forwarded-For", request.remote_addr),
        user_agent=(request.headers.get("User-Agent") or "")[:500],
    )

    db.session.add(row)
    db.session.commit()

    return token


def resolve_operator_session():
    token = get_operator_token()

    if not token:
        return None

    row = BrowserTabSession.query.filter_by(token_hash=hash_token(token)).first()

    if row is None:
        return None

    if row.revoked_at is not None:
        return None

    now = _now()

    if row.last_seen_at < now - OPERATOR_SESSION_TIMEOUT:
        row.revoked_at = now
        db.session.commit()
        return None

    user = row.user

    if user is None or not user.active:
        row.revoked_at = now
        db.session.commit()
        return None

    if row.last_seen_at < now - timedelta(minutes=60):
        row.last_seen_at = now
        db.session.commit()

    g.operator_session = row
    g.operator_token = token
    g.operator_user = user
    g.operator_policy = RbacPolicy(user)

    return row


def operator_tab_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if resolve_operator_session() is None:
            return redirect(url_for("auth.operator_login"))
        return fn(*args, **kwargs)

    return wrapper


def operator_perm_required(code: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if resolve_operator_session() is None:
                return redirect(url_for("auth.operator_login"))

            if not g.operator_policy.can(code):
                return redirect(url_for("auth.operator_login"))

            return fn(*args, **kwargs)

        return wrapper

    return deco


def revoke_operator_sessions_for_user(user_id: int) -> None:
    now = _now()

    BrowserTabSession.query.filter(
        BrowserTabSession.user_id == int(user_id),
        BrowserTabSession.revoked_at.is_(None),
    ).update(
        {BrowserTabSession.revoked_at: now},
        synchronize_session=False,
    )

    db.session.commit()


def active_user():
    return getattr(g, "operator_user", None) or current_user


def active_policy():
    if getattr(g, "operator_policy", None) is not None:
        return g.operator_policy
    return RbacPolicy(current_user)


def active_token() -> str:
    return getattr(g, "operator_token", "") or ""
