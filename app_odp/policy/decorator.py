# app_odp/policy/decorator.py
from __future__ import annotations

from functools import wraps

from flask import abort, redirect, url_for
from flask_login import current_user

from app_odp.operator_session import resolve_operator_session, active_policy


def require_perm(code: str):
    """
    Vecchio decorator: usa solo Flask-Login.
    Da mantenere solo per route che devono essere accessibili
    esclusivamente da utenti loggati via Flask-Login.
    """

    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not getattr(current_user, "is_authenticated", False):
                return redirect(url_for("auth.login"))

            policy = active_policy()
            if not policy.can(code):
                abort(403)

            return fn(*args, **kwargs)

        return wrapper

    return deco


def require_active_perm(code: str):
    """
    Nuovo decorator:
    accetta sia sessione operatore tramite tab_session,
    sia sessione normale Flask-Login.
    """

    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            operator_session = resolve_operator_session()

            if operator_session is None and not getattr(
                current_user, "is_authenticated", False
            ):
                return redirect(url_for("auth.login"))

            policy = active_policy()
            if not policy.can(code):
                abort(403)

            return fn(*args, **kwargs)

        return wrapper

    return deco


def require_active_any_perm(*codes: str):
    """
    Consente l'accesso quando l'utente possiede almeno uno
    dei permessi indicati.
    """

    normalized_codes = tuple(
        str(code or "").strip() for code in codes if str(code or "").strip()
    )

    if not normalized_codes:
        raise ValueError("È necessario specificare almeno un permesso.")

    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            operator_session = resolve_operator_session()

            if operator_session is None and not getattr(
                current_user,
                "is_authenticated",
                False,
            ):
                return redirect(url_for("auth.login"))

            policy = active_policy()

            if not any(policy.can(code) for code in normalized_codes):
                abort(403)

            return fn(*args, **kwargs)

        return wrapper

    return deco
