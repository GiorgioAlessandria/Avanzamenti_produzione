# app_odp/rbac/decorators.py
from __future__ import annotations

from functools import wraps

from flask import abort
from flask_login import current_user

from app_odp.policy.policy import RbacPolicy


def require_perm(code: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            policy = RbacPolicy(current_user)
            if not policy.can(code):
                abort(403)
            return fn(*args, **kwargs)

        return wrapper

    return deco
