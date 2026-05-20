# auth.py
import hashlib

from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_user, logout_user, login_required, current_user
from app_odp.policy.policy import RbacPolicy
from app_odp.models import User
from app_odp.operator_session import (
    create_operator_session,
    resolve_operator_session,
    revoke_operator_sessions_for_user,
)

try:
    from icecream import ic
finally:
    pass

auth_bp = Blueprint("auth", __name__)


def _get_post_login_redirect(user):
    policy = RbacPolicy(user)

    if policy.can("home"):
        return url_for("main.home")

    if policy.can("home_acquisti"):
        return url_for("main.home_acquisti")

    return None


@auth_bp.route("/operator-login", methods=["GET", "POST"])
def operator_login():
    if request.method == "POST":
        login_code = (request.form.get("login_code") or "").strip().upper()

        if not login_code:
            return render_template(
                "login.j2",
                error="Inserisci il codice di accesso.",
                operator_login=True,
            ), 400

        lookup = hashlib.sha256(login_code.encode("utf-8")).hexdigest()

        user = User.query.filter_by(
            login_code_lookup=lookup,
            active=True,
        ).first()

        if user is None or not user.check_login_code(login_code):
            return render_template(
                "login.j2",
                error="Codice di accesso non valido.",
                operator_login=True,
            ), 401

        token = create_operator_session(user)

        return redirect(url_for("main.home", tab_session=token))

    return render_template("login.j2", operator_login=True)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(_get_post_login_redirect(current_user))

    if request.method == "POST":
        login_code = (request.form.get("login_code") or "").strip().upper()

        if not login_code:
            return render_template(
                "login.j2",
                error="Inserisci il codice di accesso.",
            ), 400

        lookup = hashlib.sha256(login_code.encode("utf-8")).hexdigest()

        user = User.query.filter_by(
            login_code_lookup=lookup,
            active=True,
        ).first()

        if user is None or not user.check_login_code(login_code):
            return render_template(
                "login.j2",
                error="Codice di accesso non valido.",
            ), 401

        login_user(user)
        return redirect(_get_post_login_redirect(user))

    return render_template("login.j2")


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.route("/operator-logout")
def operator_logout():
    row = resolve_operator_session()

    if row is not None:
        revoke_operator_sessions_for_user(row.user_id)

    return redirect(url_for("auth.operator_login"))
