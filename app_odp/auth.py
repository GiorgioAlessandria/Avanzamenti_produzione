# auth.py
import hashlib

from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_user, logout_user, login_required, current_user

from app_odp.models import User

try:
    from icecream import ic
finally:
    pass

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("main.home"))

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
        return redirect(url_for("main.home"))

    return render_template("login.j2")


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
