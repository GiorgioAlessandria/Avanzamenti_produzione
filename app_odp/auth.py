# auth.py
from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_user, logout_user, login_required
from app_odp.models import User

try:
    from icecream import ic
finally:
    pass
auth_bp = Blueprint("auth", __name__)


def load_user(user_id):
    return User.query.get(int(user_id))


def user_in() -> list[str]:
    users = User.query.filter_by(active=True).order_by(User.username).all()

    return [u for u in users if u.username.lower() != "admin"]


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    active_users = user_in()
    if request.method == "POST":
        username = request.form.get("username")
        if username is None or not username:
            return render_template(
                "login.j2", users=active_users, error="Seleziona un utente"
            )
        user = User.query.filter_by(id=username, active=True).first()
        if user:
            login_user(user)
            return redirect(url_for("main.home"))
        else:
            return render_template(
                "login.j2", users=active_users, error="Utente non valido"
            )
    return render_template("login.j2", users=active_users)


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
