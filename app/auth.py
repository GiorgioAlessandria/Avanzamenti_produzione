# auth.py
from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_user, logout_user, login_required
from models import User
try:
    from icecream import ic
except:
    pass

auth_bp = Blueprint("auth", __name__)


def load_user(user_id):
    return User.query.get(int(user_id))


def user_in() -> list[str]:
    list_user = list(str())
    users = list(User.query.order_by(
        User.username).filter_by(active=True).all())
    list_user = list(str())
    for user in users:
        if str(user).lower() == 'admin':
            pass
        else:
            list_user.append(user)
    return list_user


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    active_users = user_in()
    if request.method == "POST":
        username = request.form.get("username")
        if not username:
            return render_template("login.j2", users=active_users, error="Seleziona un utente")

        user = User.query.filter_by(id=username, active=True).first()
        if user:
            login_user(user)
            return redirect(url_for("main.home"))
        else:
            return render_template("login.j2", users=active_users, error="Utente non valido")

    return render_template("login.j2", users=active_users)


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
