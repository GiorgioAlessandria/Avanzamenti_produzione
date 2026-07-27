# auth.py
import hashlib

from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_user, logout_user, current_user
from app_odp.policy.policy import RbacPolicy
from app_odp.models import User
from app_odp.operator_session import (
    create_operator_session,
    resolve_operator_session,
    revoke_operator_sessions_for_user,
)


auth_bp = Blueprint("auth", __name__)


def _get_post_login_redirect(user):
    policy = RbacPolicy(user)

    has_acquisti = policy.can("home_acquisti")
    has_produzione = policy.can("home")
    has_rifiuti = (
        policy.can("rifiuti_carica")
        or policy.can("rifiuti_elimina")
    )
    has_manutenzioni = (
        policy.can("manutenzioni_visualizza")
        or policy.can("manutenzioni_amministrazione")
    )
    has_tarature = policy.can("tarature")
    has_carica = policy.can("carica")
    has_ricezione = policy.can("ricezione")

    if has_carica:
        return url_for("main.logistica_page")

    if has_acquisti and has_produzione:
        token = create_operator_session(user)
        return url_for("main.home_acquisti", tab_session=token)

    if has_acquisti:
        return url_for("main.home_acquisti")

    if has_produzione:
        token = create_operator_session(user)
        return url_for("main.home", tab_session=token)

    if has_ricezione:
        return url_for("main.logistica_page")

    if has_rifiuti:
        return url_for("main.rifiuti_page")

    if has_manutenzioni:
        token = create_operator_session(user)
        return url_for("main.manutenzioni_home", tab_session=token)

    if has_tarature:
        token = create_operator_session(user)
        return url_for("main.tarature_home", tab_session=token)

    return None


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        target = _get_post_login_redirect(current_user)
        return redirect(target or url_for("auth.login"))

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

        policy = RbacPolicy(user)

        # Login normale: acquisti / amministrazione
        has_acquisti = policy.can("home_acquisti")
        has_produzione = policy.can("home")
        has_rifiuti = (
            policy.can("rifiuti_carica")
            or policy.can("rifiuti_elimina")
        )
        has_manutenzioni = (
            policy.can("manutenzioni_visualizza")
            or policy.can("manutenzioni_amministrazione")
        )
        has_tarature = policy.can("tarature")
        has_carica = policy.can("carica")
        has_ricezione = policy.can("ricezione")

        if has_carica:
            token = create_operator_session(user)
            return redirect(url_for("main.logistica_page", tab_session=token))

        if has_acquisti and has_produzione:
            login_user(user)
            token = create_operator_session(user)
            return redirect(url_for("main.home_acquisti", tab_session=token))

        if has_acquisti:
            login_user(user)
            return redirect(url_for("main.home_acquisti"))

        if has_produzione:
            token = create_operator_session(user)
            return redirect(url_for("main.home", tab_session=token))

        if has_ricezione:
            token = create_operator_session(user)
            return redirect(url_for("main.logistica_page", tab_session=token))

        if has_rifiuti:
            login_user(user)
            return redirect(url_for("main.rifiuti_page"))

        if has_manutenzioni:
            login_user(user)
            token = create_operator_session(user)
            return redirect(url_for("main.manutenzioni_home", tab_session=token))

        if has_tarature:
            login_user(user)
            token = create_operator_session(user)
            return redirect(url_for("main.tarature_home", tab_session=token))

        return render_template(
            "login.j2",
            error="Utente senza permessi di accesso.",
        ), 403

    return render_template("login.j2")


@auth_bp.route("/logout")
def logout():
    row = resolve_operator_session()
    if row is not None:
        revoke_operator_sessions_for_user(row.user_id)
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.route("/operator-logout")
def operator_logout():
    return logout()
