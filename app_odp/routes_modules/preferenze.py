import secrets

from flask import abort, current_app, redirect, render_template, request, session, url_for
from sqlalchemy.exc import SQLAlchemyError

from app_odp.models import db
from app_odp.operator_session import (
    active_token, active_user, get_operator_token,
)
from app_odp.policy.decorator import require_active_perm
from app_odp.phase_labels import PREFERENCE_KEY, get_phase_labels, phase_label, validate_phase_labels
from app_odp.routes_blueprint import main_bp


@main_bp.context_processor
def inject_phase_labels():
    labels = get_phase_labels(active_user())
    return {
        "phase_labels": labels,
        "phase_label": lambda value: phase_label(value, labels),
    }


@main_bp.route("/preferenze/fasi", methods=["GET", "POST"])
@require_active_perm("nomi_fase")
def preferenze_fasi():
    # Un token operatore scaduto non deve modificare il profilo del login condiviso.
    if get_operator_token() and get_operator_token() != active_token():
        abort(403)
    user = active_user()
    if not user.is_active:
        abort(403)
    csrf = session.setdefault("phase_labels_csrf", secrets.token_urlsafe(32))
    labels = get_phase_labels(user)
    rows = sorted((labels or {"1": "", "2": ""}).items(), key=lambda item: int(item[0]))
    error = None
    status = 200
    if request.method == "POST":
        if (not secrets.compare_digest(csrf.encode(), request.form.get("csrf_token", "").encode())
                or request.form.get("preference_user_id") != str(user.id)):
            abort(400)
        codes = request.form.getlist("phase_code")
        names = request.form.getlist("phase_name")
        rows = list(zip(codes, names))
        try:
            labels = validate_phase_labels(codes, names)
            if not isinstance(user.preferences, dict):
                raise ValueError("Le preferenze esistenti non sono valide. Contatta un amministratore.")
            user.set_pref(PREFERENCE_KEY, labels)
            db.session.commit()
        except ValueError as exc:
            error, status = str(exc), 400
        except SQLAlchemyError:
            db.session.rollback()
            current_app.logger.exception("Salvataggio nomi personali delle fasi non riuscito")
            error, status = "Salvataggio non riuscito. Riprova: le modifiche non sono state salvate.", 500
        else:
            args = {"saved": "1"}
            if active_token():
                args["tab_session"] = active_token()
            return redirect(url_for("main.preferenze_fasi", **args), code=303)
    response = current_app.make_response((render_template(
        "preferenze_fasi.j2", phase_rows=rows, phase_csrf=csrf,
        preference_user=user, preference_error=error,
    ), status))
    response.headers["Cache-Control"] = "no-store"
    return response
