from flask import current_app, jsonify, render_template, request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError

from app_odp.models import db
from app_odp.operator_session import active_user
from app_odp.policy.decorator import require_active_perm
from app_odp.routes_blueprint import main_bp
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniConflictError,
    VenditeAssegnazioniError,
    build_assignment_dashboard,
    create_customer_order,
    set_machine_assignment,
)
from app_odp.services.vendite_service import build_vendite_payload


@main_bp.get("/vendite")
@require_active_perm("vendite")
def vendite_page():
    return render_template("vendite.j2")


@main_bp.get("/api/vendite/ordini-macchina")
@require_active_perm("vendite")
def api_vendite_ordini_macchina():
    response = jsonify({"ok": True, "data": build_vendite_payload()})
    response.headers["Cache-Control"] = "no-store"
    return response, 200


@main_bp.get("/vendite/assegnazioni")
@require_active_perm("vendite")
def vendite_assegnazioni_page():
    return render_template("vendite_assegnazioni.j2")


@main_bp.get("/api/vendite/assegnazioni")
@require_active_perm("vendite")
def api_vendite_assegnazioni():
    response = jsonify({"ok": True, "data": build_assignment_dashboard()})
    response.headers["Cache-Control"] = "no-store"
    return response, 200


def _assignment_mutation(action, success_message: str, success_status: int = 200):
    try:
        action()
        data = build_assignment_dashboard()
        db.session.commit()
    except VenditeAssegnazioniConflictError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 409
    except VenditeAssegnazioniError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400
    except IntegrityError:
        db.session.rollback()
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "I dati sono stati modificati da un altro operatore. "
                        "Aggiornare la pagina e riprovare."
                    ),
                }
            ),
            409,
        )
    except StaleDataError:
        db.session.rollback()
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "La riga è stata modificata da un altro operatore. "
                        "Aggiornare la pagina e riprovare."
                    ),
                }
            ),
            409,
        )
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "Errore durante la gestione degli ordini cliente Vendite."
        )
        return jsonify({"ok": False, "error": "Errore durante il salvataggio."}), 500

    response = jsonify(
        {
            "ok": True,
            "message": success_message,
            "data": data,
        }
    )
    response.headers["Cache-Control"] = "no-store"
    return response, success_status


@main_bp.post("/api/vendite/ordini-cliente")
@require_active_perm("vendite")
def api_vendite_ordini_cliente_create():
    payload = request.get_json(silent=True)
    return _assignment_mutation(
        lambda: create_customer_order(payload, active_user()),
        "Ordine cliente inserito.",
        201,
    )


@main_bp.post(
    "/api/vendite/ordini-cliente/righe/<int:row_id>/assegnazione"
)
@require_active_perm("vendite")
def api_vendite_riga_assegnazione(row_id: int):
    payload = request.get_json(silent=True)
    return _assignment_mutation(
        lambda: set_machine_assignment(row_id, payload, active_user()),
        "Assegnazione aggiornata.",
    )
