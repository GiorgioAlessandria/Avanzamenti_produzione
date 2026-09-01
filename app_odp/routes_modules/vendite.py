from flask import current_app, jsonify, render_template, request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError

from app_odp.models import db
from app_odp.operator_session import active_policy, active_user
from app_odp.policy.decorator import (
    require_active_any_perm,
    require_active_perm,
)
from app_odp.routes_blueprint import main_bp
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniConflictError,
    VenditeAssegnazioniError,
    build_assignment_dashboard,
    confirm_customer_order_shipment,
    confirm_customer_order_read,
    confirm_customer_row_shipment,
    create_customer_order,
    delete_customer_order,
    set_machine_assignment,
    ship_stock_machine,
    update_customer_order_details,
    update_customer_row,
    update_customer_row_dates,
    update_customer_row_notes,
    update_packaging_notes,
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
    policy = active_policy()
    can_create_customer_orders = policy.can("carica_ordini_cliente")
    can_edit_production_notes = policy.can("assegna_matricole")
    return render_template(
        "vendite_assegnazioni.j2",
        can_create_customer_orders=can_create_customer_orders,
        can_assign_machines=(
            can_create_customer_orders or can_edit_production_notes
        ),
        can_edit_sales_notes=can_create_customer_orders,
        can_edit_production_notes=can_edit_production_notes,
        can_confirm_shipment=can_create_customer_orders,
        can_confirm_order_read=policy.can("conferma_lettura_ordine"),
    )


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
@require_active_perm("carica_ordini_cliente")
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
@require_active_any_perm("carica_ordini_cliente", "assegna_matricole")
def api_vendite_riga_assegnazione(row_id: int):
    payload = request.get_json(silent=True)
    return _assignment_mutation(
        lambda: set_machine_assignment(row_id, payload, active_user()),
        "Assegnazione aggiornata.",
    )


@main_bp.post("/api/vendite/ordini-cliente/<int:order_id>/dati")
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_ordine_cliente_dati(order_id: int):
    payload = request.get_json(silent=True)
    return _assignment_mutation(
        lambda: update_customer_order_details(order_id, payload),
        "Dati dell'ordine cliente aggiornati.",
    )


@main_bp.post("/api/vendite/ordini-cliente/righe/<int:row_id>/date")
@require_active_perm("vendite")
@require_active_any_perm("carica_ordini_cliente", "assegna_matricole")
def api_vendite_riga_date(row_id: int):
    payload = request.get_json(silent=True)
    policy = active_policy()
    return _assignment_mutation(
        lambda: update_customer_row_dates(
            row_id,
            payload,
            can_edit_delivery=policy.can("carica_ordini_cliente"),
            can_edit_available=policy.can("assegna_matricole"),
        ),
        "Date aggiornate.",
    )


@main_bp.post("/api/vendite/ordini-cliente/righe/<int:row_id>/salva")
@require_active_perm("vendite")
@require_active_any_perm("carica_ordini_cliente", "assegna_matricole")
def api_vendite_riga_salva(row_id: int):
    payload = request.get_json(silent=True)
    policy = active_policy()
    can_edit_sales = policy.can("carica_ordini_cliente")
    can_edit_production = policy.can("assegna_matricole")
    return _assignment_mutation(
        lambda: update_customer_row(
            row_id,
            payload,
            active_user(),
            can_edit_sales=can_edit_sales,
            can_edit_production=can_edit_production,
            can_assign=can_edit_sales or can_edit_production,
        ),
        "Riga aggiornata.",
    )


@main_bp.post("/api/vendite/note-imballaggio")
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_note_imballaggio():
    payload = request.get_json(silent=True)
    return _assignment_mutation(
        lambda: update_packaging_notes(payload, active_user()),
        "Note di imballaggio aggiornate.",
    )


@main_bp.post(
    "/api/vendite/ordini-cliente/righe/<int:row_id>/note"
)
@require_active_perm("vendite")
@require_active_any_perm("carica_ordini_cliente", "assegna_matricole")
def api_vendite_riga_note(row_id: int):
    payload = request.get_json(silent=True)
    policy = active_policy()
    return _assignment_mutation(
        lambda: update_customer_row_notes(
            row_id,
            payload,
            can_edit_sales=policy.can("carica_ordini_cliente"),
            can_edit_production=policy.can("assegna_matricole"),
        ),
        "Note aggiornate.",
    )


@main_bp.post(
    "/api/vendite/ordini-cliente/righe/<int:row_id>/conferma-spedizione"
)
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_riga_conferma_spedizione(row_id: int):
    payload = request.get_json(silent=True)
    can_edit_production = active_policy().can("assegna_matricole")
    return _assignment_mutation(
        lambda: confirm_customer_row_shipment(
            row_id,
            payload,
            active_user(),
            can_edit_production=can_edit_production,
        ),
        "Riga evasa.",
    )


@main_bp.post(
    "/api/vendite/ordini-cliente/<int:order_id>/conferma-spedizione"
)
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_ordine_cliente_conferma_spedizione(order_id: int):
    payload = request.get_json(silent=True)
    can_edit_production = active_policy().can("assegna_matricole")
    return _assignment_mutation(
        lambda: confirm_customer_order_shipment(
            order_id,
            payload,
            active_user(),
            can_edit_production=can_edit_production,
        ),
        "Ordine evaso.",
    )


@main_bp.post("/api/vendite/stock/spedisci")
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_stock_spedisci():
    payload = request.get_json(silent=True) or {}
    return _assignment_mutation(
        lambda: ship_stock_machine(
            payload.get("id_documento"),
            payload.get("id_riga"),
        ),
        "Spedizione della matricola STOCK confermata.",
    )


@main_bp.post(
    "/api/vendite/ordini-cliente/<int:order_id>/conferma-lettura"
)
@require_active_perm("vendite")
@require_active_perm("conferma_lettura_ordine")
def api_vendite_ordine_cliente_conferma_lettura(order_id: int):
    return _assignment_mutation(
        lambda: confirm_customer_order_read(order_id, active_user()),
        "Lettura dell'ordine cliente confermata.",
    )


@main_bp.delete(
    "/api/vendite/ordini-cliente/<int:order_id>/elimina"
)
@require_active_perm("vendite")
@require_active_perm("carica_ordini_cliente")
def api_vendite_ordine_cliente_delete(order_id: int):
    return _assignment_mutation(
        lambda: delete_customer_order(order_id),
        "Ordine cliente eliminato e matricole rese disponibili.",
    )
