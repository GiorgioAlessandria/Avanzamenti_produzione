# app_odp/routes_modules/manutenzioni.py

from __future__ import annotations

from typing import Any

from flask import (
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from app_odp.models import db
from app_odp.operator_session import active_policy, active_token, active_user
from app_odp.policy.decorator import (
    require_active_any_perm,
)
from app_odp.routes_blueprint import main_bp
from app_odp.services.manutenzioni_service import (
    CodiceMacchinarioDuplicatoError,
    MacchinarioNonTrovatoError,
    ManutenzioniServiceError,
    PermessoManutenzioniError,
    RepartoNonAutorizzatoError,
    RepartoNonValidoError,
    create_macchinario,
    get_macchinario,
    list_macchinari,
    list_operatori_reparto,
    list_reparti_manutenzioni,
    serialize_macchinari,
    serialize_macchinario,
    set_macchinario_attivo,
    set_operatori_macchinario,
    update_macchinario,
)
from app_odp.services.manutenzioni_piani_service import (
    CodicePianoDuplicatoError,
    GIORNI_SETTIMANA_LABELS,
    PianoManutenzioneNonTrovatoError,
    create_piano_manutenzione,
    delete_piano_manutenzione,
    get_piano_manutenzione,
    list_piani_macchinario,
    serialize_piani_manutenzione,
    serialize_piano_manutenzione,
    set_piano_attivo,
    update_piano_manutenzione,
)
from app_odp.services.manutenzioni_eventi_service import (
    EventoManutenzioneChiusoError,
    EventoManutenzioneNonTrovatoError,
    build_scadenziario_manutenzioni,
    delete_evento_manutenzione,
    gestisci_evento_manutenzione,
    get_evento_manutenzione,
    list_eventi_macchinario,
    serialize_eventi_manutenzione,
    serialize_evento_manutenzione,
    sync_all_active_plans,
    sync_eventi_macchinario,
    sync_eventi_piano,
    today_rome,
)
from app_odp.services.giorni_lavorativi_service import (
    giorni_non_lavorativi_nel_periodo,
)
from app_odp.services.manutenzioni_straordinarie_service import (
    ManutenzioneStraordinariaNonTrovataError,
    build_registro_straordinarie,
    create_manutenzione_straordinaria,
    get_manutenzione_straordinaria,
    list_straordinarie_macchinario,
    serialize_manutenzione_straordinaria,
    serialize_manutenzioni_straordinarie,
    update_manutenzione_straordinaria,
)
from app_odp.services.manutenzioni_giorni_non_lavorativi_service import (
    GiornoNonLavorativoDuplicatoError,
    GiornoNonLavorativoNonTrovatoError,
    create_giorni_non_lavorativi,
    delete_giorno_non_lavorativo,
    get_giorno_non_lavorativo,
    list_giorni_non_lavorativi,
    serialize_giorni_non_lavorativi,
    serialize_giorno_non_lavorativo,
    set_giorno_non_lavorativo_attivo,
    update_giorno_non_lavorativo,
)


def _parse_query_bool(
    value: Any,
    *,
    default: bool = False,
) -> bool:
    if value is None or value == "":
        return default

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "vero", "si", "sì", "yes", "on"}:
        return True
    if normalized in {"0", "false", "falso", "no", "off"}:
        return False
    return default


def _parse_filter_date(
    value: Any,
    *,
    default: date,
) -> date:
    normalized = str(value or "").strip()

    if not normalized:
        return default

    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return default


def _macchinario_payload() -> dict[str, Any]:
    """
    Accetta sia payload JSON sia dati provenienti da un form HTML.
    """
    if request.is_json:
        payload = request.get_json(silent=True)
        return payload if isinstance(payload, dict) else {}

    return request.form.to_dict()


def _serialize_single_macchinario(macchinario) -> dict[str, Any]:
    """
    Usa la serializzazione multipla per includere anche la descrizione
    del reparto proveniente da RBAC.db.
    """
    rows = serialize_macchinari([macchinario])

    if rows:
        return rows[0]

    return serialize_macchinario(macchinario)


def _service_error_response(exc: Exception):
    if isinstance(
        exc,
        (
            MacchinarioNonTrovatoError,
            PianoManutenzioneNonTrovatoError,
            EventoManutenzioneNonTrovatoError,
            GiornoNonLavorativoNonTrovatoError,
            ManutenzioneStraordinariaNonTrovataError,
        ),
    ):
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 404

    if isinstance(
        exc,
        (
            EventoManutenzioneChiusoError,
            GiornoNonLavorativoDuplicatoError,
        ),
    ):
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 409

    if isinstance(
        exc,
        (
            PermessoManutenzioniError,
            RepartoNonAutorizzatoError,
        ),
    ):
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 403

    if isinstance(
        exc,
        (
            CodiceMacchinarioDuplicatoError,
            CodicePianoDuplicatoError,
            RepartoNonValidoError,
            ManutenzioniServiceError,
            ValueError,
        ),
    ):
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 400

    current_app.logger.exception(
        "Errore interno del modulo manutenzioni: %s",
        exc,
    )

    return jsonify(
        {
            "ok": False,
            "error": "Errore interno del modulo manutenzioni.",
        }
    ), 500


# =========================================================
# PAGINA PRINCIPALE
# =========================================================


@main_bp.get("/manutenzioni")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def manutenzioni_home():
    policy = active_policy()

    sync_all_active_plans(data_dal=today_rome())

    reparto_codice = (request.args.get("reparto_codice") or "").strip()

    search = (request.args.get("search") or "").strip()

    include_inactive = _parse_query_bool(
        request.args.get("include_inactive"),
        default=True,
    )

    macchinari = list_macchinari(
        policy,
        reparto_codice=reparto_codice or None,
        search=search or None,
        include_inactive=include_inactive,
    )

    macchinari_rows = serialize_macchinari(macchinari)

    reparti = list_reparti_manutenzioni(policy)

    can_manage = policy.can("manutenzioni_gestisci_macchinari") or policy.can(
        "manutenzioni_amministrazione"
    )
    can_view_register = policy.can(
        "manutenzioni_visualizza_registro"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )

    can_execute_events = policy.can(
        "manutenzioni_esegui"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )
    can_manage_calendar = policy.can(
        "manutenzioni_gestisci_piani"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )

    selected_view = str(
        request.args.get("view") or "calendario"
    ).strip().lower()

    allowed_views = {
        "calendario",
        "macchinari",
    }

    if can_manage_calendar:
        allowed_views.add("chiusure")

    if selected_view not in allowed_views:
        selected_view = "calendario"

    selected_stato = str(
        request.args.get("stato") or ""
    ).strip().upper()

    return render_template(
        "manutenzioni/dashboard.j2",
        macchinari_rows=macchinari_rows,
        reparti=reparti,
        selected_reparto=reparto_codice,
        selected_stato=selected_stato,
        selected_view=selected_view,
        search=search,
        include_inactive=include_inactive,
        can_manage=can_manage,
        can_view_register=can_view_register,
        can_execute_events=can_execute_events,
        can_manage_calendar=can_manage_calendar,
        oggi=today_rome().isoformat(),
    )


# =========================================================
# API ELENCO MACCHINARI
# =========================================================


@main_bp.get("/api/manutenzioni/macchinari")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_macchinari_list():
    policy = active_policy()

    reparto_codice = (request.args.get("reparto_codice") or "").strip()

    search = (request.args.get("search") or "").strip()

    include_inactive = _parse_query_bool(
        request.args.get("include_inactive"),
        default=False,
    )

    try:
        macchinari = list_macchinari(
            policy,
            reparto_codice=reparto_codice or None,
            search=search or None,
            include_inactive=include_inactive,
        )

        return jsonify(
            {
                "ok": True,
                "items": serialize_macchinari(macchinari),
                "count": len(macchinari),
            }
        )

    except Exception as exc:
        current_app.logger.exception("Errore durante il caricamento dei macchinari.")
        return _service_error_response(exc)


# =========================================================
# API DETTAGLIO MACCHINARIO
# =========================================================


@main_bp.get("/api/manutenzioni/macchinari/<int:macchinario_id>")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_macchinario_get(
    macchinario_id: int,
):
    policy = active_policy()

    try:
        macchinario = get_macchinario(
            macchinario_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "item": _serialize_single_macchinario(macchinario),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


# =========================================================
# API CREAZIONE MACCHINARIO
# =========================================================


@main_bp.post("/api/manutenzioni/macchinari")
@require_active_any_perm(
    "manutenzioni_gestisci_macchinari",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_macchinario_create():
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        macchinario = create_macchinario(
            payload,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": ("Macchinario creato correttamente."),
                "item": _serialize_single_macchinario(macchinario),
            }
        ), 201

    except Exception as exc:
        db.session.rollback()

        if not isinstance(
            exc,
            (
                ManutenzioniServiceError,
                PermessoManutenzioniError,
                RepartoNonAutorizzatoError,
                ValueError,
            ),
        ):
            current_app.logger.exception("Errore durante la creazione del macchinario.")

        return _service_error_response(exc)


# =========================================================
# API MODIFICA MACCHINARIO
# =========================================================


@main_bp.patch("/api/manutenzioni/macchinari/<int:macchinario_id>")
@require_active_any_perm(
    "manutenzioni_gestisci_macchinari",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_macchinario_update(
    macchinario_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        macchinario = update_macchinario(
            macchinario_id,
            payload,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": ("Macchinario aggiornato correttamente."),
                "item": _serialize_single_macchinario(macchinario),
            }
        )

    except Exception as exc:
        db.session.rollback()

        if not isinstance(
            exc,
            (
                ManutenzioniServiceError,
                PermessoManutenzioniError,
                RepartoNonAutorizzatoError,
                ValueError,
            ),
        ):
            current_app.logger.exception(
                "Errore durante la modifica del macchinario %s.",
                macchinario_id,
            )

        return _service_error_response(exc)


# =========================================================
# API ATTIVAZIONE / DISATTIVAZIONE
# =========================================================


@main_bp.patch("/api/manutenzioni/macchinari/<int:macchinario_id>/stato")
@require_active_any_perm(
    "manutenzioni_gestisci_macchinari",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_macchinario_stato(
    macchinario_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    if "attivo" not in payload:
        return jsonify(
            {
                "ok": False,
                "error": "Il campo 'attivo' è obbligatorio.",
            }
        ), 400

    attivo = _parse_query_bool(
        payload.get("attivo"),
        default=False,
    )

    try:
        macchinario = set_macchinario_attivo(
            macchinario_id,
            attivo,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Macchinario attivato correttamente."
                    if macchinario.attivo
                    else "Macchinario disattivato correttamente."
                ),
                "item": _serialize_single_macchinario(macchinario),
            }
        )

    except Exception as exc:
        db.session.rollback()

        if not isinstance(
            exc,
            (
                ManutenzioniServiceError,
                PermessoManutenzioniError,
                RepartoNonAutorizzatoError,
                ValueError,
            ),
        ):
            current_app.logger.exception(
                "Errore durante la modifica dello stato del macchinario %s.",
                macchinario_id,
            )

        return _service_error_response(exc)


# =========================================================
# API REPARTI VISIBILI
# =========================================================


@main_bp.get("/api/manutenzioni/reparti")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_reparti():
    policy = active_policy()

    try:
        reparti = list_reparti_manutenzioni(policy)

        return jsonify(
            {
                "ok": True,
                "items": reparti,
                "count": len(reparti),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


# =========================================================
# DETTAGLIO MACCHINARIO E PIANI RICORRENTI
# =========================================================


@main_bp.get("/manutenzioni/macchinari/<int:macchinario_id>")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def manutenzioni_macchinario_detail(
    macchinario_id: int,
):
    policy = active_policy()

    macchinario = get_macchinario(
        macchinario_id,
        policy,
    )

    sync_eventi_macchinario(
        macchinario.id,
        policy,
        data_dal=today_rome(),
    )

    piani = list_piani_macchinario(
        macchinario_id,
        policy,
        include_inactive=True,
    )

    macchinario_rows = serialize_macchinari([macchinario])

    macchinario_row = (
        macchinario_rows[0] if macchinario_rows else serialize_macchinario(macchinario)
    )

    can_manage_plans = policy.can("manutenzioni_gestisci_piani") or policy.can(
        "manutenzioni_amministrazione"
    )
    eventi = list_eventi_macchinario(
        macchinario_id,
        policy,
    )

    straordinarie = list_straordinarie_macchinario(
        macchinario_id,
        policy,
    )

    can_execute_events = policy.can(
        "manutenzioni_esegui"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )
    can_manage_machine = policy.can(
        "manutenzioni_gestisci_macchinari"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )
    operatori_disponibili = (
        list_operatori_reparto(macchinario.reparto_codice)
        if can_manage_machine
        else []
    )

    selected_view = str(
        request.args.get("view") or "programmate"
    ).strip().lower()

    if selected_view not in {
        "programmate",
        "straordinarie",
    }:
        selected_view = "programmate"

    adesso_rome = datetime.now(
        ZoneInfo("Europe/Rome")
    ).strftime("%Y-%m-%dT%H:%M")

    return render_template(
        "manutenzioni/macchinario_dettaglio.j2",
        macchinario=macchinario_row,
        piani=serialize_piani_manutenzione(piani),
        giorni_settimana_labels=(GIORNI_SETTIMANA_LABELS),
        can_manage_plans=can_manage_plans,
        can_admin=(
            policy.can("admin")
            or active_user().has_role("admin")
        ),
        eventi=serialize_eventi_manutenzione(eventi),
        can_execute_events=can_execute_events,
        can_manage_machine=can_manage_machine,
        operatori_disponibili=operatori_disponibili,
        can_manage_straordinarie=can_execute_events,
        straordinarie=(
            serialize_manutenzioni_straordinarie(
                straordinarie
            )
        ),
        selected_view=selected_view,
        oggi=today_rome().isoformat(),
        adesso=adesso_rome,
    )


@main_bp.post(
    "/manutenzioni/macchinari/<int:macchinario_id>/operatori"
)
@require_active_any_perm(
    "manutenzioni_gestisci_macchinari",
    "manutenzioni_amministrazione",
)
def manutenzioni_macchinario_operatori_update(
    macchinario_id: int,
):
    selected_view = str(
        request.form.get("view") or "programmate"
    ).strip().lower()
    redirect_values = {
        "macchinario_id": macchinario_id,
        "view": selected_view,
        "tab_session": active_token(),
    }

    try:
        set_operatori_macchinario(
            macchinario_id,
            request.form.getlist("operator_public_ids"),
            active_policy(),
        )
        redirect_values["operatori_salvati"] = "1"
    except (ManutenzioniServiceError, PermissionError) as exc:
        db.session.rollback()
        redirect_values["operatori_errore"] = str(exc)

    return redirect(
        url_for(
            "main.manutenzioni_macchinario_detail",
            **redirect_values,
        )
    )


@main_bp.get("/api/manutenzioni/macchinari/<int:macchinario_id>/piani")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piani_list(
    macchinario_id: int,
):
    policy = active_policy()

    try:
        piani = list_piani_macchinario(
            macchinario_id,
            policy,
            include_inactive=True,
        )

        return jsonify(
            {
                "ok": True,
                "items": (serialize_piani_manutenzione(piani)),
                "count": len(piani),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.post("/api/manutenzioni/macchinari/<int:macchinario_id>/piani")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piano_create(
    macchinario_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        piano = create_piano_manutenzione(
            macchinario_id,
            payload,
            policy,
            created_by=active_user(),
        )
        sync_result = sync_eventi_piano(piano)

        return jsonify(
            {
                "ok": True,
                "message": ("Piano di manutenzione creato correttamente."),
                "item": serialize_piano_manutenzione(piano),
                "eventi": sync_result,
            }
        ), 201

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.get("/api/manutenzioni/piani/<int:piano_id>")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piano_get(
    piano_id: int,
):
    policy = active_policy()

    try:
        piano = get_piano_manutenzione(
            piano_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "item": (serialize_piano_manutenzione(piano)),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.patch("/api/manutenzioni/piani/<int:piano_id>")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piano_update(
    piano_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        piano = update_piano_manutenzione(
            piano_id,
            payload,
            policy,
        )
        sync_result = sync_eventi_piano(
            piano,
            data_dal=today_rome(),
        )

        return jsonify(
            {
                "ok": True,
                "message": ("Piano di manutenzione aggiornato correttamente."),
                "item": serialize_piano_manutenzione(piano),
                "eventi": sync_result,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.delete("/api/manutenzioni/piani/<int:piano_id>")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piano_delete(
    piano_id: int,
):
    policy = active_policy()

    try:
        result = delete_piano_manutenzione(
            piano_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Serie eliminata. "
                    f"{result['eventi_eliminati']} eventi programmati eliminati."
                ),
                "result": result,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.patch("/api/manutenzioni/piani/<int:piano_id>/stato")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_piano_stato(
    piano_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    if "attiva" not in payload:
        return jsonify(
            {
                "ok": False,
                "error": ("Il campo 'attiva' è obbligatorio."),
            }
        ), 400

    try:
        attiva = _parse_query_bool(
            payload.get("attiva"),
            default=False,
        )

        piano = set_piano_attivo(
            piano_id,
            attiva,
            policy,
        )
        sync_result = sync_eventi_piano(
            piano,
            data_dal=today_rome(),
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Piano attivato correttamente."
                    if piano.attiva
                    else "Piano disattivato correttamente."
                ),
                "item": (serialize_piano_manutenzione(piano)),
                "eventi": sync_result,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.post("/api/manutenzioni/macchinari/<int:macchinario_id>/eventi/genera")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_eventi_generate(
    macchinario_id: int,
):
    policy = active_policy()

    try:
        result = sync_eventi_macchinario(
            macchinario_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Calendario aggiornato: "
                    f"{result['created']} nuovi eventi, "
                    f"{result['rescheduled']} scadenze riallineate."
                ),
                "result": result,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.get("/api/manutenzioni/macchinari/<int:macchinario_id>/eventi")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_eventi_list(
    macchinario_id: int,
):
    policy = active_policy()

    try:
        eventi = list_eventi_macchinario(
            macchinario_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "items": (serialize_eventi_manutenzione(eventi)),
                "count": len(eventi),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.get("/api/manutenzioni/eventi/<int:evento_id>")
@require_active_any_perm(
    "manutenzioni_esegui",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_evento_get(
    evento_id: int,
):
    policy = active_policy()

    try:
        evento = get_evento_manutenzione(
            evento_id,
            policy,
            require_execution=True,
        )

        return jsonify(
            {
                "ok": True,
                "item": serialize_evento_manutenzione(evento),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.delete("/api/manutenzioni/eventi/<int:evento_id>")
@require_active_any_perm(
    "admin",
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_evento_delete(
    evento_id: int,
):
    policy = active_policy()

    try:
        deleted_id = delete_evento_manutenzione(
            evento_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "message": "Evento programmato eliminato.",
                "deleted_id": deleted_id,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.patch("/api/manutenzioni/eventi/<int:evento_id>/azione")
@require_active_any_perm(
    "manutenzioni_esegui",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_evento_action(
    evento_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        evento = gestisci_evento_manutenzione(
            evento_id,
            payload,
            policy,
            user=active_user(),
        )
        item = serialize_evento_manutenzione(evento)

        if item.get("straordinaria_id"):
            item["straordinaria_url"] = url_for(
                "main.manutenzioni_macchinario_detail",
                macchinario_id=item["macchinario_id"],
                view="straordinarie",
                intervento_id=item["straordinaria_id"],
            )

        return jsonify(
            {
                "ok": True,
                "message": ("Evento di manutenzione aggiornato correttamente."),
                "item": item,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)







# =========================================================
# API MANUTENZIONI STRAORDINARIE
# =========================================================


@main_bp.get(
    "/api/manutenzioni/macchinari/"
    "<int:macchinario_id>/straordinarie"
)
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_straordinarie_list(
    macchinario_id: int,
):
    policy = active_policy()

    try:
        interventi = list_straordinarie_macchinario(
            macchinario_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "items": (
                    serialize_manutenzioni_straordinarie(
                        interventi
                    )
                ),
                "count": len(interventi),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.post(
    "/api/manutenzioni/macchinari/"
    "<int:macchinario_id>/straordinarie"
)
@require_active_any_perm(
    "manutenzioni_esegui",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_straordinaria_create(
    macchinario_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        intervento = (
            create_manutenzione_straordinaria(
                macchinario_id,
                payload,
                policy,
                user=active_user(),
            )
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Intervento straordinario "
                    "registrato correttamente."
                ),
                "item": (
                    serialize_manutenzione_straordinaria(
                        intervento
                    )
                ),
            }
        ), 201

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.get(
    "/api/manutenzioni/straordinarie/"
    "<int:intervento_id>"
)
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_straordinaria_get(
    intervento_id: int,
):
    policy = active_policy()

    try:
        intervento = get_manutenzione_straordinaria(
            intervento_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "item": (
                    serialize_manutenzione_straordinaria(
                        intervento
                    )
                ),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.patch(
    "/api/manutenzioni/straordinarie/"
    "<int:intervento_id>"
)
@require_active_any_perm(
    "manutenzioni_esegui",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_straordinaria_update(
    intervento_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        intervento = (
            update_manutenzione_straordinaria(
                intervento_id,
                payload,
                policy,
                user=active_user(),
            )
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Intervento straordinario "
                    "aggiornato correttamente."
                ),
                "item": (
                    serialize_manutenzione_straordinaria(
                        intervento
                    )
                ),
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


# =========================================================
# API GIORNI NON LAVORATIVI PERSONALIZZATI
# =========================================================


@main_bp.get(
    "/api/manutenzioni/giorni-non-lavorativi"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorni_non_lavorativi_list():
    policy = active_policy()

    include_inactive = _parse_query_bool(
        request.args.get("include_inactive"),
        default=True,
    )

    try:
        items = list_giorni_non_lavorativi(
            policy,
            include_inactive=include_inactive,
        )

        return jsonify(
            {
                "ok": True,
                "items": (
                    serialize_giorni_non_lavorativi(
                        items
                    )
                ),
                "count": len(items),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.post(
    "/api/manutenzioni/giorni-non-lavorativi"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorno_non_lavorativo_create():
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        items = create_giorni_non_lavorativi(
            payload,
            policy,
        )

        try:
            realignment = sync_all_active_plans(
                data_dal=today_rome(),
            )
            message = (
                f"{len(items)} giorni non lavorativi creati. "
                f"{realignment['rescheduled']} "
                "scadenze future riprogrammate."
            )
        except Exception as exc:
            current_app.logger.exception(
                "Periodo salvato, ma riallineamento eventi fallito: %s",
                exc,
            )
            realignment = None
            message = (
                f"{len(items)} giorni non lavorativi creati. "
                "Il riallineamento verrà ritentato all'apertura della pagina."
            )

        return jsonify(
            {
                "ok": True,
                "message": message,
                "item": (
                    serialize_giorno_non_lavorativo(
                        items[0]
                    )
                ),
                "items": serialize_giorni_non_lavorativi(
                    items
                ),
                "count": len(items),
                "riallineamento": realignment,
            }
        ), 201

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.get(
    "/api/manutenzioni/giorni-non-lavorativi/"
    "<int:item_id>"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorno_non_lavorativo_get(
    item_id: int,
):
    policy = active_policy()

    try:
        item = get_giorno_non_lavorativo(
            item_id,
            policy,
        )

        return jsonify(
            {
                "ok": True,
                "item": (
                    serialize_giorno_non_lavorativo(
                        item
                    )
                ),
            }
        )

    except Exception as exc:
        return _service_error_response(exc)


@main_bp.patch(
    "/api/manutenzioni/giorni-non-lavorativi/"
    "<int:item_id>"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorno_non_lavorativo_update(
    item_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    try:
        item = update_giorno_non_lavorativo(
            item_id,
            payload,
            policy,
            commit=False,
        )

        realignment = sync_all_active_plans(
            data_dal=today_rome(),
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Giorno non lavorativo aggiornato. "
                    f"{realignment['rescheduled']} "
                    "scadenze future riallineate."
                ),
                "item": (
                    serialize_giorno_non_lavorativo(
                        item
                    )
                ),
                "riallineamento": realignment,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.delete(
    "/api/manutenzioni/giorni-non-lavorativi/"
    "<int:item_id>"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorno_non_lavorativo_delete(
    item_id: int,
):
    policy = active_policy()

    try:
        delete_giorno_non_lavorativo(
            item_id,
            policy,
        )

        try:
            realignment = sync_all_active_plans(
                data_dal=today_rome(),
            )
            message = (
                "Chiusura personalizzata cancellata. "
                f"{realignment['rescheduled']} "
                "scadenze future riallineate."
            )
        except Exception as exc:
            current_app.logger.exception(
                "Chiusura cancellata, ma riallineamento eventi fallito: %s",
                exc,
            )
            realignment = None
            message = (
                "Chiusura personalizzata cancellata. "
                "Il riallineamento verrà ritentato all'apertura della pagina."
            )

        return jsonify(
            {
                "ok": True,
                "message": message,
                "riallineamento": realignment,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)


@main_bp.patch(
    "/api/manutenzioni/giorni-non-lavorativi/"
    "<int:item_id>/stato"
)
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_giorno_non_lavorativo_stato(
    item_id: int,
):
    policy = active_policy()
    payload = _macchinario_payload()

    if "attivo" not in payload:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Il campo 'attivo' è obbligatorio."
                ),
            }
        ), 400

    try:
        item = set_giorno_non_lavorativo_attivo(
            item_id,
            payload.get("attivo"),
            policy,
            commit=False,
        )

        realignment = sync_all_active_plans(
            data_dal=today_rome(),
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Giorno non lavorativo "
                    + (
                        "attivato."
                        if item.attivo
                        else "disattivato."
                    )
                    + " "
                    + str(
                        realignment["rescheduled"]
                    )
                    + " scadenze future riallineate."
                ),
                "item": (
                    serialize_giorno_non_lavorativo(
                        item
                    )
                ),
                "riallineamento": realignment,
            }
        )

    except Exception as exc:
        db.session.rollback()
        return _service_error_response(exc)




# =========================================================
# API AGGIORNAMENTO GLOBALE CALENDARIO
# =========================================================


@main_bp.post("/api/manutenzioni/calendario/aggiorna")
@require_active_any_perm(
    "manutenzioni_gestisci_piani",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_calendario_aggiorna():
    """
    Genera e riallinea le scadenze future di tutti i piani
    di manutenzione attivi.
    """
    try:
        result = sync_all_active_plans(
            data_dal=today_rome(),
        )

        return jsonify(
            {
                "ok": True,
                "message": (
                    "Calendario aggiornato: "
                    f"{result['created']} nuovi eventi, "
                    f"{result['updated']} eventi aggiornati, "
                    f"{result['rescheduled']} scadenze riallineate, "
                    f"{result['plans']} piani elaborati."
                ),
                "result": result,
            }
        )

    except Exception as exc:
        db.session.rollback()

        current_app.logger.exception(
            "Errore durante l'aggiornamento globale "
            "del calendario manutenzioni."
        )

        return _service_error_response(exc)


# =========================================================
# API CALENDARIO MANUTENZIONI
# =========================================================


@main_bp.get("/api/manutenzioni/calendario")
@require_active_any_perm(
    "manutenzioni_visualizza",
    "manutenzioni_amministrazione",
)
def api_manutenzioni_calendario():
    policy = active_policy()
    oggi = today_rome()

    default_start = oggi.replace(day=1)
    default_end = default_start + timedelta(days=41)

    data_dal = _parse_filter_date(
        request.args.get("start"),
        default=default_start,
    )

    data_fino = _parse_filter_date(
        request.args.get("end"),
        default=default_end,
    )

    if data_fino < data_dal:
        data_dal, data_fino = data_fino, data_dal

    if (data_fino - data_dal).days > 370:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "L'intervallo del calendario non può "
                    "superare 370 giorni."
                ),
            }
        ), 400

    reparto_codice = str(
        request.args.get("reparto_codice") or ""
    ).strip()

    stato = str(
        request.args.get("stato") or ""
    ).strip().upper()

    search = str(
        request.args.get("search") or ""
    ).strip()

    try:
        result = build_scadenziario_manutenzioni(
            policy,
            reparto_codice=(
                reparto_codice or None
            ),
            data_dal=data_dal,
            data_fino=data_fino,
            stato=stato or None,
            search=search or None,
        )

        items = []

        for row in result["rows"]:
            macchinario_id = row.get(
                "macchinario_id"
            )

            items.append(
                {
                    "id": row.get("id"),
                    "title": (
                        f"{row.get('macchinario_codice') or '-'}"
                        f" · {row.get('titolo') or 'Manutenzione'}"
                    ),
                    "start": row.get(
                        "data_programmata"
                    ),
                    "allDay": True,
                    "stato": row.get("stato"),
                    "stato_visuale": row.get(
                        "stato_visuale"
                    ),
                    "data_teorica": row.get(
                        "data_teorica"
                    ),
                    "data_programmata": row.get(
                        "data_programmata"
                    ),
                    "data_spostata": bool(
                        row.get("data_spostata")
                    ),
                    "motivo_spostamento": row.get(
                        "motivo_spostamento"
                    ),
                    "titolo": row.get("titolo"),
                    "descrizione": row.get(
                        "descrizione"
                    ),
                    "piano_codice": row.get(
                        "piano_codice"
                    ),
                    "macchinario_id": macchinario_id,
                    "macchinario_codice": row.get(
                        "macchinario_codice"
                    ),
                    "macchinario_descrizione": row.get(
                        "macchinario_descrizione"
                    ),
                    "reparto_codice": row.get(
                        "reparto_codice"
                    ),
                    "ubicazione": row.get(
                        "ubicazione"
                    ),
                    "macchinario_url": (
                        url_for(
                            "main.manutenzioni_macchinario_detail",
                            macchinario_id=macchinario_id,
                        )
                        if macchinario_id is not None
                        else None
                    ),
                }
            )

        holidays = (
            giorni_non_lavorativi_nel_periodo(
                data_dal,
                data_fino,
            )
        )

        return jsonify(
            {
                "ok": True,
                "items": items,
                "holidays": holidays,
                "summary": result["summary"],
                "range": {
                    "start": data_dal.isoformat(),
                    "end": data_fino.isoformat(),
                },
            }
        )

    except Exception as exc:
        current_app.logger.exception(
            "Errore durante il caricamento "
            "del calendario manutenzioni."
        )
        return _service_error_response(exc)


@main_bp.get("/manutenzioni/scadenziario")
@require_active_any_perm(
    "manutenzioni_visualizza_registro",
    "manutenzioni_amministrazione",
)
def manutenzioni_scadenziario():
    policy = active_policy()

    oggi = today_rome()

    sync_all_active_plans(data_dal=oggi)

    default_data_dal = oggi - timedelta(days=30)
    default_data_fino = oggi + timedelta(days=90)

    data_dal = _parse_filter_date(
        request.args.get("data_dal"),
        default=default_data_dal,
    )

    data_fino = _parse_filter_date(
        request.args.get("data_fino"),
        default=default_data_fino,
    )

    if data_fino < data_dal:
        data_dal, data_fino = (
            data_fino,
            data_dal,
        )

    reparto_codice = str(
        request.args.get("reparto_codice") or ""
    ).strip()

    stato = str(
        request.args.get("stato") or ""
    ).strip().upper()

    esito_straordinaria = str(
        request.args.get("esito_straordinaria") or ""
    ).strip().upper()

    search = str(
        request.args.get("search") or ""
    ).strip()

    selected_view = str(
        request.args.get("view") or "scadenziario"
    ).strip().lower()

    if selected_view not in {
        "scadenziario",
        "straordinarie",
    }:
        selected_view = "scadenziario"

    result = build_scadenziario_manutenzioni(
        policy,
        reparto_codice=(
            reparto_codice or None
        ),
        data_dal=data_dal,
        data_fino=data_fino,
        stato=stato or None,
        search=search or None,
    )

    straordinarie_result = (
        build_registro_straordinarie(
            policy,
            reparto_codice=(
                reparto_codice or None
            ),
            data_dal=data_dal,
            data_fino=data_fino,
            esito=(
                esito_straordinaria or None
            ),
            search=search or None,
        )
    )

    reparti = list_reparti_manutenzioni(
        policy
    )

    can_manage_straordinarie = policy.can(
        "manutenzioni_esegui"
    ) or policy.can(
        "manutenzioni_amministrazione"
    )

    return render_template(
        "manutenzioni/scadenziario.j2",
        rows=result["rows"],
        summary=result["summary"],
        straordinarie_rows=(
            straordinarie_result["rows"]
        ),
        straordinarie_summary=(
            straordinarie_result["summary"]
        ),
        reparti=reparti,
        selected_reparto=reparto_codice,
        selected_stato=stato,
        selected_esito_straordinaria=(
            esito_straordinaria
        ),
        selected_view=selected_view,
        data_dal=data_dal.isoformat(),
        data_fino=data_fino.isoformat(),
        search=search,
        oggi=oggi.isoformat(),
        can_manage_straordinarie=(
            can_manage_straordinarie
        ),
    )

