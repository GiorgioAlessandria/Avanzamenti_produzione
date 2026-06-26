# app_odp/routes_modules/ordini.py

from datetime import datetime
import json

from flask import current_app, jsonify, request, url_for

from app_odp.models import (
    GiacenzaLotti,
    db,
    InputOdpRuntime,
    InputOdpLog,
    OdpRuntimeLog,
)
from decimal import Decimal
from app_odp.operator_session import active_policy, active_token, operator_perm_required
from app_odp.services.ordini_log_service import (
    _add_input_odp_closure_log,
    _add_input_odp_suspend_log,
    _add_input_odp_takeover_log,
    _add_lotti_usati_logs,
    _add_lotto_generato_log,
    _append_operazione_log,
)
from app_odp.routes_blueprint import main_bp
from app_odp.services.ordini_lotti_service import _componenti_lotto_per_ordine
from app_odp.services.ordini_service import (
    _fase_corrente_for_export,
    _advance_or_finalize_phase,
)
from app_odp.services.order_helpers import (
    ROME_TZ,
    _decimal_to_text,
    _norm_text,
    _now_rome_dt,
    _parse_bool_flag,
    _parse_qty_decimal,
    _qty_da_lavorare_decimal,
    _qty_da_lavorare_text,
    _sync_active_fields_for_phase,
    _row_key,
    _parse_qty_integer_decimal,
    _parse_minuti_non_funzionamento,
    _ordine_has_distinta_materiale,
    _resolve_registration_datetime,
)
from app_odp.services.ordini_gruppi_service import (
    available_orders_payload,
    create_mascherato_group,
    create_multiplo_group,
    dissolve_group_for_single_member_close,
    finalize_group_after_member_closures,
    first_order_for_group,
    get_active_group_for_order,
    group_members_for_close_payload,
    group_to_dict,
    mark_group_member_closed,
    mark_group_member_partial_closed,
    member_payload_key,
    member_requires_time_line,
    prepare_group_for_full_closure,
    reactivate_group,
    suspend_group,
)
from app_odp.services.session_helpers import (
    _current_user_id,
    _current_username,
)
from app_odp.services.ordini_runtime_service import (
    _ensure_stato_attivo,
    _runtime_snapshot,
    _apply_stop_minutes_to_runtime,
    _accumulate_runtime_until,
    _delete_closed_order_from_runtime_db,
    _ensure_min_active_time_before_chiusura,
    _ensure_ordine_attivo_per_chiusura,
)
from app_odp.services.priorita_service import (
    _consume_priorita_ordine,
    _priorita_row_for_operatore_ordine,
    _snapshot_priorita_in_runtime,
    _restore_priorita_for_next_phase_from_runtime,
)
from app_odp.services.etichette_service import (
    _genera_e_salva_etichetta_lotto,
    generazione_lotti,
)
from app_odp.services.common import _last_log_token
from app_odp.services.erp_export_service import (
    _build_operation_group_id,
    _get_blocking_outbox_for_phase,
    _build_export_distinta_base,
    _build_phase_payload,
    _phase_export_flags,
    _queue_phase_export,
)
from app_odp.services.home_service import (
    _home_reparto_config_by_tab,
    _policy_can_access_home_config,
    _render_fragments_for_home_config,
    _get_visible_odp_by_key,
    _tab_from_ordine,
    _home_rows_for_config,
    _fragments_for_ordine_tab,
)


def _response_status_code(result) -> int:
    if isinstance(result, tuple) and len(result) >= 2 and isinstance(result[1], int):
        return result[1]
    status_code = getattr(result, "status_code", None)
    return int(status_code or 200)


def _response_json_payload(result) -> dict:
    response = result[0] if isinstance(result, tuple) else result
    getter = getattr(response, "get_json", None)
    if callable(getter):
        return getter(silent=True) or {}
    return {}


def _merge_fragment_payload(target: dict, source_result) -> None:
    payload = _response_json_payload(source_result)
    fragments = payload.get("fragments") or {}
    if isinstance(fragments, dict):
        target.update(fragments)


def _group_response_payload(group, policy, *, changed=True, message=""):
    ordine = first_order_for_group(group)
    tab, fragments = (
        _fragments_for_ordine_tab(policy, ordine) if ordine is not None else (None, {})
    )
    return {
        "ok": True,
        "changed": changed,
        "message": message,
        "group": group_to_dict(group),
        "active_tab": tab,
        "last_event_id": _last_log_token(),
        "fragments": fragments,
    }


@main_bp.get("/api/ordini/gruppi/disponibili")
@operator_perm_required("home")
def api_ordini_gruppi_disponibili():
    policy = active_policy()
    return jsonify(
        {
            "ok": True,
            "orders": available_orders_payload(policy, only_pianificata=True),
        }
    )


@main_bp.post("/api/ordini/gruppi/multiplo/apri")
@operator_perm_required("ordini_multipli")
def api_apri_gruppo_multiplo():
    data = request.get_json(silent=True) or {}
    policy = active_policy()

    try:
        group = create_multiplo_group(data.get("orders") or [], policy)
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        current_app.logger.warning("Errore validazione apertura gruppo multiplo: %s", exc)
        return jsonify({"ok": False, "error": "Richiesta non valida."}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore apertura gruppo multiplo")
        return jsonify(
            {
                "ok": False,
                "error": "Errore interno durante l'apertura del gruppo multiplo.",
            }
        ), 500

    return jsonify(
        _group_response_payload(
            group,
            policy,
            message="Gruppo multiplo aperto correttamente.",
        )
    )


@main_bp.post("/api/ordini/gruppi/mascherato/apri")
@operator_perm_required("ordini_mascherati")
def api_apri_gruppo_mascherato():
    data = request.get_json(silent=True) or {}
    policy = active_policy()

    try:
        group = create_mascherato_group(
            data.get("principale") or {},
            data.get("mascherato") or {},
            policy,
        )
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore apertura gruppo mascherato")
        return jsonify(
            {"ok": False, "error": f"Errore apertura gruppo mascherato: {exc}"}
        ), 500

    return jsonify(
        _group_response_payload(
            group,
            policy,
            message="Gruppo mascherato aperto correttamente.",
        )
    )


@main_bp.post("/api/ordini/gruppi/<group_uid>/sospendi")
@operator_perm_required("home")
def api_sospendi_gruppo_ordini(group_uid):
    data = request.get_json(silent=True) or {}
    policy = active_policy()

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )
        group = suspend_group(
            group_uid,
            causale=_norm_text(data.get("causale")),
            minuti_non_funzionamento=minuti_non_funzionamento,
        )
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore sospensione gruppo ordini")
        return jsonify(
            {"ok": False, "error": f"Errore sospensione gruppo ordini: {exc}"}
        ), 500

    return jsonify(
        _group_response_payload(
            group,
            policy,
            message="Gruppo ordini sospeso correttamente.",
        )
    )


@main_bp.post("/api/ordini/gruppi/<group_uid>/riattiva")
@operator_perm_required("home")
def api_riattiva_gruppo_ordini(group_uid):
    policy = active_policy()

    try:
        group = reactivate_group(group_uid)
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        current_app.logger.warning(
            "Errore validazione riattivazione gruppo ordini: %s", exc
        )
        return jsonify({"ok": False, "error": "Richiesta non valida."}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore riattivazione gruppo ordini")
        return jsonify(
            {"ok": False, "error": "Errore interno durante la riattivazione del gruppo ordini."}
        ), 500

    return jsonify(
        _group_response_payload(
            group,
            policy,
            message="Gruppo ordini riattivato correttamente.",
        )
    )


@main_bp.post("/api/ordini/gruppi/<group_uid>/sciogli-membro")
@operator_perm_required("home")
def api_sciogli_gruppo_per_chiusura_singola(group_uid):
    data = request.get_json(silent=True) or {}
    policy = active_policy()

    try:
        group = dissolve_group_for_single_member_close(
            group_uid,
            id_documento=_norm_text(data.get("id_documento")),
            id_riga=_norm_text(data.get("id_riga")),
        )
        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        current_app.logger.warning(
            "Errore validazione scioglimento gruppo ordini: %s", exc
        )
        return jsonify({"ok": False, "error": "Richiesta non valida."}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore scioglimento gruppo ordini")
        return jsonify(
            {"ok": False, "error": "Errore interno durante lo scioglimento del gruppo ordini."}
        ), 500

    return jsonify(
        _group_response_payload(
            group,
            policy,
            message=(
                "Gruppo sciolto. L'ordine selezionato resta attivo per la chiusura "
                "singola; gli altri ordini sono stati messi in sospeso."
            ),
        )
    )


@main_bp.post("/api/ordini/gruppi/<group_uid>/chiudi")
@operator_perm_required("home")
def api_chiudi_gruppo_ordini(group_uid):
    """
    Chiude tutti i membri del gruppo usando la stessa logica della chiusura
    ordine singolo. Ogni membro genera il proprio outbox/AVP; il gruppo resta
    una struttura interna dell'applicazione.

    Payload atteso:
    {
      "tempo_non_funzionamento_minuti": 0,
      "note": "nota comune opzionale",
      "data_registrazione": "YYYY-MM-DD" opzionale,
      "orders": [
        {
          "id_documento": "...",
          "id_riga": "...",
          "quantita_conforme": "1",
          "quantita_non_conforme": "0",
          "lotti": [...],
          "note": "nota ordine opzionale",
          "chiusura_parziale": false
        }
      ]
    }
    """
    data = request.get_json(silent=True) or {}
    policy = active_policy()
    orders_payload = data.get("orders") or data.get("ordini") or []

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )

        group = prepare_group_for_full_closure(
            group_uid,
            minuti_non_funzionamento=minuti_non_funzionamento,
        )
        members = group_members_for_close_payload(group)

        if not members:
            raise ValueError("Il gruppo non contiene ordini chiudibili.")

        payload_by_key = {}
        for row in orders_payload:
            key = f"{_norm_text(row.get('id_documento'))}|{_norm_text(row.get('id_riga'))}"
            if key.strip("|"):
                payload_by_key[key] = row

        missing = [
            member
            for member in members
            if member_payload_key(member) not in payload_by_key
        ]
        if missing:
            missing_labels = ", ".join(
                ".".join(x for x in [m.RifRegistraz, m.NumProgrRiga] if x)
                or f"{m.IdDocumento}/{m.IdRiga}"
                for m in missing
            )
            raise ValueError(
                "Payload chiusura incompleto. Mancano i dati per: " + missing_labels
            )

        merged_fragments = {}
        closure_results = []

        for member in members:
            member_payload = dict(payload_by_key[member_payload_key(member)] or {})
            member_payload["id_documento"] = member.IdDocumento
            member_payload["id_riga"] = member.IdRiga
            member_payload["note"] = _norm_text(
                member_payload.get("note") or data.get("note") or ""
            )

            if not member_payload.get("data_registrazione") and data.get(
                "data_registrazione"
            ):
                member_payload["data_registrazione"] = data.get("data_registrazione")

            # Il tempo non funzionamento è già stato sottratto una volta a livello gruppo.
            member_payload["tempo_non_funzionamento_minuti"] = 0
            member_payload["_group_close_context"] = {
                "group_uid": group.GroupUid,
                "group_type": group.GroupType,
                "member_role": member.Role,
                "time_share_mode": member.TimeShareMode,
            }

            result = _chiudi_ordine_da_payload(
                member_payload,
                policy=policy,
                commit=False,
                force_include_time_line=member_requires_time_line(member),
                skip_min_active_time=True,
            )

            status_code = _response_status_code(result)
            if status_code >= 400:
                db.session.rollback()
                return result

            _merge_fragment_payload(merged_fragments, result)
            result_payload = _response_json_payload(result)
            member_q_ok = (
                member_payload.get("quantita_conforme")
                or member_payload.get("quantita_prodotta")
                or ""
            )
            member_q_nok = (
                member_payload.get("quantita_non_conforme")
                or member_payload.get("quantita_scartata")
                or ""
            )
            member_note = member_payload.get("note") or ""

            is_partial_member_closure = (
                _parse_bool_flag(member_payload.get("chiusura_parziale"))
                or _norm_text(result_payload.get("stato_ordine")).lower()
                == "in sospeso"
            )

            if is_partial_member_closure:
                mark_group_member_partial_closed(
                    member,
                    q_ok=member_q_ok,
                    q_nok=member_q_nok,
                    note=member_note,
                )
            else:
                mark_group_member_closed(
                    member,
                    q_ok=member_q_ok,
                    q_nok=member_q_nok,
                    note=member_note,
                )

            closure_results.append(result_payload)

        finalize_group_after_member_closures(group)

        group_outbox_exports = []
        for result_row in closure_results:
            outbox_id = result_row.get("outbox_id")
            if not outbox_id:
                continue
            group_outbox_exports.append(
                {
                    "outbox_id": outbox_id,
                    "id_documento": result_row.get("id_documento"),
                    "id_riga": result_row.get("id_riga"),
                    "num_progr_riga": result_row.get("num_progr_riga"),
                    "fase": result_row.get("fase"),
                    "stato_ordine": result_row.get("stato_ordine"),
                }
            )

        db.session.commit()

    except ValueError as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Errore chiusura gruppo ordini")
        return jsonify(
            {"ok": False, "error": f"Errore chiusura gruppo ordini: {exc}"}
        ), 500

    return jsonify(
        {
            "ok": True,
            "changed": True,
            "message": (
                "Chiusura gruppo registrata. Il gruppo resta sospeso perché almeno un ordine è stato chiuso parzialmente."
                if _norm_text(group.Status).lower() == "in sospeso"
                else "Gruppo ordini chiuso correttamente. Ogni ordine ha generato il proprio export AVP."
            ),
            "group": group_to_dict(group),
            "results": closure_results,
            "exports": group_outbox_exports,
            "outbox_ids": [row["outbox_id"] for row in group_outbox_exports],
            "last_event_id": _last_log_token(),
            "fragments": merged_fragments,
        }
    )


@main_bp.post("/api/ordini/presa")
@operator_perm_required("home")
def api_prendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Presa in carico bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_log_token(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None
    if _norm_text(getattr(ordine, "CodReparto", "")) in {"10", "20", "30", "70"}:
        if not _ordine_has_distinta_materiale(ordine):
            event_at = datetime.now(ROME_TZ).isoformat(timespec="seconds")
            action_code = "blocco_distinta_materiale_assente"
            action_note = (
                "Presa in carico bloccata: distinta materiale assente. "
                "Ordine non attivabile perché il materiale non verrebbe scaricato a magazzino."
            )
            operation_group_id = (
                f"{action_code}:{ordine.IdDocumento}:{ordine.IdRiga}:{event_at}"
            )

            rt = ordine.runtime_row

            stato_odp_pre = _norm_text(getattr(rt, "Stato_odp", "")) or stato_attuale
            stato_ordine_pre = stato_attuale
            fase_pre = _norm_text(getattr(rt, "FaseAttiva", "")) or _norm_text(
                getattr(ordine, "FaseAttiva", "")
            )
            qty_pre = (
                _norm_text(getattr(rt, "QtyDaLavorare", ""))
                or _norm_text(getattr(ordine, "QtyDaLavorare", ""))
                or _norm_text(getattr(ordine, "Quantita", ""))
            )
            data_in_carico_pre = _norm_text(getattr(rt, "Data_in_carico", ""))
            data_ultima_attivazione_pre = _norm_text(
                getattr(rt, "data_ultima_attivazione", "")
            )
            tempo_funzionamento_pre = _norm_text(getattr(rt, "Tempo_funzionamento", ""))
            rif_ordine_princ = _norm_text(getattr(rt, "RifOrdinePrinc", ""))

            payload = {
                "evento": action_code,
                "motivo": action_note,
                "ordine": {
                    "IdDocumento": _norm_text(ordine.IdDocumento),
                    "IdRiga": _norm_text(ordine.IdRiga),
                    "NumProgrRiga": _norm_text(getattr(ordine, "NumProgrRiga", "")),
                    "RifRegistraz": _norm_text(getattr(ordine, "RifRegistraz", "")),
                    "CodArt": _norm_text(getattr(ordine, "CodArt", "")),
                    "DesArt": _norm_text(getattr(ordine, "DesArt", "")),
                    "CodReparto": _norm_text(getattr(ordine, "CodReparto", "")),
                    "FaseAttiva": fase_pre,
                    "GestioneLotto": _norm_text(getattr(ordine, "GestioneLotto", "")),
                    "GestioneMatricola": _norm_text(
                        getattr(ordine, "GestioneMatricola", "")
                    ),
                },
                "runtime_pre": {
                    "StatoOdp": stato_odp_pre,
                    "StatoOrdine": stato_ordine_pre,
                    "QtyDaLavorare": qty_pre,
                    "DataInCarico": data_in_carico_pre,
                    "DataUltimaAttivazione": data_ultima_attivazione_pre,
                    "TempoFunzionamento": tempo_funzionamento_pre,
                    "RifOrdinePrinc": rif_ordine_princ,
                },
                "utente": _current_username(),
            }

            try:
                db.session.add(
                    InputOdpLog(
                        OperationGroupId=operation_group_id,
                        IdDocumento=_norm_text(ordine.IdDocumento),
                        IdRiga=_norm_text(ordine.IdRiga),
                        RifRegistraz=_norm_text(getattr(ordine, "RifRegistraz", "")),
                        CodArt=_norm_text(getattr(ordine, "CodArt", "")),
                        DesArt=_norm_text(getattr(ordine, "DesArt", "")),
                        Quantita=_norm_text(getattr(ordine, "Quantita", "")),
                        NumFase=_norm_text(getattr(ordine, "NumFase", "")),
                        CodLavorazione=_norm_text(
                            getattr(ordine, "CodLavorazione", "")
                        ),
                        CodRisorsaProd=_norm_text(
                            getattr(ordine, "CodRisorsaProd", "")
                        ),
                        DataInizioSched=_norm_text(
                            getattr(ordine, "DataInizioSched", "")
                        ),
                        DataFineSched=_norm_text(getattr(ordine, "DataFineSched", "")),
                        GestioneLotto=_norm_text(getattr(ordine, "GestioneLotto", "")),
                        GestioneMatricola=_norm_text(
                            getattr(ordine, "GestioneMatricola", "")
                        ),
                        DistintaMateriale=_norm_text(
                            getattr(ordine, "DistintaMateriale", "")
                        ),
                        CodMatricola=_norm_text(getattr(ordine, "CodMatricola", "")),
                        StatoRiga=_norm_text(getattr(ordine, "StatoRiga", "")),
                        CodFamiglia=_norm_text(getattr(ordine, "CodFamiglia", "")),
                        CodMacrofamiglia=_norm_text(
                            getattr(ordine, "CodMacrofamiglia", "")
                        ),
                        CodMagPrincipale=_norm_text(
                            getattr(ordine, "CodMagPrincipale", "")
                        ),
                        CodReparto=_norm_text(getattr(ordine, "CodReparto", "")),
                        TempoPrevistoLavoraz=_norm_text(
                            getattr(ordine, "TempoPrevistoLavoraz", "")
                        ),
                        CodClassifTecnica=_norm_text(
                            getattr(ordine, "CodClassifTecnica", "")
                        ),
                        CodTipoDoc=_norm_text(getattr(ordine, "CodTipoDoc", "")),
                        FaseAttiva=fase_pre,
                        QtyDaLavorare=qty_pre,
                        RisorsaAttiva=_norm_text(getattr(ordine, "RisorsaAttiva", "")),
                        LavorazioneAttiva=_norm_text(
                            getattr(ordine, "LavorazioneAttiva", "")
                        ),
                        AttrezzaggioAttivo=_norm_text(
                            getattr(ordine, "AttrezzaggioAttivo", "")
                        ),
                        RifOrdinePrinc=rif_ordine_princ,
                        Note=action_note,
                        StatoOrdinePre=stato_ordine_pre,
                        StatoOrdinePost=stato_ordine_pre,
                        QtyDaLavorarePre=qty_pre,
                        QtyDaLavorarePost=qty_pre,
                        ClosedBy=_current_username(),
                        ClosedAt=event_at,
                        VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
                        NoteChiusura=action_note,
                    )
                )

                db.session.add(
                    OdpRuntimeLog(
                        OperationGroupId=operation_group_id,
                        EventSequence=1,
                        Topic="ordine",
                        Scope="presa_in_carico",
                        CodArt=_norm_text(getattr(ordine, "CodArt", "")),
                        CodReparto=_norm_text(getattr(ordine, "CodReparto", "")),
                        PayloadJson=json.dumps(payload, ensure_ascii=False),
                        IdDocumento=_norm_text(ordine.IdDocumento),
                        IdRiga=_norm_text(ordine.IdRiga),
                        RifRegistraz=_norm_text(getattr(ordine, "RifRegistraz", "")),
                        Azione=action_code,
                        Motivo=action_note,
                        UtenteOperazione=_current_username(),
                        EventAt=event_at,
                        StatoOdpPre=stato_odp_pre,
                        StatoOdpPost=stato_odp_pre,
                        StatoOrdinePre=stato_ordine_pre,
                        StatoOrdinePost=stato_ordine_pre,
                        FasePre=fase_pre,
                        FasePost=fase_pre,
                        DataInCaricoPre=data_in_carico_pre,
                        DataInCaricoPost=data_in_carico_pre,
                        DataUltimaAttivazionePre=data_ultima_attivazione_pre,
                        DataUltimaAttivazionePost=data_ultima_attivazione_pre,
                        TempoFunzionamentoPre=tempo_funzionamento_pre,
                        TempoFunzionamentoPost=tempo_funzionamento_pre,
                        QtyDaLavorarePre=qty_pre,
                        QtyDaLavorarePost=qty_pre,
                        Note=action_note,
                        RifOrdinePrinc=rif_ordine_princ,
                        VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
                        NumProgrRiga=_norm_text(getattr(ordine, "NumProgrRiga", "")),
                    )
                )

                db.session.commit()

            except Exception:
                db.session.rollback()
                current_app.logger.exception(
                    "Errore scrittura log per blocco distinta mancante su ordine %s/%s",
                    _norm_text(ordine.IdDocumento),
                    _norm_text(ordine.IdRiga),
                )

            return (
                jsonify(
                    ok=False,
                    error=(
                        "Ordine bloccato: distinta materiale assente. "
                        "Impossibile prendere in carico l'ordine perché il materiale "
                        "non verrebbe scaricato a magazzino. Contattare l'ufficio competente."
                    ),
                ),
                409,
            )

    if stato_norm == "pianificata":
        now_dt = _now_rome_dt()

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")
        priorita_row = _priorita_row_for_operatore_ordine(
            operatore_id=_current_user_id(),
            id_documento=ordine.IdDocumento,
            id_riga=ordine.IdRiga,
            fase=fase_corrente,
        )

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
            rif_ordine_princ="",
        )
        _snapshot_priorita_in_runtime(
            stato=stato,
            priorita_row=priorita_row,
            operatore_id=_current_user_id(),
            when_iso=now_iso,
        )
        ordine.StatoOrdine = "Attivo"
        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="presa_in_carico",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post=_norm_text(ordine.StatoOrdine),
            qty_pre=qty_pre,
            qty_post=_qty_da_lavorare_text(ordine),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento="Presa in carico ordine",
        )

        _consume_priorita_ordine(
            ordine.IdDocumento,
            ordine.IdRiga,
            ordine.FaseAttiva,
        )

        db.session.commit()
        changed = True
        message = "Ordine preso in carico"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    elif stato_norm == "in sospeso":
        message = "Ordine in sospeso: usare la riattivazione"

    else:
        message = f"Ordine non prendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/sospendi")
@operator_perm_required("home")
def api_sospendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = _current_username()

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)

        removed_seconds, tempo_funzionamento = _apply_stop_minutes_to_runtime(
            stato,
            minuti_non_funzionamento,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="sospensione",
            when_iso=now_iso,
        )

        _add_input_odp_suspend_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="In Sospeso",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            suspended_by=_current_username(),
            suspended_at=now_iso,
            causale=causale,
            minuti_non_funzionamento=minuti_non_funzionamento,
            secondi_non_funzionamento=removed_seconds,
            note_evento="Sospensione ordine",
        )
        db.session.commit()
        changed = True
        message = "Ordine sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine già in sospeso"

    else:
        message = f"Ordine non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/sospendi")
@operator_perm_required("home")
def api_sospendi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine macchina. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = _current_username()

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="sospensione",
            when_iso=now_iso,
        )

        _add_input_odp_suspend_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="In Sospeso",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            suspended_by=_current_username(),
            suspended_at=now_iso,
            causale=causale,
            minuti_non_funzionamento=None,
            secondi_non_funzionamento=None,
            note_evento="Sospensione ordine",
        )

        db.session.commit()
        changed = True
        message = "Ordine macchina sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine macchina già in sospeso"

    else:
        message = f"Ordine macchina non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)
    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/riattiva")
@operator_perm_required("home")
def api_riattiva_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)
    fase_corrente = _fase_corrente_for_export(ordine)

    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_log_token(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()

    stato_attuale = _norm_text(getattr(stato, "Stato_odp", "")) or _norm_text(
        ordine.StatoOrdine
    )
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

        _sync_active_fields_for_phase(ordine, fase_corrente)

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La riattivazione non può ripristinare correttamente il runtime."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato_ordine_pre = _norm_text(stato_attuale)
        qty_pre = _qty_da_lavorare_text(ordine, stato=stato)
        now_iso = now_dt.isoformat(timespec="seconds")
        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="riattivazione",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="Attivo",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento="Riattivazione ordine",
        )
        _sync_active_fields_for_phase(ordine, fase_corrente)
        db.session.commit()
        changed = True
        message = "Ordine riattivato"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    else:
        message = f"Ordine non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/riattiva")
@operator_perm_required("home")
def api_riattiva_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_log_token(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine macchina. "
                            "La riattivazione non può ripristinare correttamente il runtime."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato_ordine_pre = _norm_text(stato_attuale)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="riattivazione_macchina",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="Attivo",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento=f"Riattivazione ordine macchina | Matricola: {matricola}",
        )
        db.session.commit()
        changed = True
        message = "Ordine macchina riattivato"

    elif stato_norm == "attivo":
        message = "Ordine macchina già attivo"

    else:
        message = f"Ordine macchina non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


def _chiudi_ordine_da_payload(
    data: dict,
    *,
    policy=None,
    commit: bool = True,
    force_include_time_line: bool | None = None,
    skip_min_active_time: bool = False,
):
    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    q_ok_raw = data.get("quantita_conforme") or data.get("quantita_prodotta")
    q_nok_raw = data.get("quantita_non_conforme") or data.get("quantita_scartata")
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []
    chiusura_parziale = _parse_bool_flag(data.get("chiusura_parziale"))

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = policy or active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if not _parse_bool_flag(data.get("_group_close_context")) and not data.get(
        "_group_close_context"
    ):
        active_group = get_active_group_for_order(id_documento, id_riga)
        if active_group is not None:
            dissolve_group_for_single_member_close(
                active_group.GroupUid,
                id_documento=id_documento,
                id_riga=id_riga,
            )
    can_override_registration_date = policy.can("modifica_data_chiusura")
    can_choose_time_line = policy.can("export_avp_senza_riga_tempo")
    include_time_line = True

    if force_include_time_line is not None:
        include_time_line = bool(force_include_time_line)
    elif can_choose_time_line:
        include_time_line = _parse_bool_flag(data.get("include_time_line", True))

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()
    closure_error = _ensure_ordine_attivo_per_chiusura(ordine, stato=stato)
    if closure_error:
        return closure_error

    now_dt = _now_rome_dt()

    if not skip_min_active_time:
        min_time_error = _ensure_min_active_time_before_chiusura(
            stato,
            now_dt,
            can_bypass=can_choose_time_line and not include_time_line,
        )
        if min_time_error:
            return min_time_error
    try:
        q_tot = _qty_da_lavorare_decimal(ordine, stato=stato)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        q_ok = (
            _parse_qty_integer_decimal(q_ok_raw, "Quantità conforme")
            if q_ok_raw is not None
            else q_tot
        )
        q_nok = (
            _parse_qty_integer_decimal(q_nok_raw, "Quantità KO")
            if q_nok_raw is not None
            else Decimal("0")
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if q_ok < 0 or q_nok < 0:
        return (
            jsonify({"ok": False, "error": "Le quantità non possono essere negative"}),
            400,
        )

    q_lavorata = q_ok + q_nok
    qty_residua = q_tot - q_lavorata
    qty_residua_text = _decimal_to_text(qty_residua)
    qty_lavorata_text = _decimal_to_text(q_lavorata)
    qty_pre_text = _qty_da_lavorare_text(ordine, stato=stato)
    distinta_base_export = _build_export_distinta_base(
        ordine=ordine,
        fase_corrente=fase_corrente,
        q_lavorata=q_lavorata,
        q_tot=q_tot,
    )
    if chiusura_parziale:
        if q_lavorata <= 0:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Per la chiusura parziale devi indicare una quantità lavorata > 0.",
                    }
                ),
                400,
            )

        if q_lavorata >= q_tot:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Nella chiusura parziale la quantità lavorata deve essere strettamente minore della quantità totale dell'ordine.",
                    }
                ),
                400,
            )

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_integer_decimal(
                    lotto_row.get("Quantita"),
                    f"Quantità lotto {cod_art}/{rif_lotto}",
                )
            except ValueError as e:
                return (
                    jsonify({"ok": False, "error": f"Quantità lotto non valida: {e}"}),
                    400,
                )

            if not cod_art or not rif_lotto:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": "Codice e lotto obbligatori per ogni riga.",
                        }
                    ),
                    400,
                )
            if qty <= 0:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                        }
                    ),
                    400,
                )

            cod_mag = _norm_text(lotto_row.get("CodMag"))

            lotto_query = GiacenzaLotti.query.filter_by(
                CodArt=cod_art,
                RifLottoAlfa=rif_lotto,
            )

            if cod_mag:
                lotto_query = lotto_query.filter_by(CodMag=cod_mag)

            lotto_db = lotto_query.first()
            if lotto_db is None:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                        }
                    ),
                    400,
                )
            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")
            if qty > giacenza:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: qtà {qty} > giacenza {giacenza}.",
                        }
                    ),
                    400,
                )

    now_iso = now_dt.isoformat(timespec="seconds")

    try:
        _registration_day, registration_dt, registration_date_text = (
            _resolve_registration_datetime(
                data.get("data_registrazione"),
                allow_override=can_override_registration_date,
                fallback_dt=now_dt,
            )
        )
    except ValueError as e:
        current_app.logger.warning(
            "Invalid registration date during order closure.",
            exc_info=True,
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Data registrazione non valida.",
                }
            ),
            400,
        )

    registration_iso = registration_dt.isoformat(timespec="seconds")
    lotto_prodotto = None
    action_name = "chiusura_parziale" if chiusura_parziale else "chiusura_finale"
    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action=action_name,
        when_iso=now_iso,
    )

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato)
    phase_export_flags = _phase_export_flags(
        ordine,
        fase_corrente,
        chiusura_parziale=chiusura_parziale,
    )

    if _norm_text(ordine.GestioneLotto).lower() == "si" and q_ok > 0:
        rif_lotto_prodotto = generazione_lotti(registration_dt)

        for row in lotti_input:
            esito_row = _norm_text(row.get("Esito", "ok")).lower()
            if esito_row != "ok":
                continue

        lotto_prodotto = {
            "CodArt": ordine.CodArt,
            "RifLottoAlfa": rif_lotto_prodotto,
            "Quantita": _decimal_to_text(q_ok),
            "Fase": fase_corrente,
        }

    label_filename = None

    if lotto_prodotto:
        label_filename = _genera_e_salva_etichetta_lotto(
            codice=ordine.CodArt,
            descrizione=ordine.DesArt,
            lotto=lotto_prodotto["RifLottoAlfa"],
            quantita=lotto_prodotto["Quantita"],
        )

    tempo_finale = "0"
    elapsed_seconds = 0
    removed_seconds = 0
    runtime_pre = _runtime_snapshot(stato)
    stato_ordine_pre = _norm_text(ordine.StatoOrdine)
    qty_pre = _qty_da_lavorare_text(ordine)

    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            elapsed_seconds = _accumulate_runtime_until(stato, now_dt)

        removed_seconds, tempo_finale = _apply_stop_minutes_to_runtime(
            stato,
            minuti_non_funzionamento,
        )

    outbox = None

    if chiusura_parziale:
        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La chiusura non può proseguire in modo coerente."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )
        payload = _build_phase_payload(
            distinta_base=distinta_base_export,
            ordine=ordine,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            lotto_prodotto=lotto_prodotto,
            note=note,
            now_iso=registration_iso,
            registrazione_data=registration_date_text,
            chiusura_parziale=True,
            tipo_documento=ordine.CodTipoDoc,
            risorsa=ordine.RisorsaAttiva,
            magazzino=ordine.CodMagPrincipale,
            variante=ordine.VarianteArt,
            include_time_line=include_time_line,
            **phase_export_flags,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )
    else:
        payload = _build_phase_payload(
            ordine=ordine,
            distinta_base=distinta_base_export,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            lotto_prodotto=lotto_prodotto,
            note=note,
            now_iso=registration_iso,
            registrazione_data=registration_date_text,
            chiusura_parziale=False,
            tipo_documento=ordine.CodTipoDoc,
            risorsa=ordine.RisorsaAttiva,
            magazzino=ordine.CodMagPrincipale,
            variante=ordine.VarianteArt,
            include_time_line=include_time_line,
            **phase_export_flags,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )

    db.session.flush()

    note_chiusura_log = note
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )

    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=_current_username(),
    )
    if transition["tipo"] == "avanzata":
        _restore_priorita_for_next_phase_from_runtime(
            stato=stato,
            ordine=ordine,
            next_phase=transition["fase_successiva"],
        )
    runtime_post = _runtime_snapshot(stato)

    if transition["tipo"] == "finale" and stato is not None:
        runtime_post["stato_odp"] = "Chiusa"
        runtime_post["data_ultima_attivazione"] = ""

    if chiusura_parziale:
        stato_post_log = "In Sospeso"
        qty_post_log = qty_residua_text
    elif transition["tipo"] == "finale":
        stato_post_log = "Chiusa"
        qty_post_log = "0"

    note_chiusura_log = note
    stato_post_log = _norm_text(ordine.StatoOrdine)
    qty_post_log = _norm_text(ordine.QtyDaLavorare)

    if chiusura_parziale:
        stato_post_log = "In Sospeso"
        qty_post_log = qty_residua_text
    elif transition["tipo"] == "finale":
        stato_post_log = "Chiusa"
        qty_post_log = "0"
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )
    _add_input_odp_closure_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        fase_consuntivata=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        minuti_non_funzionamento=minuti_non_funzionamento,
        secondi_non_funzionamento=removed_seconds,
        chiusura_parziale=chiusura_parziale,
        note_chiusura=note_chiusura_log,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=stato_post_log,
        qty_pre=qty_pre_text,
        qty_post=qty_post_log,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _append_operazione_log(
        topic="fase_consuntivata_parziale"
        if chiusura_parziale
        else "fase_consuntivata",
        ordine=ordine,
        action=action_name,
        event_at=now_iso,
        username=_current_username(),
        runtime_pre=runtime_pre,
        runtime_post=runtime_post,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=stato_post_log,
        qty_pre=qty_pre_text,
        qty_post=qty_post_log,
        q_ok=str(q_ok),
        q_nok=str(q_nok),
        elapsed_seconds=elapsed_seconds,
        tempo_non_funzionamento_minuti=minuti_non_funzionamento,
        tempo_non_funzionamento_secondi=removed_seconds,
        note=note_chiusura_log,
        fase=fase_corrente,
        extra_payload={
            "quantita_lavorata_step": qty_lavorata_text,
            "qty_da_lavorare_pre": qty_pre_text,
            "qty_da_lavorare_post": qty_post_log,
            "lotti_count": len(lotti_input),
            "chiusura_parziale": chiusura_parziale,
            "outbox_id": outbox.outbox_id if outbox else None,
            "export_status": outbox.status if outbox else None,
            "lotto_prodotto": lotto_prodotto,
            "emit_product_line": phase_export_flags["emit_product_line"],
            "is_last_phase": phase_export_flags["is_last_phase"],
            "fase_successiva": phase_export_flags["fase_successiva"],
            "phase_sequence": phase_export_flags["phase_sequence"],
        },
    )

    _add_lotti_usati_logs(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotti_input=lotti_input,
        fase=fase_corrente,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotto_generato_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotto_prodotto=lotto_prodotto,
        closed_by=_current_username(),
        closed_at=now_iso,
        label_filename=label_filename or "",
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = _norm_text(ordine.StatoOrdine)
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)

    if chiusura_parziale:
        stato_ordine_response = "In Sospeso"
        qty_da_lavorare_response = qty_residua_text
    elif transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
    fragments = {}
    if tab:
        config = _home_reparto_config_by_tab(tab)

        if config is not None and _policy_can_access_home_config(policy, config):
            odp = _home_rows_for_config(
                policy,
                config,
                apply_priorita=True,
                sort_priorita=True,
            )
            fragments = _render_fragments_for_home_config(config, odp)

    if commit:
        db.session.commit()
    label_url = (
        url_for(
            "main.etichetta_png",
            filename=label_filename,
            tab_session=active_token(),
        )
        if label_filename
        else None
    )
    if transition["tipo"] == "finale":
        message = (
            "Ordine chiuso definitivamente, archiviato nel db_log "
            "e rimosso dal database operativo."
        )
    elif transition["tipo"] == "avanzata":
        message = (
            f"Fase {transition['fase_corrente']} consuntivata. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e riportato in pianificata sulla fase "
            f"{transition['fase_successiva']}."
        )
    else:
        message = (
            f"Fase {transition['fase_corrente']} chiusa parzialmente. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e messo in sospeso sulla stessa fase."
        )

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": stato_ordine_response,
                "qty_da_lavorare": qty_da_lavorare_response,
                "outbox_id": outbox.outbox_id if outbox else None,
                "outbox_status": outbox.status if outbox else None,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
                "label_url": label_url,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/chiudi")
@operator_perm_required("home")
def api_chiudi_ordine():
    data = request.get_json(silent=True) or {}
    return _chiudi_ordine_da_payload(data, commit=True)


@main_bp.post("/api/ordini/montaggio/macchina/chiudi")
@operator_perm_required("home")
def api_chiudi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)
    can_override_registration_date = policy.can("modifica_data_chiusura")
    can_choose_time_line = policy.can("export_avp_senza_riga_tempo")
    include_time_line = True

    if can_choose_time_line:
        include_time_line = _parse_bool_flag(data.get("include_time_line", True))

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non appartenente alla vista montaggio"}
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()

    closure_error = _ensure_ordine_attivo_per_chiusura(ordine, stato=stato)
    if closure_error:
        return closure_error

    now_dt = _now_rome_dt()

    min_time_error = _ensure_min_active_time_before_chiusura(
        stato,
        now_dt,
        can_bypass=can_choose_time_line and not include_time_line,
    )
    if min_time_error:
        return min_time_error

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
        ignore_parent_gestione_lotto=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_decimal(lotto_row.get("Quantita"))
            except ValueError as e:
                return jsonify(
                    {"ok": False, "error": f"Quantità lotto non valida: {e}"}
                ), 400

            if not cod_art or not rif_lotto:
                return jsonify(
                    {
                        "ok": False,
                        "error": "Codice e lotto sono obbligatori per ogni riga lotti.",
                    }
                ), 400

            if qty <= 0:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                    }
                ), 400

            cod_mag = _norm_text(lotto_row.get("CodMag"))

            lotto_query = GiacenzaLotti.query.filter_by(
                CodArt=cod_art,
                RifLottoAlfa=rif_lotto,
            )

            if cod_mag:
                lotto_query = lotto_query.filter_by(CodMag=cod_mag)

            lotto_db = lotto_query.first()

            if lotto_db is None:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                    }
                ), 400

            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")

            if qty > giacenza:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità {qty} supera giacenza {giacenza}.",
                    }
                ), 400

    try:
        q_tot = _qty_da_lavorare_decimal(ordine, stato=stato)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    q_ok = q_tot
    q_nok = Decimal("0")
    qty_residua = Decimal("0")
    qty_residua_text = "0"
    qty_lavorata_text = _decimal_to_text(q_tot)
    chiusura_parziale = False

    now_iso = now_dt.isoformat(timespec="seconds")

    try:
        _registration_day, registration_dt, registration_date_text = (
            _resolve_registration_datetime(
                data.get("data_registrazione"),
                allow_override=can_override_registration_date,
                fallback_dt=now_dt,
            )
        )
    except ValueError as e:
        current_app.logger.warning("Invalid registration datetime input: %s", e)
        return jsonify({"ok": False, "error": "Invalid registration data."}), 400

    registration_iso = registration_dt.isoformat(timespec="seconds")
    lotto_prodotto = None
    action_name = "chiusura_macchina"
    elapsed_seconds = 0
    minuti_non_funzionamento = 0
    removed_seconds = 0
    runtime_pre = _runtime_snapshot(stato)
    stato_ordine_pre = _norm_text(ordine.StatoOrdine)
    qty_pre = _qty_da_lavorare_text(ordine, stato=stato)
    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action="chiusura_macchina",
        when_iso=now_iso,
    )

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato, fase_override=fase)
    phase_export_flags = _phase_export_flags(
        ordine,
        fase_corrente,
        chiusura_parziale=False,
    )
    q_lavorata = q_ok + q_nok

    distinta_base_export = _build_export_distinta_base(
        ordine=ordine,
        fase_corrente=fase_corrente,
        q_lavorata=q_lavorata,
        q_tot=q_tot,
    )
    payload = _build_phase_payload(
        ordine=ordine,
        distinta_base=distinta_base_export,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        lotti_input=lotti_input,
        lotto_prodotto=None,
        note=note,
        now_iso=registration_iso,
        registrazione_data=registration_date_text,
        tipo_documento=ordine.CodTipoDoc,
        risorsa=ordine.RisorsaAttiva,
        magazzino=ordine.CodMagPrincipale,
        variante=ordine.VarianteArt,
        include_time_line=include_time_line,
        **phase_export_flags,
    )

    outbox = _queue_phase_export(
        ordine=ordine,
        fase_corrente=fase_corrente,
        payload=payload,
    )

    db.session.flush()

    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=_current_username(),
    )
    if transition["tipo"] == "avanzata":
        _restore_priorita_for_next_phase_from_runtime(
            stato=stato,
            ordine=ordine,
            next_phase=transition["fase_successiva"],
        )

    runtime_post = _runtime_snapshot(stato)

    if transition["tipo"] == "finale" and stato is not None:
        runtime_post["stato_odp"] = "Chiusa"
        runtime_post["data_ultima_attivazione"] = ""

    note_chiusura_log = note
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )
    _append_operazione_log(
        topic="fase_consuntivata_montaggio_macchina",
        ordine=ordine,
        action=action_name,
        event_at=now_iso,
        username=_current_username(),
        runtime_pre=runtime_pre,
        runtime_post=runtime_post,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=_norm_text(ordine.StatoOrdine),
        qty_pre=qty_pre,
        qty_post=_norm_text(ordine.QtyDaLavorare),
        q_ok=str(q_ok),
        q_nok=str(q_nok),
        elapsed_seconds=elapsed_seconds,
        tempo_non_funzionamento_minuti=0,
        tempo_non_funzionamento_secondi=0,
        note=note_chiusura_log,
        fase=fase_corrente,
        extra_payload={
            "matricola": matricola,
            "lotti_count": len(lotti_input),
            "outbox_id": outbox.outbox_id if outbox else None,
            "export_status": outbox.status if outbox else None,
            "emit_product_line": phase_export_flags["emit_product_line"],
            "is_last_phase": phase_export_flags["is_last_phase"],
            "fase_successiva": phase_export_flags["fase_successiva"],
            "phase_sequence": phase_export_flags["phase_sequence"],
        },
    )

    _add_input_odp_closure_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        fase_consuntivata=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        minuti_non_funzionamento=minuti_non_funzionamento,
        secondi_non_funzionamento=removed_seconds,
        chiusura_parziale=chiusura_parziale,
        note_chiusura=note_chiusura_log,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=_norm_text(ordine.StatoOrdine),
        qty_pre=qty_pre,
        qty_post=_norm_text(ordine.QtyDaLavorare),
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotti_usati_logs(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotti_input=lotti_input,
        fase=fase_corrente,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotto_generato_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotto_prodotto=lotto_prodotto,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = ordine.StatoOrdine
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)
    if transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
        message = (
            "Ordine macchina chiuso definitivamente, archiviato nel db_log "
            "e rimosso dal database operativo."
        )
    else:
        message = (
            f"Fase macchina {transition['fase_corrente']} consuntivata. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e riportato in pianificata sulla fase "
            f"{transition['fase_successiva']}."
        )
    fragments = {}
    if tab:
        config = _home_reparto_config_by_tab(tab)

        if config is not None and _policy_can_access_home_config(policy, config):
            odp = _home_rows_for_config(
                policy,
                config,
                apply_priorita=True,
                sort_priorita=True,
            )
            fragments = _render_fragments_for_home_config(config, odp)

    db.session.commit()

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": stato_ordine_response,
                "qty_da_lavorare": qty_da_lavorare_response,
                "outbox_id": outbox.outbox_id,
                "outbox_status": outbox.status,
                "active_tab": tab,
                "last_event_id": _last_log_token(),
                "fragments": fragments,
                "num_progr_riga": ordine.NumProgrRiga,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/lotti-componenti")
@operator_perm_required("home")
def api_lotti_componenti():
    data = request.get_json(silent=True) or {}
    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    modalita = _norm_text(data.get("modalita")).lower()
    is_macchina = modalita == "m"

    if not id_documento or not id_riga:
        return jsonify({"ok": False, "error": "IdDocumento e IdRiga obbligatori"}), 400

    policy = active_policy()
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if is_macchina:
        componenti_lotto = _componenti_lotto_per_ordine(
            ordine,
            include_senza_lotti=True,
            ignore_parent_gestione_lotto=True,
        )

        return jsonify(
            {
                "ok": True,
                "gestioneLotto": True,
                "force_show_section": len(componenti_lotto) > 0,
                "haComponentiLotto": any(
                    isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                    for c in componenti_lotto
                ),
                "componenti": componenti_lotto,
            }
        )

    ordine_gestione_lotto = _norm_text(ordine.GestioneLotto).lower() == "si"

    if not ordine_gestione_lotto:
        return jsonify(
            {
                "ok": True,
                "gestioneLotto": False,
                "haComponentiLotto": False,
                "componenti": [],
            }
        )

    componenti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )

    return jsonify(
        {
            "ok": True,
            "gestioneLotto": True,
            "force_show_section": len(componenti_lotto) > 0,
            "haComponentiLotto": any(
                isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                for c in componenti_lotto
            ),
            "componenti": componenti_lotto,
        }
    )
