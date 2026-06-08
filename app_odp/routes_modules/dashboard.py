# app_odp/routes_modules/dashboard.py

from io import BytesIO

from flask import abort, jsonify, render_template, request, send_file
from openpyxl import Workbook

from app_odp.operator_session import (
    active_token,
    active_user,
    operator_or_login_required,
)
from app_odp.policy.decorator import require_active_perm

from app_odp.routes import (
    main_bp,
    _current_policy,
    _json_safe,
    _norm_text,
)

from app_odp.services.dashboard_service import (
    _add_aggregate_sheet,
    _build_dashboard_kpi_payload,
    _build_kpi_snapshot_payload,
    _dashboard_build_cruscotto_payload,
    _dashboard_produzione_allowed_sections,
    _dashboard_produzione_default_section,
    _dashboard_produzione_initial_payload,
    _write_kpi_summary_sheet,
    _write_sheet_from_rows,
)
from app_odp.routes import (
    main_bp,
    _current_policy,
    _first_not_blank,
    _json_safe,
    _norm_text,
    _ordine_ref_label,
    _safe_float,
)
from app_odp.models import (
    InputOdp,
    InputOdpRuntime,
    User,
    user_roles,
)


def _hours_to_work_days(total_hours: float, hours_per_day: float = 8.0) -> float:
    total = float(total_hours or 0.0)
    if total <= 0:
        return 0.0
    return round(total / hours_per_day, 2)


def _order_hours_snapshot_reparto(ordine: InputOdp) -> float:
    fase_attiva = _norm_text(getattr(ordine, "FaseAttiva", "")) or "1"
    ore_lavorazione = InputOdp._active_value_from_phase_list(
        getattr(ordine, "TempoPrevistoLavoraz", ""),
        fase_attiva,
    )
    ore_lavorazione_val = _safe_float(ore_lavorazione)

    return ore_lavorazione_val


@main_bp.get("/dashboard-produzione")
@operator_or_login_required
def dashboard_produzione():
    policy = _current_policy()

    allowed_sections = _dashboard_produzione_allowed_sections(policy)

    if not allowed_sections["cruscotto"] and not allowed_sections["kpi"]:
        abort(403)

    requested_section = _norm_text(request.args.get("section")).lower()
    default_section = _dashboard_produzione_default_section(policy)

    if requested_section not in {"cruscotto", "kpi"}:
        active_section = default_section
    elif requested_section == "cruscotto" and not allowed_sections["cruscotto"]:
        active_section = default_section
    elif requested_section == "kpi" and not allowed_sections["kpi"]:
        active_section = default_section
    else:
        active_section = requested_section

    if not active_section:
        abort(403)

    return render_template(
        "dashboard_produzione.j2",
        active_section=active_section,
        dashboard_payload=_dashboard_produzione_initial_payload(policy),
        allowed_sections=allowed_sections,
        tab_session=active_token(),
        operator_user=active_user(),
        operator_policy=policy,
        policy=policy,
    )


@main_bp.get("/api/dashboard-produzione/cruscotto")
@require_active_perm("dashboard_produzione")
def api_dashboard_produzione_cruscotto():
    policy = _current_policy()

    data = _dashboard_build_cruscotto_payload(policy)

    return jsonify(
        {
            "ok": True,
            "data": _json_safe(data),
        }
    ), 200


@main_bp.get("/api/dashboard-produzione/kpi")
@require_active_perm("kpi_produzione")
def api_dashboard_produzione_kpi():
    data = _build_dashboard_kpi_payload()

    return jsonify(
        {
            "ok": True,
            "data": _json_safe(data),
        }
    ), 200


@main_bp.get("/api/dashboard-produzione/kpi/export")
@require_active_perm("kpi_export")
def api_dashboard_produzione_kpi_export():
    data = _build_dashboard_kpi_payload(detail_limit=None)

    wb = Workbook()

    ws_summary = wb.active
    _write_kpi_summary_sheet(ws_summary, data)

    ws_detail = wb.create_sheet(title="Dettaglio")
    detail_headers = [
        ("data_chiusura", "Data chiusura"),
        ("azione", "Azione"),
        ("ordine", "Ordine"),
        ("id_documento", "IdDocumento"),
        ("id_riga", "IdRiga"),
        ("rif_registraz", "RifRegistraz"),
        ("articolo", "CodArt"),
        ("descrizione", "Descrizione"),
        ("reparto", "Reparto"),
        ("risorsa", "Risorsa"),
        ("lavorazione", "Lavorazione"),
        ("operatore", "Operatore"),
        ("fase", "Fase"),
        ("stato", "Stato"),
        ("tempo_previsto_ore", "Tempo previsto ore"),
        ("tempo_reale_ore", "Tempo reale ore"),
        ("scostamento_ore", "Scostamento ore"),
        ("scostamento_percentuale", "% scostamento"),
        ("data_fine_prevista", "Data fine prevista"),
        ("ritardo_giorni", "Ritardo giorni"),
        ("is_collaudo", "Collaudo"),
        ("operation_group_id", "OperationGroupId"),
    ]
    _write_sheet_from_rows(ws_detail, detail_headers, data.get("details") or [])

    aggregati = data.get("aggregati") or {}
    _add_aggregate_sheet(wb, "Reparti", aggregati.get("reparti") or [])
    _add_aggregate_sheet(wb, "Risorse", aggregati.get("risorse") or [])
    _add_aggregate_sheet(wb, "Lavorazioni", aggregati.get("lavorazioni") or [])
    _add_aggregate_sheet(wb, "Operatori", aggregati.get("operatori") or [])
    _add_aggregate_sheet(wb, "Articoli", aggregati.get("articoli") or [])

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filters = data.get("filters") or {}
    date_from = filters.get("date_from", "")
    date_to = filters.get("date_to", "")

    filename = f"kpi_produzione_{date_from}_{date_to}.xlsx"

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


@main_bp.get("/api/dashboard-produzione/kpi/snapshots")
@require_active_perm("kpi_produzione")
def api_dashboard_produzione_kpi_snapshots():
    data = _build_kpi_snapshot_payload()

    return jsonify(
        {
            "ok": True,
            "data": _json_safe(data),
        }
    ), 200


@main_bp.get("/api/dash-reparto")
@main_bp.get("/dash-reparto")
@require_active_perm("dash_reparto")
def dash_reparto():
    user = active_user()
    manageable_role_ids = user.manageable_role_ids
    utenti_subordinati = []

    if manageable_role_ids:
        utenti_subordinati = (
            User.query.join(user_roles, user_roles.c.user_id == User.id)
            .filter(
                User.active.is_(True),
                User.id != user.id,
                user_roles.c.role_id.in_(manageable_role_ids),
            )
            .distinct()
            .order_by(User.username.asc())
            .all()
        )

    utenti_data = {}
    utenti_data[user.username] = {
        "id": user.id,
        "username": user.username,
        "is_current": True,
        "kpi": {
            "attivi": 0,
            "sospesi": 0,
            "ore_lavorazione_attivi": 0.0,
            "ore_lavorazione_sospesi": 0.0,
            "minuti_attrezzaggio_attivi": 0.0,
            "minuti_attrezzaggio_sospesi": 0.0,
        },
        "ordini_attivi": [],
        "ordini_sospesi": [],
    }

    for utente in utenti_subordinati:
        utenti_data[utente.username] = {
            "id": utente.id,
            "username": utente.username,
            "is_current": False,
            "kpi": {
                "attivi": 0,
                "sospesi": 0,
                "ore_lavorazione_attivi": 0.0,
                "ore_lavorazione_sospesi": 0.0,
                "minuti_attrezzaggio_attivi": 0.0,
                "minuti_attrezzaggio_sospesi": 0.0,
                "giorni_impegno_attivi": 0.0,
                "giorni_impegno_attivi_sospesi": 0.0,
            },
            "ordini_attivi": [],
            "ordini_sospesi": [],
        }

    if utenti_data:
        ordini = (
            InputOdp.query.join(InputOdp.runtime_row)
            .filter(
                InputOdpRuntime.Stato_odp.in_(("Attivo", "In Sospeso")),
                InputOdpRuntime.Utente_operazione.in_(list(utenti_data.keys())),
            )
            .all()
        )

        for ordine in ordini:
            runtime = ordine.runtime_row
            if runtime is None:
                continue

            username_operatore = _norm_text(runtime.Utente_operazione)
            if username_operatore not in utenti_data:
                continue

            ore_lavorazione = _order_hours_snapshot_reparto(ordine)
            minuti_attrezzaggio = _safe_float(getattr(ordine, "AttrezzaggioAttivo", ""))

            record = {
                "ordine": _ordine_ref_label(ordine),
                "descrizione": _norm_text(ordine.DesArt),
                "quantita": _norm_text(ordine.Quantita),
                "risorsa": _first_not_blank(
                    runtime.RisorsaAttiva,
                    InputOdp._active_value_from_phase_list(
                        ordine.CodRisorsaProd,
                        ordine.FaseAttiva,
                    ),
                    default="-",
                ),
                "tempo_lavorazione": round(ore_lavorazione, 2),
                "tempo_attrezzaggio": round(minuti_attrezzaggio, 2),
            }

            stato_runtime = _norm_text(runtime.Stato_odp).lower()

            if stato_runtime == "attivo":
                bucket_key = "ordini_attivi"
                utenti_data[username_operatore]["kpi"]["ore_lavorazione_attivi"] += (
                    ore_lavorazione
                )
                utenti_data[username_operatore]["kpi"][
                    "minuti_attrezzaggio_attivi"
                ] += minuti_attrezzaggio
            else:
                bucket_key = "ordini_sospesi"
                utenti_data[username_operatore]["kpi"]["ore_lavorazione_sospesi"] += (
                    ore_lavorazione
                )
                utenti_data[username_operatore]["kpi"][
                    "minuti_attrezzaggio_sospesi"
                ] += minuti_attrezzaggio

            utenti_data[username_operatore][bucket_key].append(record)

    for payload in utenti_data.values():
        payload["kpi"]["attivi"] = len(payload["ordini_attivi"])
        payload["kpi"]["sospesi"] = len(payload["ordini_sospesi"])
        payload["kpi"]["ore_lavorazione_attivi"] = round(
            payload["kpi"]["ore_lavorazione_attivi"],
            2,
        )
        payload["kpi"]["ore_lavorazione_sospesi"] = round(
            payload["kpi"]["ore_lavorazione_sospesi"],
            2,
        )
        payload["kpi"]["minuti_attrezzaggio_attivi"] = round(
            payload["kpi"]["minuti_attrezzaggio_attivi"],
            2,
        )
        payload["kpi"]["minuti_attrezzaggio_sospesi"] = round(
            payload["kpi"]["minuti_attrezzaggio_sospesi"],
            2,
        )
        ore_attivi_tot = payload["kpi"]["ore_lavorazione_attivi"] + (
            payload["kpi"]["minuti_attrezzaggio_attivi"] / 60.0
        )

        ore_attivi_sospesi_tot = payload["kpi"]["ore_lavorazione_sospesi"] + (
            payload["kpi"]["minuti_attrezzaggio_sospesi"] / 60.0
        )

        payload["kpi"]["giorni_impegno_attivi"] = _hours_to_work_days(ore_attivi_tot)
        payload["kpi"]["giorni_impegno_attivi_sospesi"] = _hours_to_work_days(
            ore_attivi_sospesi_tot
        )

    lista_utenti = sorted(
        utenti_data.values(),
        key=lambda x: (
            0 if x.get("is_current") else 1,
            (x.get("username") or "").lower(),
        ),
    )

    return render_template(
        "dash_reparto.j2",
        utenti_dashboard=lista_utenti,
    )
