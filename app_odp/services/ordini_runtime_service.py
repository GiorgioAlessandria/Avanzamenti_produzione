from datetime import datetime

from app_odp.models import db, InputOdpRuntime, OdpWorkGroup, OdpWorkGroupMember
from app_odp.services.order_helpers import (
    _norm_text,
    _qty_da_lavorare_text,
    _parse_iso_dt,
    _tempo_to_seconds,
    _seconds_to_tempo_text,
)
from flask import jsonify

MIN_SECONDS_BEFORE_CLOSE_WITHOUT_TIME_PERMISSION = 180
ACTIVE_OPERATOR_BLOCK_MESSAGE = (
    "Operatore gia impegnato su un altro ordine attivo. "
    "Sospendere o chiudere l'ordine prima di aprirne uno nuovo."
)
ORDER_ACTIVE_OTHER_OPERATOR_MESSAGE = (
    "Ordine gia attivo da un altro operatore. "
    "Mettere in sospeso l'altro ordine prima di attivare quello desiderato."
)


def _runtime_order_key(id_documento: str, id_riga: str) -> tuple[str, str]:
    return _norm_text(id_documento), _norm_text(id_riga)


def _active_group_uid_for_order(id_documento: str, id_riga: str) -> str:
    member = (
        OdpWorkGroupMember.query.filter_by(
            IdDocumento=_norm_text(id_documento),
            IdRiga=_norm_text(id_riga),
        )
        .filter(OdpWorkGroupMember.Status == "Attivo")
        .order_by(OdpWorkGroupMember.id.desc())
        .first()
    )
    if member is None:
        return ""

    group = OdpWorkGroup.query.filter_by(GroupUid=member.GroupUid).first()
    if group is None or _norm_text(group.Status) != "Attivo":
        return ""

    return _norm_text(group.GroupUid)


def _active_operator_activity_keys(username: str) -> set[tuple]:
    username_norm = _norm_text(username)
    if not username_norm:
        return set()

    keys = set()
    rows = InputOdpRuntime.query.filter_by(
        Stato_odp="Attivo",
        Utente_operazione=username_norm,
    ).all()

    for row in rows:
        group_uid = _active_group_uid_for_order(row.IdDocumento, row.IdRiga)
        if group_uid:
            keys.add(("group", group_uid))
        else:
            keys.add(("order", *_runtime_order_key(row.IdDocumento, row.IdRiga)))

    return keys


def _ensure_order_not_active_for_other_operator(
    id_documento: str, id_riga: str, username: str
) -> None:
    runtime = InputOdpRuntime.query.filter_by(
        IdDocumento=_norm_text(id_documento),
        IdRiga=_norm_text(id_riga),
    ).first()
    if runtime is None:
        return

    if _norm_text(runtime.Stato_odp).lower() != "attivo":
        return

    owner = _norm_text(runtime.Utente_operazione)
    if owner and owner != _norm_text(username):
        raise ValueError(ORDER_ACTIVE_OTHER_OPERATOR_MESSAGE)


def _ensure_operator_has_no_conflicting_active(
    username: str, allowed_order_keys=()
) -> None:
    allowed = {_runtime_order_key(doc, row) for doc, row in allowed_order_keys}

    for key in _active_operator_activity_keys(username):
        if key[0] == "order" and (key[1], key[2]) in allowed:
            continue
        raise ValueError(ACTIVE_OPERATOR_BLOCK_MESSAGE)


def _ensure_operator_can_activate_order(
    id_documento: str, id_riga: str, username: str
) -> None:
    key = _runtime_order_key(id_documento, id_riga)
    _ensure_order_not_active_for_other_operator(*key, username)
    _ensure_operator_has_no_conflicting_active(username, [key])


def _ensure_operator_can_activate_group(order_keys, username: str) -> None:
    keys = [_runtime_order_key(doc, row) for doc, row in order_keys]
    for id_documento, id_riga in keys:
        _ensure_order_not_active_for_other_operator(id_documento, id_riga, username)
    _ensure_operator_has_no_conflicting_active(username, keys)


def _ensure_stato_attivo(
    ordine,
    stato,
    username: str,
    when_dt: datetime,
    fase_corrente: str,
    rif_ordine_princ: str | None = None,
):
    now_iso = when_dt.isoformat(timespec="seconds")

    if stato is None:
        stato = InputOdpRuntime(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            Stato_odp="Attivo",
            Data_in_carico=now_iso,
            Tempo_funzionamento="0",
            Utente_operazione=username,
            FaseAttiva=fase_corrente,
            data_ultima_attivazione=now_iso,
            Note=_norm_text(getattr(ordine, "Note", "")),
            QtyDaLavorare=_qty_da_lavorare_text(ordine),
            RisorsaAttiva=_norm_text(getattr(ordine, "RisorsaAttiva", "")),
            LavorazioneAttiva=_norm_text(getattr(ordine, "LavorazioneAttiva", "")),
            AttrezzaggioAttivo=_norm_text(getattr(ordine, "AttrezzaggioAttivo", "")),
            RifOrdinePrinc=rif_ordine_princ,
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        )
        db.session.add(stato)
        return stato

    stato.Stato_odp = "Attivo"
    stato.Utente_operazione = username

    fase_precedente = _norm_text(getattr(stato, "FaseAttiva", ""))

    if fase_corrente:
        if fase_precedente and fase_precedente != _norm_text(fase_corrente):
            stato.Tempo_funzionamento = "0"
            stato.Data_in_carico = None
            stato.data_ultima_attivazione = None

        stato.FaseAttiva = fase_corrente

    if not _norm_text(stato.Data_in_carico):
        stato.Data_in_carico = now_iso

    if not _norm_text(stato.Tempo_funzionamento):
        stato.Tempo_funzionamento = "0"

    if rif_ordine_princ is not None:
        stato.RifOrdinePrinc = rif_ordine_princ

    stato.VarianteArt = _norm_text(getattr(ordine, "VarianteArt", ""))
    stato.data_ultima_attivazione = now_iso
    return stato


def _reset_runtime_for_next_phase(
    stato,
    ordine,
    username: str,
    next_phase: str,
):
    if stato is None:
        return

    next_phase = _norm_text(next_phase)

    stato.Stato_odp = "Pianificata"
    stato.Utente_operazione = username
    stato.FaseAttiva = next_phase
    stato.Tempo_funzionamento = "0"
    stato.data_ultima_attivazione = None
    stato.Data_in_carico = None
    stato.QtyDaLavorare = _norm_text(getattr(ordine, "QtyDaLavorare", ""))
    stato.RisorsaAttiva = _norm_text(getattr(ordine, "RisorsaAttiva", ""))
    stato.LavorazioneAttiva = _norm_text(getattr(ordine, "LavorazioneAttiva", ""))
    stato.AttrezzaggioAttivo = _norm_text(getattr(ordine, "AttrezzaggioAttivo", ""))
    stato.VarianteArt = _norm_text(getattr(ordine, "VarianteArt", ""))


def _set_runtime_sospeso(
    stato,
    username: str,
    fase_corrente: str,
    qty_residua_text: str = "",
):
    if stato is None:
        return
    stato.Stato_odp = "In Sospeso"
    stato.Utente_operazione = username
    if fase_corrente:
        stato.FaseAttiva = fase_corrente
    if qty_residua_text != "":
        stato.QtyDaLavorare = qty_residua_text
    stato.data_ultima_attivazione = None


def _runtime_snapshot(stato) -> dict:
    return {
        "stato_odp": _norm_text(getattr(stato, "Stato_odp", "")),
        "fase": _norm_text(getattr(stato, "FaseAttiva", "")),
        "data_in_carico": _norm_text(getattr(stato, "Data_in_carico", "")),
        "data_ultima_attivazione": _norm_text(
            getattr(stato, "data_ultima_attivazione", "")
        ),
        "tempo_funzionamento": _norm_text(getattr(stato, "Tempo_funzionamento", "")),
        "qty_da_lavorare": _norm_text(getattr(stato, "QtyDaLavorare", "")),
        "utente_operazione": _norm_text(getattr(stato, "Utente_operazione", "")),
        "rif_ordine_princ": _norm_text(getattr(stato, "RifOrdinePrinc", "")),
    }


def _ensure_stop_minutes_within_elapsed(
    minuti_non_funzionamento: int,
    elapsed_seconds: int,
) -> None:
    requested_seconds = max(0, int(minuti_non_funzionamento or 0)) * 60
    measured_seconds = max(0, int(elapsed_seconds or 0))
    if requested_seconds <= measured_seconds:
        return

    hours, remainder = divmod(measured_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    raise ValueError(
        "Il tempo non funzionamento inserito supera il tempo misurato "
        "dall'ultima attivazione. "
        f"Tempo massimo disponibile: {hours:02d}:{minutes:02d}:{seconds:02d}."
    )


def _apply_stop_minutes_to_runtime(
    stato,
    minuti_non_funzionamento: int,
    *,
    max_removable_seconds: int,
) -> tuple[int, str]:
    if stato is None or minuti_non_funzionamento <= 0:
        return 0, _norm_text(getattr(stato, "Tempo_funzionamento", "")) or "0"

    _ensure_stop_minutes_within_elapsed(
        minuti_non_funzionamento,
        max_removable_seconds,
    )

    total_seconds = _tempo_to_seconds(stato.Tempo_funzionamento)
    requested_seconds = minuti_non_funzionamento * 60

    removable_seconds = min(
        requested_seconds,
        total_seconds,
        max(0, int(max_removable_seconds)),
    )

    new_total_seconds = max(0, total_seconds - removable_seconds)
    stato.Tempo_funzionamento = _seconds_to_tempo_text(new_total_seconds)

    return removable_seconds, _norm_text(stato.Tempo_funzionamento) or "0"


def _accumulate_runtime_until(stato, end_dt: datetime) -> int:
    if stato is None:
        return 0

    start_dt = _parse_iso_dt(stato.data_ultima_attivazione)
    if start_dt is None:
        stato.data_ultima_attivazione = None
        if not _norm_text(stato.Tempo_funzionamento):
            stato.Tempo_funzionamento = "0"
        return 0

    elapsed_seconds = max(0, int((end_dt - start_dt).total_seconds()))
    total_seconds = _tempo_to_seconds(stato.Tempo_funzionamento) + elapsed_seconds

    stato.Tempo_funzionamento = _seconds_to_tempo_text(total_seconds)
    stato.data_ultima_attivazione = None
    return elapsed_seconds


def _delete_closed_order_from_runtime_db(ordine, stato=None) -> None:
    """
    Elimina l'ordine dal DB runtime principale dopo aver già salvato tutto nel db_log.
    Cancella InputOdpRuntime solo se la riga esiste ancora, poi cancella InputOdp.
    """
    id_documento = _norm_text(getattr(ordine, "IdDocumento", ""))
    id_riga = _norm_text(getattr(ordine, "IdRiga", ""))

    if id_documento and id_riga:
        (
            db.session.query(InputOdpRuntime)
            .filter(
                InputOdpRuntime.IdDocumento == id_documento,
                InputOdpRuntime.IdRiga == id_riga,
            )
            .delete(synchronize_session=False)
        )

    if ordine is not None:
        db.session.delete(ordine)

    db.session.flush()


def _ensure_min_active_time_before_chiusura(
    stato,
    now_dt: datetime,
    *,
    can_bypass: bool,
    min_seconds: int = MIN_SECONDS_BEFORE_CLOSE_WITHOUT_TIME_PERMISSION,
):
    """
    Impedisce la chiusura troppo rapida agli operatori senza permission
    export_avp_senza_riga_tempo.

    Usa data_ultima_attivazione come riferimento principale.
    Se manca, usa Data_in_carico come fallback.
    """
    if can_bypass:
        return None

    if stato is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: runtime ordine non trovato. "
                        "Riattivare l'ordine prima della chiusura."
                    ),
                }
            ),
            409,
        )

    start_dt = _parse_iso_dt(
        getattr(stato, "data_ultima_attivazione", "")
    ) or _parse_iso_dt(getattr(stato, "Data_in_carico", ""))

    if start_dt is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: data di attivazione non disponibile. "
                        "Riattivare l'ordine e attendere almeno 3 minuti prima della chiusura."
                    ),
                }
            ),
            409,
        )

    elapsed_seconds = max(0, int((now_dt - start_dt).total_seconds()))

    if elapsed_seconds >= min_seconds:
        return None

    remaining_seconds = min_seconds - elapsed_seconds
    remaining_minutes = (remaining_seconds + 59) // 60

    return (
        jsonify(
            {
                "ok": False,
                "error": (
                    "Ordine non chiudibile: attendere almeno 3 minuti "
                    "dalla presa in carico o dall'ultima riattivazione. "
                    f"Tempo residuo circa {remaining_minutes} min."
                ),
            }
        ),
        409,
    )


def _stato_operativo_chiusura(ordine, stato=None) -> str:
    """
    Stato reale da usare per decidere se un ordine è chiudibile.

    Priorità:
    1. runtime.Stato_odp
    2. ordine.StatoOrdine
    """
    stato_runtime = _norm_text(getattr(stato, "Stato_odp", ""))
    if stato_runtime:
        return stato_runtime

    return _norm_text(getattr(ordine, "StatoOrdine", ""))


def _ensure_ordine_attivo_per_chiusura(ordine, stato=None):
    stato_attuale = _stato_operativo_chiusura(ordine, stato=stato)
    stato_norm = stato_attuale.lower()

    if stato_norm == "attivo":
        return None

    if stato_norm == "in sospeso":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: è in sospeso. "
                        "Riattiva l'ordine prima della chiusura."
                    ),
                }
            ),
            409,
        )

    if stato_norm == "pianificata":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non chiudibile: è ancora Pianificata.",
                }
            ),
            409,
        )

    return (
        jsonify(
            {
                "ok": False,
                "error": f"Ordine non chiudibile: stato attuale '{stato_attuale or '-'}'.",
            }
        ),
        409,
    )
