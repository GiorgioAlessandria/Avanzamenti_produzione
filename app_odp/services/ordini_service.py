# app_odp/services/ordini_service.py

from decimal import Decimal

from app_odp.routes import (
    _decimal_to_text,
    _fase_to_int,
    _norm_text,
    _reset_runtime_for_next_phase,
    _set_runtime_sospeso,
    _sync_active_fields_for_phase,
    _phase_sequence_for_ordine,
    _get_phase_transition,
)


def _fase_corrente_for_export(ordine, stato=None, fase_override="") -> str:
    raw = (
        _norm_text(fase_override)
        or _norm_text(getattr(stato, "FaseAttiva", ""))
        or _norm_text(getattr(ordine, "FaseAttiva", ""))
    )
    fase_int = _fase_to_int(raw)
    if fase_int is not None and fase_int > 0:
        return str(fase_int)

    fasi = _phase_sequence_for_ordine(ordine)
    if len(fasi) == 1:
        return fasi[0]

    return ""


def _advance_or_finalize_phase(
    *,
    ordine,
    stato,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    qty_residua: Decimal,
    qty_residua_text: str,
    qty_lavorata_text: str,
    chiusura_parziale: bool,
    username: str,
):
    is_last_phase, next_phase = _get_phase_transition(ordine, fase_corrente)

    if chiusura_parziale:
        _set_runtime_sospeso(
            stato,
            username,
            fase_corrente,
            qty_residua_text=qty_residua_text,
        )
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": fase_corrente,
            "fase_successiva": fase_corrente,
        }

    if is_last_phase:
        ordine.StatoOrdine = "Chiusa"
        ordine.FaseAttiva = fase_corrente
        ordine.QtyDaLavorare = "0"
        _sync_active_fields_for_phase(ordine, fase_corrente)
        return {
            "tipo": "finale",
            "fase_corrente": fase_corrente,
            "fase_successiva": None,
        }

    ordine.StatoOrdine = "Pianificata"
    ordine.FaseAttiva = next_phase
    ordine.QtyDaLavorare = _decimal_to_text(q_ok)

    # Aggiorna RisorsaAttiva, LavorazioneAttiva, AttrezzaggioAttivo
    # in base alla nuova fase.
    _sync_active_fields_for_phase(ordine, next_phase)

    # Azzera il runtime per la nuova fase.
    _reset_runtime_for_next_phase(
        stato=stato,
        ordine=ordine,
        username=username,
        next_phase=next_phase,
    )

    return {
        "tipo": "avanzata",
        "fase_corrente": fase_corrente,
        "fase_successiva": next_phase,
    }
