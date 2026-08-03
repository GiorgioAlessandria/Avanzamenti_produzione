from decimal import Decimal
import json
from pathlib import Path

from flask import current_app
import re
from app_odp.models import db, ErpOutbox, InputOdp, InputOdpLog
from app_odp.services.session_helpers import _current_username
from app_odp.services.order_helpers import (
    _norm_text,
    _now_rome_dt,
    _get_phase_transition,
    _phase_sequence_for_ordine,
    _parse_distinta_materiale,
    _fase_to_int,
    _scaled_component_qty,
    _component_udm,
    _decimal_to_text_for_udm,
)


def _safe_txt_suffix(value: str, fallback: str = "export") -> str:
    raw = _norm_text(value)
    if not raw:
        return fallback

    out = []
    for ch in raw:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")

    cleaned = "".join(out).strip("_")
    return cleaned or fallback


def _safe_txt_prefix(value: str, fallback: str = "AVPB") -> str:
    raw = _norm_text(value)
    if not raw:
        return fallback

    out = []
    for ch in raw:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")

    cleaned = "".join(out).strip("_")
    return cleaned or fallback


def _get_erp_export_dir() -> Path:
    """
    Recupera la cartella export dai config caricati nell'app factory.
    Se manca, usa una cartella locale di fallback.
    """
    raw = current_app.config.get("ERP_EXPORT_DIR", "")
    if raw:
        export_dir = Path(raw)
    else:
        export_dir = Path(current_app.instance_path) / "erp_exports"

    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir


def _build_export_txt_path(
    prefix: str = "AVPB",
    suffix: str = "",
    output_dir: str | Path | None = None,
) -> Path:
    now_txt = _now_rome_dt().strftime("%Y%m%d_%H%M%S")
    safe_prefix = _safe_txt_prefix(prefix, "AVPB")
    safe_suffix = _safe_txt_suffix(suffix, "export")
    file_name = f"{safe_prefix}_{safe_suffix}_{now_txt}.txt"

    export_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir
        else _get_erp_export_dir().resolve()
    )
    if not export_dir.exists() or not export_dir.is_dir():
        raise ValueError("La cartella di output non esiste o non è valida.")

    candidate_path = (export_dir / file_name).resolve()
    try:
        candidate_path.relative_to(export_dir)
    except ValueError:
        raise ValueError("Invalid export path: resolved path escapes export directory.")

    return candidate_path


def _write_txt_content(
    lines: list[str],
    *,
    prefix: str = "AVPB",
    suffix: str = "",
    encoding: str = "utf-8",
    output_dir: str | Path | None = None,
) -> Path:
    path_txt = _build_export_txt_path(
        prefix=prefix,
        suffix=suffix,
        output_dir=output_dir,
    )
    content = "\n".join(lines) + "\n"
    path_txt.write_text(content, encoding=encoding, newline="\r\n")
    return path_txt


def _json_loads_safe(raw, default):
    try:
        return json.loads(raw)
    except Exception:
        return default


def _get_pending_avp_outbox(outbox_id: int | None = None) -> list[ErpOutbox]:
    q = ErpOutbox.query.filter(
        ErpOutbox.kind == "consuntivo_fase",
        ErpOutbox.status == "pending",
    )

    if outbox_id is not None:
        q = q.filter(ErpOutbox.outbox_id == outbox_id)

    return q.order_by(ErpOutbox.outbox_id.asc()).all()


def _get_outbox_payload(outbox: ErpOutbox) -> dict:
    payload = _json_loads_safe(outbox.payload_json or "{}", {})
    return payload if isinstance(payload, dict) else {}


def _get_pending_avp_export_rows(outbox_id: int | None = None) -> list[dict]:
    rows = []
    for outbox in _get_pending_avp_outbox(outbox_id=outbox_id):
        rows.append(
            {
                "outbox": outbox,
                "payload": _get_outbox_payload(outbox),
                "source_row": _get_export_source_row(outbox),
            }
        )
    return rows


def _get_export_source_row(outbox: ErpOutbox):
    """
    Prova prima su InputOdp corrente.
    Se non esiste più, ripiega sull'ultimo snapshot InputOdpLog.
    """
    ordine = InputOdp.query.filter_by(
        IdDocumento=outbox.IdDocumento,
        IdRiga=outbox.IdRiga,
    ).first()
    if ordine is not None:
        return ordine

    return (
        InputOdpLog.query.filter_by(
            IdDocumento=outbox.IdDocumento,
            IdRiga=outbox.IdRiga,
        )
        .order_by(InputOdpLog.log_id.desc())
        .first()
    )


def _build_operation_group_id(ordine, action: str, when_iso: str) -> str:
    stamp = re.sub(r"\D+", "", _norm_text(when_iso))[:14]
    if not stamp:
        stamp = _now_rome_dt().strftime("%Y%m%d%H%M%S")

    return (
        f"{stamp}_"
        f"{_safe_txt_suffix(_norm_text(ordine.IdDocumento), 'doc')}_"
        f"{_safe_txt_suffix(_norm_text(ordine.IdRiga), 'riga')}_"
        f"{_safe_txt_suffix(_norm_text(action), 'op')}"
    )


def _get_blocking_outbox_for_phase(
    id_documento: str,
    id_riga: str,
    fase: str,
):
    fase = _norm_text(fase)
    if not fase:
        return None

    return (
        ErpOutbox.query.filter_by(
            IdDocumento=id_documento,
            IdRiga=id_riga,
            Fase=fase,
        )
        .filter(ErpOutbox.status.in_(["pending", "error"]))
        .order_by(ErpOutbox.outbox_id.desc())
        .first()
    )


def _normalize_lotti_for_payload(lotti_input: list[dict]) -> list[dict]:
    rows = []
    for row in lotti_input or []:
        rows.append(
            {
                "CodArt": _norm_text(row.get("CodArt")),
                "VarianteArt": _norm_text(row.get("VarianteArt")),
                "RifLottoAlfa": _norm_text(row.get("RifLottoAlfa")),
                "CodMag": _norm_text(row.get("CodMag")),
                "Quantita": str(row.get("Quantita", 0)),
                "Esito": _norm_text(row.get("Esito", "ok")),
            }
        )
    return rows


def _normalize_lotto_prodotto_for_payload(lotto: dict | None) -> dict | None:
    if not lotto:
        return None

    return _norm_text(lotto.get("RifLottoAlfa"))


def _phase_export_flags(
    ordine,
    fase_corrente: str,
    *,
    chiusura_parziale: bool = False,
) -> dict:
    """
    Determina se questa fase deve generare product_line nel TXT ERP.

    Regola:
    - monofase: product_line
    - multifase: product_line solo sull'ultima fase
    - chiusura parziale: product_line con salda_riga=0 e quantità effettive
    """
    is_last_phase, next_phase = _get_phase_transition(ordine, fase_corrente)
    phase_sequence = _phase_sequence_for_ordine(ordine)

    return {
        "is_last_phase": bool(is_last_phase),
        "fase_successiva": next_phase or "",
        "phase_sequence": phase_sequence,
        "emit_product_line": bool(is_last_phase),
    }


def _build_phase_payload(
    ordine,
    distinta_base,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    lotti_input: list[dict],
    lotto_prodotto: dict | None,
    note: str,
    now_iso: str,
    registrazione_data: str = "",
    chiusura_parziale: bool = False,
    tipo_documento: str = "",
    risorsa: str = "",
    magazzino: str = "",
    variante: str = "",
    tempo_avanzamento_minuti: int | None = None,
    tempo_avanzamento_ore: str | None = None,
    emit_product_line: bool | None = None,
    is_last_phase: bool | None = None,
    fase_successiva: str | None = None,
    phase_sequence: list[str] | None = None,
) -> dict:
    salda_riga = 0 if chiusura_parziale is True else 1
    if is_last_phase is None or fase_successiva is None:
        calc_is_last_phase, calc_next_phase = _get_phase_transition(
            ordine,
            fase_corrente,
        )

        if is_last_phase is None:
            is_last_phase = calc_is_last_phase

        if fase_successiva is None:
            fase_successiva = calc_next_phase or ""

    if phase_sequence is None:
        phase_sequence = _phase_sequence_for_ordine(ordine)

    if emit_product_line is None:
        emit_product_line = bool(is_last_phase)
    tempo_avanzamento_forzato = tempo_avanzamento_ore is not None
    return {
        "kind": "consuntivo_fase",
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "rif_registraz": ordine.RifRegistraz,
        "cod_art": ordine.CodArt,
        "descrizione": ordine.DesArt,
        "fase": fase_corrente,
        "quantita_ok": str(q_ok),
        "quantita_ko": str(q_nok),
        "tempo_funzionamento": tempo_finale,
        "tempo_funzionamento_calcolato": tempo_finale,
        "tempo_avanzamento_forzato": tempo_avanzamento_forzato,
        "tempo_avanzamento_minuti": (
            tempo_avanzamento_minuti if tempo_avanzamento_forzato else None
        ),
        "tempo_avanzamento_ore": (
            tempo_avanzamento_ore if tempo_avanzamento_forzato else tempo_finale
        ),
        "tempo_avanzamento_operatore": (
            _current_username() if tempo_avanzamento_forzato else ""
        ),
        "note": note,
        "lotti": _normalize_lotti_for_payload(lotti_input),
        "lotto_prodotto": _normalize_lotto_prodotto_for_payload(lotto_prodotto),
        "created_at": now_iso,
        "created_by": _current_username(),
        "registrazione_data": registrazione_data,
        "salda_riga": salda_riga,
        "tipo_documento": tipo_documento,
        "risorsa": risorsa,
        "magazzino": magazzino,
        "distinta_base": distinta_base,
        "variante": variante,
        "num_progr_riga": ordine.NumProgrRiga,
        "emit_product_line": bool(emit_product_line),
        "is_last_phase": bool(is_last_phase),
        "fase_successiva": _norm_text(fase_successiva),
        "phase_sequence": phase_sequence,
        "num_fase": ordine.NumFase,
    }


def _queue_phase_export(ordine, fase_corrente: str, payload: dict):
    outbox = ErpOutbox(
        kind="consuntivo_fase",
        status="pending",
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        CodArt=ordine.CodArt,
        Fase=fase_corrente,
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    db.session.add(outbox)
    db.session.flush()
    return outbox


def _build_export_distinta_base(
    ordine,
    fase_corrente: str,
    q_lavorata: Decimal,
    q_tot: Decimal,
) -> str:
    distinta = _parse_distinta_materiale(ordine)
    fase_corrente_int = _fase_to_int(fase_corrente)

    out = []

    for progressivo_riga, comp in enumerate(distinta, start=1):
        if not isinstance(comp, dict):
            continue

        comp_fase = _fase_to_int(comp.get("NumFase"))
        if fase_corrente_int is not None and comp_fase != fase_corrente_int:
            continue

        qty_scalata = _scaled_component_qty(
            comp.get("Quantita"),
            q_lavorata=q_lavorata,
            q_tot=q_tot,
        )

        out.append(
            {
                **comp,
                "ProgressivoRiga": _norm_text(
                    comp.get("IdRigacomponente")
                    or comp.get("ProgressivoRiga")
                    or progressivo_riga
                ),
                "Quantita": _decimal_to_text_for_udm(qty_scalata, _component_udm(comp)),
                "VarianteArt": _norm_text(comp.get("VarianteArt", "")),
            }
        )

    return json.dumps(out, ensure_ascii=False)
