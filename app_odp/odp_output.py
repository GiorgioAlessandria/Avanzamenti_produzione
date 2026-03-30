from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import re


DEFAULT_AVP_CFG = {
    # TES
    "tes_tipo_documento": 704,
    "tes_numero_registrazione": 0,
    "tes_appendice": "",
    # RIG
    "rig_tipo_op_qta": 702,
    "rig_tipo_op_ore": 709,
    "rig_magazzino_principale": "0",
    "rig_causale_prestazione": 0,
    # campo 90
    # possibili valori:
    # - "raw_rif_registraz"
    # - "riga"
    # - "riga_fase"
    # - "barcode17"
    # - "barcode22"
    "rif90_mode": "raw_rif_registraz",
    # quantità esportata:
    # - "ok"      => solo quantita_ok
    # - "worked"  => quantita_ok + quantita_ko
    "qta_mode": "ok",
    # compatibilità con il comportamento attuale
    "include_header": False,
    # per ora lasciato disattivo per non cambiare il tracciato esistente
    "include_tes": False,
}

AVP_COLUMNS = [
    "TipoRecord",  # 1
    "TESTipoDoc",  # 10
    "TESDataReg",  # 20
    "TESNReg",  # 30
    "TESApp",  # 40
    "RIGTipoOpAvp",  # 80
    "RIGRifORP",  # 90
    "RIGCodArt",  # 100
    "RIGQta",  # 140
    "RIGMagPrinc",  # 210
    "RIGCodRisorsa",  # 300
    "RIGCausalePrest",  # 310
    "RIGOreLav",  # 322
]


def _text(value) -> str:
    return str(value or "").strip()


def _parse_decimal(value) -> Decimal:
    raw = _text(value).replace(",", ".")
    if raw == "":
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return Decimal("0")


def _format_decimal_it(value, places: int = 2) -> str:
    dec = _parse_decimal(value)
    quant = Decimal("1") if places <= 0 else Decimal("1." + ("0" * places))
    dec = dec.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{dec:.{places}f}".replace(".", ",")


def _format_datetime_for_avp(value) -> str:
    raw = _text(value)
    if not raw:
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            pass

    return raw


def _zero_fill_digits(value, width: int) -> str:
    raw = re.sub(r"\D+", "", _text(value))
    if not raw:
        raw = "0"
    return raw.zfill(width)


def _parse_jsonish_list(value) -> list[str]:
    if value in (None, ""):
        return []

    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw = str(value).strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return [raw]
        raw_items = parsed if isinstance(parsed, list) else [parsed]

    out = []
    for item in raw_items:
        s = _text(item)
        if s:
            out.append(s)
    return out


def _fase_to_int(value) -> int | None:
    s = _text(value)
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _parse_phase_list(value) -> list[str]:
    raw = _text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        out = []
        for item in parsed:
            fase_int = _fase_to_int(item)
            if fase_int is not None and fase_int > 0:
                out.append(str(fase_int))
        return out

    totale_fasi = _fase_to_int(raw)
    if totale_fasi is not None and totale_fasi > 0:
        return [str(i) for i in range(1, totale_fasi + 1)]

    return []


def _active_value_for_phase(raw_values, raw_phases, fase_corrente: str) -> str:
    values = _parse_jsonish_list(raw_values)
    phases = _parse_phase_list(raw_phases)
    fase_corrente = _text(fase_corrente)

    if not values:
        return ""

    if phases and len(phases) == len(values):
        for fase, value in zip(phases, values):
            if fase == fase_corrente:
                return _text(value)

    fase_int = _fase_to_int(fase_corrente)
    if fase_int is not None:
        idx = fase_int - 1
        if 0 <= idx < len(values):
            return _text(values[idx])

    return _text(values[0])


def _build_rif_orp(payload: dict, cfg: dict) -> str:
    mode = _text(cfg.get("rif90_mode")) or "raw_rif_registraz"

    rif_reg = _text(payload.get("rif_registraz"))
    id_doc = _text(payload.get("id_documento"))
    id_riga = _text(payload.get("id_riga"))
    fase = _text(payload.get("fase"))

    if mode == "raw_rif_registraz":
        return rif_reg

    if mode == "riga":
        if rif_reg:
            return ".".join([x for x in [rif_reg, id_riga] if x])
        return ""

    if mode == "riga_fase":
        if rif_reg:
            return ".".join([x for x in [rif_reg, id_riga, fase] if x])
        return ""

    if mode == "barcode17":
        return (
            f"{_zero_fill_digits(id_doc, 9)}"
            f"{_zero_fill_digits(id_riga, 4)}"
            f"{_zero_fill_digits(fase, 4)}"
        )

    if mode == "barcode22":
        return (
            f"{_zero_fill_digits(id_doc, 9)}"
            f"{_zero_fill_digits(id_riga, 9)}"
            f"{_zero_fill_digits(fase, 4)}"
        )

    return rif_reg


def _pick_resource_code(source_row, fase_corrente: str) -> str:
    if source_row is None:
        return ""

    raw_risorse = _text(getattr(source_row, "CodRisorsaProd", ""))
    raw_fasi = _text(getattr(source_row, "NumFase", ""))

    by_phase = _active_value_for_phase(raw_risorse, raw_fasi, fase_corrente)
    if by_phase:
        return by_phase

    risorsa_attiva = _text(getattr(source_row, "RisorsaAttiva", ""))
    if risorsa_attiva:
        return risorsa_attiva

    return raw_risorse


def _pick_magazzino_principale(source_row, cfg: dict) -> str:
    if source_row is None:
        return _text(cfg.get("rig_magazzino_principale", "0"))

    cod_mag = _text(getattr(source_row, "CodMagPrincipale", ""))
    return cod_mag or _text(cfg.get("rig_magazzino_principale", "0"))


def _pick_tipo_documento(source_row, cfg: dict):
    tipo_doc = _text(getattr(source_row, "CodTipoDoc", "")) if source_row else ""
    return tipo_doc or cfg.get("tes_tipo_documento", 704)


def _pick_qta_export(payload: dict, cfg: dict) -> Decimal:
    q_ok = _parse_decimal(payload.get("quantita_ok"))
    q_ko = _parse_decimal(payload.get("quantita_ko"))

    mode = _text(cfg.get("qta_mode")) or "ok"
    if mode == "worked":
        return q_ok + q_ko

    return q_ok


def _serialize_avp_cell(value, numeric: bool = False) -> str:
    if value is None:
        value = ""

    text = str(value)

    if numeric:
        return text

    escaped = text.replace('"', '""')
    return f'"{escaped}"'


def _serialize_avp_row(values: list, numeric_indexes: set[int] | None = None) -> str:
    numeric_indexes = numeric_indexes or set()
    rendered = []
    for idx, value in enumerate(values):
        rendered.append(_serialize_avp_cell(value, numeric=(idx in numeric_indexes)))
    return ";".join(rendered)


def _build_tes_row(first_payload: dict, source_row, cfg: dict) -> list:
    tipo_doc = _pick_tipo_documento(source_row, cfg)
    data_reg = _format_datetime_for_avp(first_payload.get("created_at"))
    n_reg = cfg.get("tes_numero_registrazione", 0)
    tes_app = cfg.get("tes_appendice", "")

    return [
        "TES",
        tipo_doc,
        data_reg,
        n_reg,
        tes_app,
        0,
        "",
        "",
        _format_decimal_it(0, 2),
        "",
        "",
        0,
        _format_decimal_it(0, 3),
    ]


def _build_rig_row(payload: dict, source_row, cfg: dict) -> list | None:
    qta = _pick_qta_export(payload, cfg)
    ore = _parse_decimal(payload.get("tempo_funzionamento"))

    if qta <= 0 and ore <= 0:
        return None

    tipo_doc = _pick_tipo_documento(source_row, cfg)
    data_reg = _format_datetime_for_avp(payload.get("created_at"))
    n_reg = cfg.get("tes_numero_registrazione", 0)
    tes_app = cfg.get("tes_appendice", "")
    fase = _text(payload.get("fase"))

    return [
        "RIG",
        tipo_doc,
        data_reg,
        n_reg,
        tes_app,
        cfg.get("rig_tipo_op_qta", 702),
        _build_rif_orp(payload, cfg),
        _text(payload.get("cod_art")),
        _format_decimal_it(qta, 2),
        _pick_magazzino_principale(source_row, cfg),
        _pick_resource_code(source_row, fase),
        cfg.get("rig_causale_prestazione", 0),
        _format_decimal_it(ore, 3),
    ]


def txt_generator(export_rows: list[dict], cfg: dict | None = None) -> str:
    """
    Struttura txt
    Testata
    1:tipo record; TES
    10: tipo documento; "70[0-9]"
    20: registrazione data; dd/mm/YYYY HH:MM
    30: registrazione numero;
    40: registrazione appendice digitata;
    80: tipo operazione avanzamento; ""
    90: riferimento ordine produzione; "2008.1.15.1,00 "
    100: Codice articolo; ""
    140: Quantità principale; ""
    150: Quantità scarti prima scelta; ""
    160: Quantità scarti seconda scelta; ""
    290: Riga saldata; ""
    340: Riferimento lotto pf:codice alfanumerico; ""
    210: magazzino principale; ""
    300: Codice risorsa; ""
    310: Causale prestazione; ""
    322: ore lavorate risorsa 1; ""
    Riga
    1: tipo record; RIG
    10: tipo documento; ""
    20: registrazione data "";
    30: registrazione numero "";
    40: registrazione appendice digitata "";
    80: tipo operazione avanzamento "3[0-9]" esempio: 701;
    90: riferimento ordine produzione "4[0-9].[0-9].2[0-9].[0-9],2[0-9]." es. 2008.1.15.1,00;
    100: Codice articolo "2[A-Z]2[0-9]-3[0-9]-4[0-9]" es. BE12-345-6789;
    140: Quantità principale "3[0-9]";
    150: Quantità scarti prima scelta "3[0-9]";
    160: Quantità scarti seconda scelta "3[0-9]";
    290: Riga saldata "1/0" es. 0;
    340: Riferimento lotto pf:codice alfanumerico "6[0-9]" es. 612345;
    210: magazzino principale "[0-9]?[0-9]" es. 0;
    300: Codice risorsa "3[A-Z]" es. ASS;
    310: Causale prestazione "";
    322: ore lavorate risorsa 1 "2[0-9],2[0-9]" es.23,58;
    """
    if not export_rows:
        raise ValueError("Nessun record pending da esportare")

    final_cfg = dict(DEFAULT_AVP_CFG)
    if cfg:
        final_cfg.update(cfg)

    lines = []

    if final_cfg.get("include_header"):
        lines.append(_serialize_avp_row(AVP_COLUMNS))

    if final_cfg.get("include_tes"):
        first_payload = export_rows[0].get("payload") or {}
        first_source_row = export_rows[0].get("source_row")
        tes_row = _build_tes_row(first_payload, first_source_row, final_cfg)
        lines.append(
            _serialize_avp_row(
                tes_row,
                numeric_indexes={1, 3, 5, 8, 11, 12},
            )
        )

    for row in export_rows:
        payload = row.get("payload") or {}
        source_row = row.get("source_row")

        rig_row = _build_rig_row(payload, source_row, final_cfg)
        if rig_row is not None:
            lines.append(
                _serialize_avp_row(
                    rig_row,
                    numeric_indexes={1, 3, 5, 8, 11, 12},
                )
            )

    if not lines:
        raise ValueError("Nessuna riga AVP esportabile")

    payload = {}

    return "\n".join(lines) + "\n"
