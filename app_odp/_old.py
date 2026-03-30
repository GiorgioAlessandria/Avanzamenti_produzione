def _format_datetime_for_avp(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return _now_rome_dt().strftime("%d/%m/%Y %H:%M:%S")

    dt = _parse_iso_dt(raw)
    if dt is not None:
        return dt.strftime("%d/%m/%Y %H:%M:%S")

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=ROME_TZ)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            pass

    return raw


def _format_decimal_it(value, places: int = 2) -> str:
    try:
        dec = _parse_qty_decimal(value)
    except ValueError:
        dec = Decimal("0")

    quant = Decimal("1") if places <= 0 else Decimal("1." + ("0" * places))
    dec = dec.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{dec:.{places}f}".replace(".", ",")


def _zero_fill_digits(value, width: int) -> str:
    raw = re.sub(r"\D+", "", _norm_text(value))
    if not raw:
        raw = "0"
    return raw.zfill(width)


def _build_rif_orp(payload: dict, cfg: dict) -> str:
    mode = _norm_text(cfg.get("rif90_mode")) or "raw_rif_registraz"

    rif_reg = _norm_text(payload.get("rif_registraz"))
    id_doc = _norm_text(payload.get("id_documento"))
    id_riga = _norm_text(payload.get("id_riga"))
    fase = _norm_text(payload.get("fase"))

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

    raw_risorse = _norm_text(getattr(source_row, "CodRisorsaProd", ""))
    raw_fasi = _norm_text(getattr(source_row, "NumFase", ""))

    # Per l'export conta la risorsa della fase esportata,
    # non la RisorsaAttiva corrente della riga.
    by_phase = _active_value_for_phase(raw_risorse, raw_fasi, fase_corrente)
    if by_phase:
        return by_phase

    risorsa_attiva = _norm_text(getattr(source_row, "RisorsaAttiva", ""))
    if risorsa_attiva:
        return risorsa_attiva

    return raw_risorse


def _pick_magazzino_principale(source_row, cfg: dict) -> str:
    if source_row is None:
        return _norm_text(cfg.get("rig_magazzino_principale", "0"))
    return _first_not_blank(
        getattr(source_row, "CodMagPrincipale", ""),
        cfg.get("rig_magazzino_principale", "0"),
    )


def _pick_tipo_documento(source_row, cfg: dict):
    tipo_doc = _norm_text(getattr(source_row, "CodTipoDoc", "")) if source_row else ""
    return tipo_doc or cfg.get("tes_tipo_documento", 704)


def _pick_qta_export(payload: dict, cfg: dict) -> Decimal:
    try:
        q_ok = _parse_qty_decimal(payload.get("quantita_ok"))
    except ValueError:
        q_ok = Decimal("0")

    try:
        q_ko = _parse_qty_decimal(payload.get("quantita_ko"))
    except ValueError:
        q_ko = Decimal("0")

    mode = _norm_text(cfg.get("qta_mode")) or "ok"
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
        "TES",  # 1
        tipo_doc,  # 10
        data_reg,  # 20
        n_reg,  # 30
        tes_app,  # 40
        0,  # 80
        "",  # 90
        "",  # 100
        _format_decimal_it(0, 2),  # 140
        "",  # 210
        "",  # 300
        0,  # 310
        _format_decimal_it(0, 3),  # 322
    ]


def _build_rig_row(payload: dict, source_row, cfg: dict) -> list | None:
    qta = _pick_qta_export(payload, cfg)

    try:
        ore = _parse_qty_decimal(payload.get("tempo_funzionamento"))
    except ValueError:
        ore = Decimal("0")

    # se entrambe zero/non valorizzate, non esportare nulla
    if qta <= 0 and ore <= 0:
        return None

    tipo_doc = _pick_tipo_documento(source_row, cfg)
    data_reg = _format_datetime_for_avp(payload.get("created_at"))
    n_reg = cfg.get("tes_numero_registrazione", 0)
    tes_app = cfg.get("tes_appendice", "")

    fase = _norm_text(payload.get("fase"))

    return [
        "RIG",  # 1
        tipo_doc,  # 10
        data_reg,  # 20
        n_reg,  # 30
        tes_app,  # 40
        cfg.get("rig_tipo_op_qta", 702),  # 80
        _build_rif_orp(payload, cfg),  # 90
        _norm_text(payload.get("cod_art")),  # 100
        _format_decimal_it(qta, 2),  # 140
        _pick_magazzino_principale(source_row, cfg),  # 210
        _pick_resource_code(source_row, fase),  # 300
        cfg.get("rig_causale_prestazione", 0),  # 310
        _format_decimal_it(ore, 3),  # 322
    ]


def _build_avp_txt_content(outbox_rows: list[ErpOutbox]) -> str:
    if not outbox_rows:
        raise ValueError("Nessun record pending da esportare")

    cfg = _erp_avp_cfg()
    lines = []

    if cfg.get("include_header"):
        lines.append(_serialize_avp_row(AVP_COLUMNS))

    for outbox in outbox_rows:
        payload = _get_outbox_payload(outbox)
        source_row = _get_export_source_row(outbox)

        rig_row = _build_rig_row(payload, source_row, cfg)
        if rig_row is not None:
            lines.append(
                _serialize_avp_row(
                    rig_row,
                    numeric_indexes={1, 3, 5, 8, 11, 12},
                )
            )

    return "\n".join(lines) + "\n"
