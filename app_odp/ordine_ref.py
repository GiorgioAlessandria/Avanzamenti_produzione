from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def _norm_text(value) -> str:
    return str(value or "").strip()


def _first_not_blank(*values) -> str:
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return ""


def format_erp_decimal_ref_part(value) -> str:
    """
    Converte parti numeriche del riferimento gestionale:
    5      -> 5,00
    5.0    -> 5,00
    5,0    -> 5,00
    1.0    -> 1,00
    """
    raw = _norm_text(value)
    if not raw:
        return ""

    try:
        number = Decimal(raw.replace(",", "."))
    except (InvalidOperation, ValueError):
        return raw

    number = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(number, "f").replace(".", ",")


def format_ordine_ref_display(
    rif_registraz,
    num_progr_riga=None,
    id_riga=None,
) -> str:
    """
    Formato visuale gestionale:
    2026.1.252 5,00
    """
    rif = _norm_text(rif_registraz)
    riga_raw = _first_not_blank(num_progr_riga, id_riga)
    riga = format_erp_decimal_ref_part(riga_raw)

    if rif and riga:
        return f"{rif} {riga}"

    return rif or riga


def format_ordine_ref_export(
    rif_registraz,
    num_progr_riga=None,
    id_riga=None,
    fase=None,
) -> str:
    """
    Formato TXT gestionale:
    senza fase -> 2026.1.252.5,00
    con fase   -> 2026.1.252.5,00.1,00
    """
    rif = _norm_text(rif_registraz)
    riga_raw = _first_not_blank(num_progr_riga, id_riga)
    riga = format_erp_decimal_ref_part(riga_raw)

    parts = []
    if rif:
        parts.append(rif)
    if riga:
        parts.append(riga)

    fase_txt = format_erp_decimal_ref_part(fase)
    if fase_txt:
        parts.append(fase_txt)

    return ".".join(parts)


def format_ordine_ref_display_from_ordine(ordine) -> str:
    return format_ordine_ref_display(
        getattr(ordine, "RifRegistraz", ""),
        getattr(ordine, "NumProgrRiga", ""),
        getattr(ordine, "IdRiga", ""),
    )
