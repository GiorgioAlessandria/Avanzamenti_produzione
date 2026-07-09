# app_odp/services/acquisti_service.py

from openpyxl import Workbook
from openpyxl.styles import Font

from app_odp.models import (
    AcqArticoli,
    AcqArticoliLookup,
    AcqGiacenze,
    AcqScortaSegnalata,
    db,
)
from datetime import datetime, timedelta
from app_odp.services.ordini_query_service import (
    _base_odp_query,
)
from decimal import Decimal, InvalidOperation
from uuid import uuid4
from app_odp.services.order_helpers import (
    _ordine_ref_label,
    _norm_text,
    _normalize_variante_articolo_search,
    _decimal_to_text,
    _parse_distinta_materiale,
    _parse_qty_decimal,
    _qty_requires_integer_udm,
    _safe_float,
    _qty_da_lavorare_text,
    _qty_da_lavorare_decimal,
    _scaled_component_qty,
    _fase_to_int,
    _remaining_phase_codes_for_ordine,
    _normalize_indice_articolo_search,
    _first_code_from_cell,
    _active_value_for_phase,
    _ordine_state_rank,
    _now_rome_dt,
)


def _build_acquisti_ordini_rows() -> dict:
    ordini = _base_odp_query().all()

    buckets = {
        "montaggio_sl": [],
        "montaggio_m": [],
        "officina": [],
        "carpenteria": [],
    }

    for ordine in ordini:
        stato = _ordine_stato_effettivo(ordine)
        stato_norm = _norm_text(stato).lower()

        if not _is_open_order_state(stato):
            continue

        runtime = getattr(ordine, "runtime_row", None)
        qty = _qty_da_lavorare_text(ordine, stato=runtime)

        fase_attiva = (
            _norm_text(getattr(runtime, "FaseAttiva", ""))
            or _norm_text(getattr(ordine, "FaseAttiva", ""))
            or "1"
        )

        reparto_attivo_raw = _active_value_for_phase(
            getattr(ordine, "CodReparto", ""),
            getattr(ordine, "NumFase", ""),
            fase_attiva,
        )
        reparto_attivo = _first_code_from_cell(
            reparto_attivo_raw
        ) or _first_code_from_cell(getattr(ordine, "CodReparto", ""))

        ordine_produzione = _ordine_ref_label(ordine)

        row_sl = {
            "OrdineProduzione": ordine_produzione,
            "CodArt": _norm_text(getattr(ordine, "CodArt", "")),
            "VarianteArt": _norm_text(getattr(ordine, "VarianteArt", "")),
            "Revisione": _normalize_indice_articolo_search(
                getattr(ordine, "IndiceModifica", "")
            ),
            "DesArt": _norm_text(getattr(ordine, "DesArt", "")),
            "Qty": qty,
            "Stato": stato,
        }

        row_m = {
            "OrdineProduzione": ordine_produzione,
            "CodArt": _norm_text(getattr(ordine, "CodArt", "")),
            "DesArt": _norm_text(getattr(ordine, "DesArt", "")),
            "Qty": qty,
            "Stato": stato,
        }

        is_macchina = (
            _norm_text(getattr(ordine, "GestioneMatricola", "")).lower() == "si"
        )

        if reparto_attivo == "10":
            if is_macchina:
                buckets["montaggio_m"].append(row_m)
            else:
                buckets["montaggio_sl"].append(row_sl)

        elif reparto_attivo == "20":
            buckets["officina"].append(row_sl)

        elif reparto_attivo == "30":
            buckets["carpenteria"].append(row_sl)

    for key in ("montaggio_sl", "officina", "carpenteria"):
        buckets[key].sort(
            key=lambda x: (
                _ordine_state_rank(x.get("Stato", "")),
                (x.get("OrdineProduzione") or "").lower(),
                (x.get("CodArt") or "").lower(),
                (x.get("VarianteArt") or "").lower(),
                (x.get("Revisione") or "").lower(),
                (x.get("DesArt") or "").lower(),
            )
        )

    buckets["montaggio_m"].sort(
        key=lambda x: (
            _ordine_state_rank(x.get("Stato", "")),
            (x.get("OrdineProduzione") or "").lower(),
            (x.get("CodArt") or "").lower(),
            (x.get("DesArt") or "").lower(),
        )
    )

    return buckets


ACQUISTI_MAGAZZINI_GIACENZA = ("0", "6", "10", "11", "12", "13")
ACQUISTI_MAGAZZINI_MATERIALE = ("0",)
ACQUISTI_MAGAZZINI_GIACENZA_CONTROLLO = ("6", "0", "10", "11", "12", "13")
ACQUISTI_MAGAZZINI_GIACENZA_LABELS = {
    "6": "6-Accettazione",
    "0": "0-Principale",
    "10": "10-Scarti",
    "11": "11-Obsoleto",
    "12": "12-DEMO",
    "13": "13-Rottamare",
}


def _decimal_fraction_text(value) -> str:
    try:
        qty = _parse_qty_decimal(value)
    except ValueError:
        return ""

    if qty == qty.to_integral_value():
        return ""

    return _decimal_to_text(qty)


def _acquisti_giacenza_decimal_warnings(row: dict) -> list[str]:
    if not _qty_requires_integer_udm(row.get("MagUM")):
        return []

    warnings = []
    for mag in ACQUISTI_MAGAZZINI_GIACENZA_CONTROLLO:
        qty_text = _decimal_fraction_text(row.get(f"Mag_{mag}", 0))
        if qty_text:
            label = ACQUISTI_MAGAZZINI_GIACENZA_LABELS.get(mag, mag)
            warnings.append(f"{label}: {qty_text}")

    return warnings


def _apply_acquisti_giacenza_controls(row: dict) -> dict:
    warnings = _acquisti_giacenza_decimal_warnings(row)
    row["MagazziniDecimaliNonValidi"] = warnings
    row["HasMagazziniDecimaliNonValidi"] = bool(warnings)
    row["MagazziniDecimaliNonValidiText"] = (
        "UdM intera con decimali: " + ", ".join(warnings) if warnings else ""
    )
    return row


def _build_acquisti_giacenze_map(
    magazzini_codes: tuple[str, ...],
) -> dict[str, dict[str, float]]:
    grouped: dict[str, dict[str, float]] = {}
    allowed_mags = {_normalize_acq_mag_code(mag) for mag in magazzini_codes}

    for giacenza in AcqGiacenze.query.all():
        cod_art = _norm_text(giacenza.CodArt)
        cod_mag = _normalize_acq_mag_code(giacenza.CodMag)

        if not cod_art or cod_mag not in allowed_mags:
            continue

        article_bucket = grouped.setdefault(
            cod_art,
            {mag: 0.0 for mag in allowed_mags},
        )

        article_bucket[cod_mag] += _safe_float(getattr(giacenza, "Giacenza", 0))

    return grouped


def _build_acquisti_materiale_mag0_map() -> dict[tuple[str, str], float]:
    grouped: dict[tuple[str, str], float] = {}

    for giacenza in AcqGiacenze.query.all():
        cod_art = _norm_text(giacenza.CodArt)
        variante_art = _normalize_variante_articolo_search(
            getattr(giacenza, "VarianteArt", "")
        )
        cod_mag = _normalize_acq_mag_code(giacenza.CodMag)

        if not cod_art or cod_mag != "0":
            continue

        key = _material_key(cod_art, variante_art)
        grouped[key] = grouped.get(key, 0.0) + _safe_float(
            getattr(giacenza, "Giacenza", 0)
        )

    return grouped


def _new_acq_material_row(cod_art: str, variante_art: str) -> dict:
    return {
        "CodArt": _norm_text(cod_art),
        "VarianteArt": _norm_text(variante_art),
        "IndiceModifica": "",
        "DesArt": "",
        "QtyMag0": None,
        "MaterialeDaConsumare": 0.0,
        "MaterialeImpegnato": 0.0,
        "MaterialeProdotto": 0.0,
        "RimanenzaMateriale": 0.0,
        "PianTempoApprovFisso": 0,
        "LottoRiordino": 0.0,
        "PuntoRiordino": 0.0,
        "Mag0Missing": True,
        "DistintaDettagli": [],
        "OrdineDettagli": [],
        "MagUM": "",
    }


def _acq_revision_rank(value) -> tuple[int, str]:
    rev = _normalize_indice_articolo_search(value).upper()
    if not rev:
        return (0, "")
    if len(rev) == 1 and "A" <= rev <= "Z":
        return (ord(rev) - ord("A") + 1, rev)
    return (0, rev)


def _latest_acq_lookup_by_codart_variante() -> dict[tuple[str, str], AcqArticoliLookup]:
    out = {}

    for item in AcqArticoliLookup.query.all():
        cod_art = _norm_text(item.CodArt)
        variante_art = _normalize_variante_art(getattr(item, "VarianteArt", ""))
        if not cod_art:
            continue

        key = _material_key(cod_art, variante_art)
        current = out.get(key)

        if current is None or _acq_revision_rank(
            item.IndiceModifica
        ) > _acq_revision_rank(current.IndiceModifica):
            out[key] = item

    return out


def _build_acquisti_materiale_rows() -> list[dict]:
    ordini = _base_odp_query().all()

    articoli_map = {
        _norm_text(row.CodArt): row
        for row in AcqArticoli.query.all()
        if _norm_text(row.CodArt)
    }
    lookup_by_codart_variante = _latest_acq_lookup_by_codart_variante()

    giacenze_materiale_totali = _build_acquisti_materiale_mag0_map()
    grouped: dict[tuple[str, str], dict] = {}

    for ordine in ordini:
        stato = _ordine_stato_effettivo(ordine)
        stato_norm = _norm_text(stato).lower()

        if not _is_open_order_state(stato):
            continue

        runtime = getattr(ordine, "runtime_row", None)

        try:
            qty_residua = _qty_da_lavorare_decimal(ordine, stato=runtime)
        except ValueError:
            continue

        if qty_residua <= 0:
            continue

        try:
            qty_totale = _parse_qty_decimal(getattr(ordine, "Quantita", "0"))
        except ValueError:
            qty_totale = qty_residua

        ordine_ref = _ordine_ref_label(ordine)
        cod_art_ordine = _norm_text(getattr(ordine, "CodArt", ""))
        variante_ordine = _norm_text(getattr(ordine, "VarianteArt", ""))
        des_art_ordine = _norm_text(getattr(ordine, "DesArt", ""))

        # materiale prodotto dall'ordine
        if cod_art_ordine:
            key = _material_key(cod_art_ordine, variante_ordine)
            row = grouped.setdefault(
                key, _new_acq_material_row(cod_art_ordine, variante_ordine)
            )

            if not row["DesArt"]:
                row["DesArt"] = des_art_ordine

            row["MaterialeProdotto"] += float(qty_residua)
            row["OrdineDettagli"].append(
                {
                    "Ordine": ordine_ref,
                    "Stato": _norm_text(ordine.StatoOrdine),
                    "Quantita": _decimal_to_text(qty_residua),
                    "Tipo": "ordine",
                    "DescrizioneOrdine": des_art_ordine,
                }
            )

        # materiali in distinta per tutte le fasi residue
        remaining_phases = _remaining_phase_codes_for_ordine(ordine)
        distinta = _parse_distinta_materiale(ordine)

        for comp in distinta:
            if not isinstance(comp, dict):
                continue

            comp_phase = _fase_to_int(comp.get("NumFase"))
            if comp_phase is None or str(comp_phase) not in remaining_phases:
                continue

            comp_cod_art = _norm_text(comp.get("CodArt", ""))
            comp_variante = _norm_text(comp.get("VarianteArt", ""))
            comp_des_art = _norm_text(comp.get("DesArt", ""))

            if not comp_cod_art:
                continue

            qty_comp_residua = _scaled_component_qty(
                comp.get("Quantita"),
                q_lavorata=qty_residua,
                q_tot=qty_totale,
            )

            if qty_comp_residua <= 0:
                continue

            key = _material_key(comp_cod_art, comp_variante)
            row = grouped.setdefault(
                key, _new_acq_material_row(comp_cod_art, comp_variante)
            )
            articolo_comp = articoli_map.get(comp_cod_art)

            if not row["MagUM"]:
                row["MagUM"] = _extract_comp_udm(comp, articolo=articolo_comp)

            if not row["DesArt"]:
                row["DesArt"] = comp_des_art

            if "pianificat" in stato_norm:
                row["MaterialeDaConsumare"] += float(qty_comp_residua)
            else:
                row["MaterialeImpegnato"] += float(qty_comp_residua)

            if not row["MagUM"]:
                row["MagUM"] = _first_not_blank_text(
                    getattr(articolo_comp, "MagUM", "") if articolo_comp else "",
                )

            row["DistintaDettagli"].append(
                {
                    "Ordine": ordine_ref,
                    "Stato": _norm_text(ordine.StatoOrdine),
                    "Fase": str(comp_phase),
                    "Quantita": _decimal_to_text(qty_comp_residua),
                    "Tipo": "distinta",
                    "DescrizioneOrdine": des_art_ordine,
                    "MagUM": _extract_comp_udm(comp, articolo=articolo_comp),
                }
            )

    rows_out = []
    for _, row in grouped.items():
        articolo = articoli_map.get(row["CodArt"])
        lookup = lookup_by_codart_variante.get(
            _material_key(row["CodArt"], row["VarianteArt"])
        ) or lookup_by_codart_variante.get((row["CodArt"], ""))

        giacenza_totale = giacenze_materiale_totali.get(
            _material_key(row["CodArt"], _normalize_variante_art(row["VarianteArt"]))
        )

        if not row["DesArt"]:
            row["DesArt"] = _norm_text(getattr(lookup, "DesArt", ""))

        if not row["MagUM"]:
            row["MagUM"] = _first_not_blank_text(
                getattr(lookup, "MagUM", "") if lookup else "",
                getattr(articolo, "MagUM", "") if articolo else "",
            )

        row["IndiceModifica"] = _normalize_indice_articolo_search(
            (getattr(lookup, "IndiceModifica", "") if lookup else "")
            or (getattr(articolo, "IndiceModifica", "") if articolo else "")
        )
        row["LottoRiordino"] = float(
            (getattr(lookup, "LottoRiordino", None) if lookup else None)
            or (getattr(articolo, "LottoRiordino", 0) if articolo else 0)
            or 0
        )
        row["PuntoRiordino"] = float(
            (getattr(lookup, "PuntoRiordino", None) if lookup else None)
            or (getattr(articolo, "PuntoRiordino", 0) if articolo else 0)
            or 0
        )
        row["PianTempoApprovFisso"] = int(
            (getattr(lookup, "PianTempoApprovFisso", None) if lookup else None)
            or (getattr(articolo, "PianTempoApprovFisso", 0) if articolo else 0)
            or 0
        )

        row["QtyMag0"] = float(giacenza_totale or 0)
        row["Mag0Missing"] = False

        row["RimanenzaMateriale"] = (
            float(row["QtyMag0"] or 0)
            + float(row["MaterialeProdotto"] or 0)
            - float(row["MaterialeDaConsumare"] or 0)
            - float(row["MaterialeImpegnato"] or 0)
        )

        row["QtyMag0Text"] = _decimal_to_text(Decimal(str(row["QtyMag0"])))
        row["MaterialeDaConsumareText"] = _decimal_to_text(
            Decimal(str(row["MaterialeDaConsumare"]))
        )
        row["MaterialeImpegnatoText"] = _decimal_to_text(
            Decimal(str(row["MaterialeImpegnato"]))
        )
        row["MaterialeProdottoText"] = _decimal_to_text(
            Decimal(str(row["MaterialeProdotto"]))
        )
        row["LottoRiordinoText"] = _decimal_to_text(Decimal(str(row["LottoRiordino"])))
        row["PuntoRiordinoText"] = _decimal_to_text(Decimal(str(row["PuntoRiordino"])))

        row["DistintaDettagli"].sort(
            key=lambda x: (
                (x.get("Ordine") or "").lower(),
                (x.get("Fase") or "").lower(),
            )
        )
        row["OrdineDettagli"].sort(key=lambda x: ((x.get("Ordine") or "").lower(),))

        row["RimanenzaMaterialeText"] = _decimal_to_text(
            Decimal(str(row["RimanenzaMateriale"]))
        )
        row["RimanenzaMaterialeCritica"] = row["RimanenzaMateriale"] <= 0
        row["RimanenzaMaterialeSottoScorta"] = not row[
            "RimanenzaMaterialeCritica"
        ] and row["RimanenzaMateriale"] <= float(row["PuntoRiordino"] or 0)

        row["ModalPayload"] = {
            "cod_art": row["CodArt"],
            "variante_art": row["VarianteArt"],
            "indice_modifica": row["IndiceModifica"],
            "des_art": row["DesArt"],
            "mag_um": row["MagUM"],
            "in_distinta": row["DistintaDettagli"],
            "in_ordine": row["OrdineDettagli"],
        }

        rows_out.append(row)


def _build_acquisti_giacenze_rows() -> list[dict]:
    magazzini = ["6", "0", "10", "11", "12", "13"]

    articoli_by_codart = {_norm_text(a.CodArt): a for a in AcqArticoli.query.all()}

    lookup_by_codart_variante = {}

    for item in AcqArticoliLookup.query.all():
        cod_art = _norm_text(item.CodArt)
        variante_art = _normalize_variante_art(getattr(item, "VarianteArt", ""))

        if not cod_art:
            continue

        lookup_by_codart_variante.setdefault(
            (cod_art, variante_art),
            item,
        )

    rows = {}

    for giac in AcqGiacenze.query.all():
        cod_art = _norm_text(giac.CodArt)
        variante_art = _normalize_variante_art(getattr(giac, "VarianteArt", ""))
        cod_mag = _normalize_acq_mag_code(giac.CodMag)

        if not cod_art:
            continue

        key = (cod_art, variante_art)

        lookup = lookup_by_codart_variante.get(key) or lookup_by_codart_variante.get(
            (cod_art, "")
        )

        articolo_base = articoli_by_codart.get(cod_art)

        if key not in rows:
            rows[key] = {
                "CodArt": cod_art,
                "VarianteArt": variante_art,
                "IndiceModifica": _normalize_indice_articolo_search(
                    getattr(lookup, "IndiceModifica", "")
                    or getattr(articolo_base, "IndiceModifica", "")
                ),
                "DesArt": (
                    _norm_text(getattr(lookup, "DesArt", ""))
                    or _norm_text(getattr(articolo_base, "DesArt", ""))
                ),
                "MagUM": (
                    _norm_text(getattr(lookup, "MagUM", ""))
                    or _norm_text(getattr(articolo_base, "MagUM", ""))
                ),
                "Mag_6": 0.0,
                "Mag_0": 0.0,
                "Mag_10": 0.0,
                "Mag_11": 0.0,
                "Mag_12": 0.0,
                "Mag_13": 0.0,
                "PuntoRiordino": float(
                    getattr(lookup, "PuntoRiordino", None)
                    or getattr(articolo_base, "PuntoRiordino", 0)
                    or 0
                ),
                "LottoRiordino": float(
                    getattr(lookup, "LottoRiordino", None)
                    or getattr(articolo_base, "LottoRiordino", 0)
                    or 0
                ),
                "PianTempoApprovFisso": int(
                    getattr(lookup, "PianTempoApprovFisso", None)
                    or getattr(articolo_base, "PianTempoApprovFisso", 0)
                    or 0
                ),
                "DataPrevistaApprovvigionamento": (
                    _norm_text(getattr(lookup, "DataPrevistaApprovvigionamento", ""))
                    or _norm_text(
                        getattr(articolo_base, "DataPrevistaApprovvigionamento", "")
                    )
                ),
            }

        mag_key = f"Mag_{cod_mag}"

        if mag_key in rows[key]:
            rows[key][mag_key] += _safe_float(getattr(giac, "Giacenza", 0))

    for row in rows.values():
        _apply_acquisti_giacenza_controls(row)

    return sorted(
        rows.values(),
        key=lambda r: (
            _norm_text(r.get("CodArt")).lower(),
            _norm_text(r.get("VarianteArt")).lower(),
        ),
    )


def _contains_insensitive(value, needle: str) -> bool:
    needle_norm = _norm_text(needle).lower()
    if not needle_norm:
        return True
    return needle_norm in _norm_text(value).lower()


def _filter_acquisti_giacenze_rows(
    rows: list[dict],
    *,
    codart: str = "",
    variante: str = "",
    desart: str = "",
    only_negative: bool = False,
    only_understock: bool = False,
    **legacy_kwargs,
) -> list[dict]:
    """
    Filtra le righe giacenza per export Excel.

    Accetta sia i nomi usati dalla query string/frontend:
    - codart
    - variante
    - desart

    sia eventuali nomi interni/vecchi:
    - cod_art
    - variante_art
    - des_art
    """

    codart_filter = _norm_text(codart or legacy_kwargs.get("cod_art", ""))
    variante_filter = _norm_text(
        variante
        or legacy_kwargs.get("variante_art", "")
        or legacy_kwargs.get("variante", "")
    )
    desart_filter = _norm_text(desart or legacy_kwargs.get("des_art", ""))

    out = []

    for row in rows:
        mag0 = float(row.get("Mag_0") or 0)
        punto_riordino = float(row.get("PuntoRiordino") or 0)

        is_negative_mag0 = mag0 < 0
        is_understock_mag0 = (not is_negative_mag0) and (mag0 < punto_riordino)

        if codart_filter and not _contains_insensitive(
            row.get("CodArt"), codart_filter
        ):
            continue

        if variante_filter and not _contains_insensitive(
            row.get("VarianteArt"), variante_filter
        ):
            continue

        if desart_filter and not _contains_insensitive(
            row.get("DesArt"), desart_filter
        ):
            continue

        if only_negative and not is_negative_mag0:
            continue

        if only_understock and not is_understock_mag0:
            continue

        out.append(row)

    return out


def _filter_acquisti_materiale_rows(
    rows: list[dict],
    *,
    codart: str = "",
    variante: str = "",
    desart: str = "",
    only_critical: bool = False,
    only_understock: bool = False,
) -> list[dict]:
    out = []

    for row in rows:
        if not _contains_insensitive(row.get("CodArt"), codart):
            continue
        if not _contains_insensitive(row.get("VarianteArt"), variante):
            continue
        if not _contains_insensitive(row.get("DesArt"), desart):
            continue
        if only_critical and not bool(row.get("RimanenzaMaterialeCritica")):
            continue
        if only_understock and not bool(row.get("RimanenzaMaterialeSottoScorta")):
            continue

        out.append(row)

    return out


def _build_acquisti_excel_workbook(section: str, rows: list[dict]) -> Workbook:
    wb = Workbook()
    ws = wb.active
    if section == "giacenza":
        show_decimal_control = any(
            row.get("HasMagazziniDecimaliNonValidi") for row in rows
        )
        headers = [
            "CodArt",
            "Variante",
            "Revisione",
            "Descrizione",
            "UdM",
            *(["Controllo"] if show_decimal_control else []),
            "6-Accettazione",
            "0-Principale",
            "10-Scarti",
            "11-Obsoleto",
            "12-DEMO",
            "13-Rottamare",
            "Punto riordino",
            "Lotto riordino",
            "Lead time",
            "Data prevista approvvigionamento",
        ]
        data_rows = [
            [
                row.get("CodArt", ""),
                row.get("VarianteArt", ""),
                row.get("IndiceModifica", ""),
                row.get("DesArt", ""),
                row.get("MagUM", ""),
                *(
                    [row.get("MagazziniDecimaliNonValidiText", "")]
                    if show_decimal_control
                    else []
                ),
                row.get("Mag_6", 0),
                row.get("Mag_0", 0),
                row.get("Mag_10", 0),
                row.get("Mag_11", 0),
                row.get("Mag_12", 0),
                row.get("Mag_13", 0),
                row.get("PuntoRiordino", 0),
                row.get("LottoRiordino", 0),
                row.get("PianTempoApprovFisso", 0),
                row.get("DataPrevistaApprovvigionamento", ""),
            ]
            for row in rows
        ]

    elif section == "materiale":
        ws.title = "Materiale"
        headers = [
            "Articolo",
            "Var.",
            "Rev.",
            "Descrizione",
            "UdM",
            "Giacenza",
            "Fabbisogno pianificato",
            "Fabbisogno impegnato",
            "Produzione prevista",
            "Rimanenza finale",
            "Scorta",
            "Lead time",
            "Lotto minimo",
            "Dettaglio",
        ]
        data_rows = [
            [
                row.get("CodArt", ""),
                row.get("VarianteArt", ""),
                row.get("IndiceModifica", ""),
                row.get("DesArt", ""),
                row.get("MagUM", ""),
                row.get("QtyMag0Text", ""),
                "Assente" if row.get("Mag0Missing") else row.get("QtyMag0Text", ""),
                row.get("MaterialeDaConsumareText", ""),
                row.get("MaterialeImpegnatoText", ""),
                row.get("MaterialeProdottoText", ""),
                row.get("RimanenzaMaterialeText", ""),
                row.get("PuntoRiordinoText", ""),
                row.get("PianTempoApprovFisso", 0),
                row.get("LottoRiordinoText", ""),
                "Ordini",
            ]
            for row in rows
        ]
    elif section == "scorte":
        ws.title = "Scorte"
        headers = [
            "Data lettura",
            "Codice",
            "Var.",
            "Rev.",
            "Descrizione",
            "Scorta",
            "Lead time",
            "Lotto minimo",
            "Stato",
            "Annullata",
            "Segnalato da",
            "Reparto",
            "Note",
        ]
        data_rows = [
            [
                row.get("DataLetturaText", ""),
                row.get("CodArt", ""),
                row.get("VarianteArt", ""),
                row.get("IndiceModifica", ""),
                row.get("DesArt", ""),
                row.get("PuntoRiordinoText", ""),
                row.get("PianTempoApprovFisso", ""),
                row.get("LottoRiordinoText", ""),
                row.get("Stato", ""),
                "Sì" if row.get("Annullata") else "No",
                row.get("SegnalatoDa", ""),
                row.get("RepartoSegnalatore", ""),
                row.get("Note", ""),
            ]
            for row in rows
        ]
    else:
        raise ValueError(f"Sezione export non valida: {section}")

    ws.append(headers)
    for row in data_rows:
        ws.append(row)

    # stile intestazione
    for cell in ws[1]:
        cell.font = Font(bold=True)

    ws.freeze_panes = "A2"

    # larghezza colonne semplice
    for column_cells in ws.columns:
        max_len = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            if len(value) > max_len:
                max_len = len(value)
        ws.column_dimensions[column_letter].width = min(max(max_len + 2, 10), 40)

    return wb


def _normalize_acq_mag_code(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    try:
        num = Decimal(raw.replace(",", "."))
        if num == num.to_integral_value():
            return str(int(num))
    except (InvalidOperation, ValueError):
        pass

    return raw


def _material_key(cod_art: str, variante_art: str) -> tuple[str, str]:
    return (
        _norm_text(cod_art),
        _normalize_variante_articolo_search(variante_art),
    )


def _first_not_blank_text(*values) -> str:
    for value in values:
        txt = _norm_text(value)
        if txt:
            return txt
    return ""


def _extract_comp_udm(comp: dict, articolo=None) -> str:
    return _first_not_blank_text(
        comp.get("TecniciUm"),
        comp.get("MagUM"),
        comp.get("Udm"),
        comp.get("UM"),
        getattr(articolo, "MagUM", ""),
    )


def _norm_variante_art(value) -> str:
    if value is None:
        return ""

    text = str(value).strip()

    if text.lower() in {"nan", "none", "null"}:
        return ""
    if text.upper() in {"X", "-"}:
        return ""

    return text


def _normalize_variante_art(value) -> str:
    variante = _norm_text(value)
    if not variante:
        return ""
    if variante == "-" or variante.upper() == "X":
        return ""
    return variante


def _ordine_stato_effettivo(ordine) -> str:
    runtime = getattr(ordine, "runtime_row", None)
    stato_runtime = _norm_text(getattr(runtime, "Stato_odp", ""))
    if stato_runtime:
        return stato_runtime
    return _norm_text(getattr(ordine, "StatoOrdine", ""))


def _is_open_order_state(stato: str) -> bool:
    s = _norm_text(stato).lower()
    if s == "chiusa":
        return False

    return "attiv" in s or "pianificat" in s or "sospes" in s or "apert"


SCORTA_SEGNALAZIONE_LIBERA_MIN_LEN = 3
SCORTA_SEGNALAZIONE_LIBERA_MAX_LEN = 300
SCORTA_SEGNALAZIONE_LIBERA_CODART_PREFIX = "__MANUALE__:"


def _is_scorta_segnalazione_libera(row) -> bool:
    cod_art = _norm_text(getattr(row, "CodArt", ""))
    return not cod_art or cod_art.startswith(SCORTA_SEGNALAZIONE_LIBERA_CODART_PREFIX)


def _parse_scorta_qrcode(raw_qrcode: str) -> tuple[str, str, str]:
    raw = _norm_text(raw_qrcode).replace("\n", "").replace("\r", "")

    if not raw:
        raise ValueError("QR code vuoto.")

    parts = raw.split("|")

    if len(parts) != 3:
        raise ValueError(
            "Formato QR non valido. Formato atteso: codice|variante|revisione."
        )

    cod_art = _norm_text(parts[0])
    variante_art = _normalize_variante_articolo_search(parts[1])
    indice_modifica = _normalize_indice_articolo_search(parts[2])

    if not cod_art:
        raise ValueError("Codice articolo mancante nel QR code.")

    return cod_art, variante_art, indice_modifica


def _find_scorta_lookup(cod_art: str, variante_art: str, indice_modifica: str):
    rows = AcqArticoliLookup.query.filter_by(CodArt=cod_art).all()

    if not rows:
        return None

    def row_variante(row) -> str:
        return _normalize_variante_articolo_search(getattr(row, "VarianteArt", ""))

    def row_revisione(row) -> str:
        return _normalize_indice_articolo_search(getattr(row, "IndiceModifica", ""))

    exact = [
        row
        for row in rows
        if row_variante(row) == variante_art and row_revisione(row) == indice_modifica
    ]

    if len(exact) == 1:
        return exact[0]

    if len(exact) > 1:
        raise ValueError(
            f"Articolo ambiguo nel lookup: {cod_art}|{variante_art}|{indice_modifica}."
        )

    compatible = [
        row
        for row in rows
        if (not variante_art or row_variante(row) == variante_art)
        and (not indice_modifica or row_revisione(row) == indice_modifica)
    ]

    if len(compatible) == 1:
        return compatible[0]

    if not variante_art and not indice_modifica and len(rows) == 1:
        return rows[0]

    if len(compatible) > 1:
        raise ValueError(
            f"Articolo ambiguo: {cod_art}. Specificare variante e/o revisione nel QR."
        )

    return None


def _scorta_operator_payload(user) -> tuple[str, str]:
    segnalato_da = (
        getattr(user, "username", None)
        or getattr(user, "name", None)
        or getattr(user, "email", None)
        or str(getattr(user, "id", "utente_sconosciuto"))
    )

    reparto = _norm_text(getattr(user, "RepartoPrinc", ""))

    return segnalato_da, reparto


def _create_scorta_from_qrcode(
    raw_qrcode: str, user, *, allow_free_text: bool = False
) -> tuple[AcqScortaSegnalata, bool]:
    raw_text = _norm_text(raw_qrcode)

    if not raw_text:
        raise ValueError("QR code o descrizione vuoti.")

    if "|" not in raw_text:
        if not allow_free_text:
            raise PermissionError(
                "Inserimento manuale non abilitato per questo operatore."
            )

        descrizione = " ".join(raw_text.replace("\n", " ").replace("\r", " ").split())

        if len(descrizione) < SCORTA_SEGNALAZIONE_LIBERA_MIN_LEN:
            raise ValueError("Descrizione materiale troppo breve.")

        if len(descrizione) > SCORTA_SEGNALAZIONE_LIBERA_MAX_LEN:
            raise ValueError("Descrizione materiale troppo lunga.")

        segnalato_da, reparto = _scorta_operator_payload(user)
        now_iso = _now_rome_dt().isoformat(timespec="seconds")

        row = AcqScortaSegnalata(
            DataLettura=now_iso,
            StatoChangedAt=now_iso,
            RawQrCode=descrizione,
            CodArt=f"{SCORTA_SEGNALAZIONE_LIBERA_CODART_PREFIX}{uuid4().hex}",
            VarianteArt="",
            IndiceModifica="",
            DesArt=descrizione,
            Stato="Aperta",
            Annullata=False,
            Note="",
            SegnalatoDa=segnalato_da,
            RepartoSegnalatore=reparto,
            LookupTrovato=False,
        )

        db.session.add(row)
        return row, True

    cod_art, variante_art, indice_modifica = _parse_scorta_qrcode(raw_qrcode)
    segnalato_da, reparto = _scorta_operator_payload(user)

    existing = (
        AcqScortaSegnalata.query.filter_by(
            CodArt=cod_art,
            VarianteArt=variante_art,
            IndiceModifica=indice_modifica,
            SegnalatoDa=segnalato_da,
            Stato="Aperta",
        )
        .filter(AcqScortaSegnalata.Annullata.is_(False))
        .first()
    )

    if existing:
        return existing, False

    lookup = _find_scorta_lookup(cod_art, variante_art, indice_modifica)
    lookup_trovato = lookup is not None

    now_iso = _now_rome_dt().isoformat(timespec="seconds")

    row = AcqScortaSegnalata(
        DataLettura=now_iso,
        StatoChangedAt=now_iso,
        RawQrCode=_norm_text(raw_qrcode),
        CodArt=cod_art,
        VarianteArt=variante_art,
        IndiceModifica=indice_modifica,
        DesArt=_norm_text(getattr(lookup, "DesArt", "")),
        PuntoRiordino=getattr(lookup, "PuntoRiordino", None),
        LottoRiordino=getattr(lookup, "LottoRiordino", None),
        PianTempoApprovFisso=getattr(lookup, "PianTempoApprovFisso", None),
        Stato="Aperta",
        Annullata=False,
        Note="",
        SegnalatoDa=segnalato_da,
        RepartoSegnalatore=reparto,
        LookupTrovato=lookup_trovato,
    )

    db.session.add(row)
    return row, True


def _format_datetime_it(value: str) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        return raw


def _num_text(value) -> str:
    if value is None:
        return ""
    return _decimal_to_text(Decimal(str(value)))


def _scorta_to_row(row: AcqScortaSegnalata) -> dict:
    is_segnalazione_libera = _is_scorta_segnalazione_libera(row)
    return {
        "Id": row.id,
        "DataLettura": row.DataLettura,
        "DataLetturaText": _format_datetime_it(row.DataLettura),
        "RawQrCode": row.RawQrCode,
        "CodArt": "" if is_segnalazione_libera else row.CodArt,
        "VarianteArt": row.VarianteArt,
        "IndiceModifica": row.IndiceModifica,
        "DesArt": row.DesArt,
        "PuntoRiordino": row.PuntoRiordino,
        "PuntoRiordinoText": _num_text(row.PuntoRiordino),
        "LottoRiordino": row.LottoRiordino,
        "LottoRiordinoText": _num_text(row.LottoRiordino),
        "PianTempoApprovFisso": row.PianTempoApprovFisso,
        "Stato": row.Stato,
        "Annullata": bool(row.Annullata),
        "Note": row.Note or "",
        "SegnalatoDa": row.SegnalatoDa,
        "RepartoSegnalatore": row.RepartoSegnalatore,
        "LookupTrovato": bool(row.LookupTrovato),
        "ScortaApertaOltre3Giorni": _is_scorta_aperta_oltre_3_giorni(row),
        "StatoChangedAt": row.StatoChangedAt,
        "StatoChangedAtText": _format_datetime_it(row.StatoChangedAt),
    }


def _build_acquisti_scorte_rows() -> list[dict]:
    rows = AcqScortaSegnalata.query.order_by(
        AcqScortaSegnalata.DataLettura.desc(), AcqScortaSegnalata.id.desc()
    ).all()

    return [_scorta_to_row(row) for row in rows]


def _filter_acquisti_scorte_rows(
    rows: list[dict],
    *,
    codart: str = "",
    variante: str = "",
    desart: str = "",
    stato: str = "",
    segnalato_da: str = "",
    include_annullate: bool = False,
) -> list[dict]:
    out = []

    stato_filter = _norm_text(stato).lower()

    for row in rows:
        is_annullata = bool(row.get("Annullata"))

        if stato_filter == "annullata":
            if not is_annullata:
                continue

        else:
            if is_annullata and not include_annullate:
                continue

            if stato_filter and _norm_text(row.get("Stato")).lower() != stato_filter:
                continue

        if not _contains_insensitive(row.get("CodArt"), codart):
            continue

        if not _contains_insensitive(row.get("VarianteArt"), variante):
            continue

        if not _contains_insensitive(row.get("DesArt"), desart):
            continue

        if not _contains_insensitive(row.get("SegnalatoDa"), segnalato_da):
            continue

        out.append(row)

    return out


def _is_scorta_aperta_oltre_3_giorni(row: AcqScortaSegnalata) -> bool:
    if _norm_text(row.Stato).lower() != "aperta":
        return False

    if bool(row.Annullata):
        return False

    raw_date = _norm_text(row.StatoChangedAt) or _norm_text(row.DataLettura)
    if not raw_date:
        return False

    try:
        data_riferimento = datetime.fromisoformat(raw_date)
    except ValueError:
        return False

    now = _now_rome_dt()

    if data_riferimento.tzinfo is None:
        data_riferimento = data_riferimento.replace(tzinfo=now.tzinfo)

    return now - data_riferimento >= timedelta(days=3)


def _parse_iso_datetime(value):
    raw = _norm_text(value)
    if not raw:
        return None

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    now = _now_rome_dt()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=now.tzinfo)

    return dt


def _delete_scorte_chiuse_oltre_7_giorni() -> int:
    now = _now_rome_dt()
    limit = now - timedelta(days=7)

    rows = AcqScortaSegnalata.query.filter(
        (AcqScortaSegnalata.Annullata.is_(True))
        | (AcqScortaSegnalata.Stato == "Ordinata")
    ).all()

    deleted = 0

    for row in rows:
        changed_at = _parse_iso_datetime(row.StatoChangedAt)

        if changed_at is None:
            continue

        if changed_at <= limit:
            db.session.delete(row)
            deleted += 1

    return deleted
