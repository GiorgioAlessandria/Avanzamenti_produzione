from __future__ import annotations
from typing import Literal

from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from app_odp.ordine_ref import format_ordine_ref_export


def _component_matches_payload_phase(component: dict, payload: dict) -> bool:
    """
    Esporta solo componenti assegnati alla fase corrente.

    Se il componente non ha NumFase in un ordine multifase, non va esportato
    come componente di fase.
    """
    if not _is_multiphase_payload(payload):
        return True

    fase_payload = _to_int(payload.get("fase"))
    fase_component = _to_int(component.get("NumFase"))

    if fase_payload is None:
        return False

    if fase_component is None:
        return False

    return fase_component == fase_payload


def _text(value) -> str:
    return str(value or "").strip()


def _to_decimal(value) -> Decimal:
    raw = _text(value).replace(",", ".")
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return Decimal("0")


def _load_distinta_base(value) -> list[dict]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]

    raw = _text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return []

    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]

    return []


def row_writer(
    tipo_record: Literal["TES", "RIG"],
    tipo_documento=710,
    registrazione_data="",
    codice_documento=None,
    operazione_avanzamento="",
    riferimento_ordine="",
    codice_articolo="",
    variante="",
    quantita_principale=None,
    quantita_scarti_prima=None,
    quantita_scarti_seconda=None,
    riga_saldata=None,
    riferimento_lotto_padre=None,
    riferimento_lotto_pf=None,
    magazzino_principale=None,
    codice_risorsa="",
    causale_prestazione="",
    ore_lavorate=None,
):
    tipo_documento = tipo_documento if tipo_documento is not None else ""
    codice_documento = codice_documento if codice_documento is not None else ""
    quantita_principale = quantita_principale if quantita_principale is not None else ""
    quantita_scarti_prima = (
        quantita_scarti_prima if quantita_scarti_prima is not None else ""
    )
    quantita_scarti_seconda = (
        quantita_scarti_seconda if quantita_scarti_seconda is not None else ""
    )
    if tipo_record == "RIG":
        riga_saldata = riga_saldata if riga_saldata is not None else "0"
    else:
        riga_saldata = ""
    riferimento_lotto_pf = (
        riferimento_lotto_pf if riferimento_lotto_pf is not None else ""
    )
    riferimento_lotto_padre = (
        riferimento_lotto_padre if riferimento_lotto_padre is not None else ""
    )
    magazzino_principale = (
        magazzino_principale if magazzino_principale is not None else ""
    )
    ore_lavorate = ore_lavorate if ore_lavorate is not None else ""

    return (
        f"{tipo_record};{tipo_documento};{registrazione_data};{codice_documento};"
        f"{operazione_avanzamento};{riferimento_ordine};{codice_articolo};{variante};"
        f"{quantita_principale};{quantita_scarti_prima};{quantita_scarti_seconda};"
        f"{riga_saldata};{riferimento_lotto_padre};{riferimento_lotto_pf};{magazzino_principale};"
        f"{codice_risorsa};{causale_prestazione};{ore_lavorate}"
    )


def _bool_from_payload(value) -> bool | None:
    if isinstance(value, bool):
        return value

    raw = _text(value).lower()

    if raw in {"1", "true", "si", "sì", "yes", "on"}:
        return True

    if raw in {"0", "false", "no", "off"}:
        return False

    return None


def _to_int(value) -> int | None:
    raw = _text(value)

    if not raw:
        return None

    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _jsonish_list(value) -> list[str]:
    if isinstance(value, list):
        return [_text(x) for x in value if _text(x)]

    raw = _text(value)

    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return [raw]

    if isinstance(parsed, list):
        return [_text(x) for x in parsed if _text(x)]

    return [_text(parsed)] if _text(parsed) else []


def _phase_sequence_from_payload(payload: dict) -> list[str]:
    for key in ("phase_sequence", "num_fase", "num_fasi", "fasi"):
        values = _jsonish_list(payload.get(key))
        if values:
            return values

    totale_fasi = _to_int(payload.get("totale_fasi"))

    if totale_fasi and totale_fasi > 0:
        return [str(i) for i in range(1, totale_fasi + 1)]

    return []


def _should_emit_product_line(payload: dict) -> bool:
    explicit_emit = _bool_from_payload(payload.get("emit_product_line"))

    if explicit_emit is not None:
        return explicit_emit

    is_last_phase = _bool_from_payload(payload.get("is_last_phase"))

    if is_last_phase is not None:
        return is_last_phase

    fase_corrente = _text(payload.get("fase"))
    fasi = _phase_sequence_from_payload(payload)

    if not fasi:
        return True

    if len(fasi) == 1:
        return True

    return fase_corrente == _text(fasi[-1])


def _is_multiphase_payload(payload: dict) -> bool:
    return len(_phase_sequence_from_payload(payload)) > 1


def _normal_phase_suffix(value) -> str:
    value_int = _to_int(value)

    if value_int is not None:
        return str(value_int)

    return _text(value)


def _ref_with_suffix(base_ref: str, suffix_value) -> str:
    suffix = _normal_phase_suffix(suffix_value)

    if not suffix:
        return base_ref

    return f"{base_ref}.{suffix},00"


def _phase_ref_for_export(base_ref: str, payload: dict) -> str:
    if not _is_multiphase_payload(payload):
        return base_ref

    return _ref_with_suffix(base_ref, payload.get("fase"))


def txt_generator(
    export_rows: list[dict],
    *,
    include_time_line: bool = True,
) -> list[str]:
    if not export_rows:
        raise ValueError("Nessun record pending da esportare")

    payload = export_rows[0]["payload"]

    registrazione_data = _text(payload.get("registrazione_data"))

    if not registrazione_data:
        created_at_raw = _text(payload.get("created_at"))
        if not created_at_raw:
            raise ValueError("Payload export privo di data registrazione.")
        registrazione_data = datetime.fromisoformat(created_at_raw).strftime("%d/%m/%Y")
    id_documento = payload["id_documento"]
    id_riga = payload["id_riga"]
    rif_registraz = payload["rif_registraz"]
    fase = payload["fase"]
    num_progr_riga = payload.get("num_progr_riga")

    codice_articolo = payload["cod_art"]
    variante_articolo = payload.get("variante", "")
    lotto_articolo = payload["lotto_prodotto"]
    magazzino = payload["magazzino"]
    risorsa = payload["risorsa"]
    salda_riga = payload["salda_riga"]

    q_ok = _to_decimal(payload["quantita_ok"])
    q_ko = _to_decimal(payload["quantita_ko"])
    tempo_funzionamento = _to_decimal(payload["tempo_funzionamento"])

    distinta_base_raw = _load_distinta_base(payload.get("distinta_base"))

    distinta_base = [
        component
        for component in distinta_base_raw
        if _component_matches_payload_phase(component, payload)
    ]

    lotti_components = payload.get("lotti") or []

    riferimento_ordine_base = format_ordine_ref_export(
        rif_registraz,
        num_progr_riga=num_progr_riga,
        id_riga=id_riga,
        fase="",
    )

    riferimento_ordine_fase = _phase_ref_for_export(
        riferimento_ordine_base,
        payload,
    )

    lines = []

    head_line = row_writer(
        tipo_record="TES",
        tipo_documento=710,
        registrazione_data=registrazione_data,
        codice_documento=id_documento,
    )
    lines.append(head_line)

    emit_product_line = _should_emit_product_line(payload)

    if emit_product_line:
        product_line = row_writer(
            tipo_record="RIG",
            tipo_documento=710,
            registrazione_data=registrazione_data,
            codice_documento=id_documento,
            operazione_avanzamento="701",
            riferimento_ordine=riferimento_ordine_base,
            codice_articolo=codice_articolo,
            variante=variante_articolo,
            quantita_principale=str(q_ok),
            quantita_scarti_prima=str(q_ko),
            quantita_scarti_seconda=0,
            riga_saldata=salda_riga,
            riferimento_lotto_padre=lotto_articolo,
            riferimento_lotto_pf=lotto_articolo,
            magazzino_principale=magazzino,
            codice_risorsa=risorsa,
            causale_prestazione="",
            ore_lavorate=str(tempo_funzionamento),
        )
        lines.append(product_line)

    if include_time_line:
        product_time_line = row_writer(
            tipo_record="RIG",
            tipo_documento=710,
            registrazione_data=registrazione_data,
            codice_documento=id_documento,
            operazione_avanzamento="709",
            riferimento_ordine=riferimento_ordine_fase,
            codice_articolo=codice_articolo,
            variante=variante_articolo,
            quantita_principale=0,
            quantita_scarti_prima=str(q_ko),
            quantita_scarti_seconda=0,
            riga_saldata=salda_riga,
            riferimento_lotto_padre=lotto_articolo,
            riferimento_lotto_pf=lotto_articolo,
            magazzino_principale=magazzino,
            codice_risorsa=risorsa,
            causale_prestazione="",
            ore_lavorate=str(tempo_funzionamento),
        )
        lines.append(product_time_line)

    for component_row_index, component in enumerate(distinta_base, start=1):
        if not isinstance(component, dict):
            continue

        cod_art_component = _text(component.get("CodArt"))
        variante_component = _text(component.get("VarianteArt"))

        riferimento_ordine_component = _ref_with_suffix(
            riferimento_ordine_base, component_row_index
        )

        righe_lotto_component = [
            riga
            for riga in lotti_components
            if _text(riga.get("CodArt")) == cod_art_component
            and _text(riga.get("VarianteArt")) == variante_component
        ]

        if righe_lotto_component:
            for riga_lotto_component in righe_lotto_component:
                lotto_component = _text(riga_lotto_component.get("RifLottoAlfa"))
                quantita_lotto = _text(riga_lotto_component.get("Quantita"))
                magazzino_lotto = _text(riga_lotto_component.get("CodMag")) or magazzino
                component_line = row_writer(
                    tipo_record="RIG",
                    tipo_documento=710,
                    registrazione_data=registrazione_data,
                    codice_documento=id_documento,
                    operazione_avanzamento="703",
                    riferimento_ordine=riferimento_ordine_component,
                    codice_articolo=component.get("CodArt", ""),
                    variante=component.get("VarianteArt", ""),
                    quantita_principale=quantita_lotto,
                    riga_saldata=salda_riga,
                    riferimento_lotto_padre=lotto_component,
                    riferimento_lotto_pf=lotto_articolo,
                    magazzino_principale=magazzino_lotto,
                    codice_risorsa=risorsa,
                    causale_prestazione="",
                    ore_lavorate=str(tempo_funzionamento),
                )
                lines.append(component_line)
        else:
            component_line = row_writer(
                tipo_record="RIG",
                tipo_documento=710,
                registrazione_data=registrazione_data,
                codice_documento=id_documento,
                operazione_avanzamento="703",
                riferimento_ordine=riferimento_ordine_component,
                codice_articolo=component.get("CodArt", ""),
                variante=component.get("VarianteArt", ""),
                quantita_principale=component.get("Quantita", ""),
                riga_saldata=salda_riga,
                riferimento_lotto_padre="",
                riferimento_lotto_pf=lotto_articolo,
                magazzino_principale=magazzino,
                codice_risorsa=risorsa,
                causale_prestazione="",
                ore_lavorate=str(tempo_funzionamento),
            )
            lines.append(component_line)

    return lines
