from __future__ import annotations
from typing import Literal

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json


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


def _codice_articolo_export(cod_art, variante_art="") -> str:
    cod_art = _text(cod_art)
    variante_art = _text(variante_art)

    if not variante_art:
        return cod_art

    # Se il gestionale vuole un altro formato, basta cambiare questa riga.
    return f"{cod_art}|{variante_art}"


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
    riferimento_lotto=None,
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
    riferimento_lotto = riferimento_lotto if riferimento_lotto is not None else ""
    magazzino_principale = (
        magazzino_principale if magazzino_principale is not None else ""
    )
    ore_lavorate = ore_lavorate if ore_lavorate is not None else ""

    return (
        f"{tipo_record};{tipo_documento};{registrazione_data};{codice_documento};"
        f"{operazione_avanzamento};{riferimento_ordine};{codice_articolo};{variante};"
        f"{quantita_principale};{quantita_scarti_prima};{quantita_scarti_seconda};"
        f"{riga_saldata};{riferimento_lotto};{magazzino_principale};"
        f"{codice_risorsa};{causale_prestazione};{ore_lavorate}"
    )


def txt_generator(export_rows: list[dict]) -> list[str]:
    if not export_rows:
        raise ValueError("Nessun record pending da esportare")

    payload = export_rows[0]["payload"]

    created_at = datetime.fromisoformat(payload["created_at"]).strftime("%d/%m/%Y")
    id_documento = payload["id_documento"]
    id_riga = payload["id_riga"]
    rif_registraz = payload["rif_registraz"]
    fase = payload["fase"]

    codice_articolo = payload["cod_art"]
    variante_articolo = payload.get("variante", "")
    lotto_articolo = payload["lotto_prodotto"]
    magazzino = payload["magazzino"]
    risorsa = payload["risorsa"]
    salda_riga = payload["salda_riga"]

    q_ok = _to_decimal(payload["quantita_ok"])
    q_ko = _to_decimal(payload["quantita_ko"])
    q_lavorata = q_ok + q_ko
    tempo_funzionamento = _to_decimal(payload["tempo_funzionamento"])

    distinta_base = _load_distinta_base(payload.get("distinta_base"))
    lotti_components = payload.get("lotti") or []

    riferimento_ordine = f"{rif_registraz}.{id_riga},00"
    riferimento_ordine_time = f"{rif_registraz}.{id_riga},00.{fase},00"

    lines = []

    head_line = row_writer(
        tipo_record="TES",
        tipo_documento=710,
        registrazione_data=created_at,
        codice_documento=id_documento,
    )
    lines.append(head_line)

    ore_per_pezzo = Decimal("0")
    print(tempo_funzionamento, q_lavorata)
    if q_lavorata > 0:
        ore_per_pezzo = (tempo_funzionamento / q_lavorata).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

    product_line = row_writer(
        tipo_record="RIG",
        tipo_documento=710,
        registrazione_data=created_at,
        codice_documento=id_documento,
        operazione_avanzamento="701",
        riferimento_ordine=riferimento_ordine,
        codice_articolo=codice_articolo,
        variante=variante_articolo,
        quantita_principale=str(q_ok),
        quantita_scarti_prima=str(q_ko),
        quantita_scarti_seconda=0,
        riga_saldata=salda_riga,
        riferimento_lotto=lotto_articolo,
        magazzino_principale=magazzino,
        codice_risorsa=risorsa,
        causale_prestazione="",
        ore_lavorate=str(ore_per_pezzo),
    )
    lines.append(product_line)

    product_time_line = row_writer(
        tipo_record="RIG",
        tipo_documento=710,
        registrazione_data=created_at,
        codice_documento=id_documento,
        operazione_avanzamento="709",
        riferimento_ordine=riferimento_ordine_time,
        codice_articolo=codice_articolo,
        variante=variante_articolo,
        quantita_principale=str(q_ok),
        quantita_scarti_prima=str(q_ko),
        quantita_scarti_seconda=0,
        riga_saldata=salda_riga,
        riferimento_lotto=lotto_articolo,
        magazzino_principale=magazzino,
        codice_risorsa=risorsa,
        causale_prestazione="",
        ore_lavorate=str(ore_per_pezzo),
    )
    lines.append(product_time_line)

    for component in distinta_base:
        if not isinstance(component, dict):
            continue

        cod_art_component = _text(component.get("CodArt"))
        variante_component = _text(component.get("VarianteArt"))

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

                component_line = row_writer(
                    tipo_record="RIG",
                    tipo_documento=710,
                    registrazione_data=created_at,
                    codice_documento=id_documento,
                    operazione_avanzamento="703",
                    riferimento_ordine=riferimento_ordine,
                    codice_articolo=component.get("CodArt", ""),
                    variante=component.get("VarianteArt", ""),
                    quantita_principale=quantita_lotto,
                    riga_saldata=salda_riga,
                    riferimento_lotto=lotto_component,
                    magazzino_principale=magazzino,
                    codice_risorsa=risorsa,
                    causale_prestazione="",
                    ore_lavorate=str(ore_per_pezzo),
                )
                lines.append(component_line)
        else:
            component_line = row_writer(
                tipo_record="RIG",
                tipo_documento=710,
                registrazione_data=created_at,
                codice_documento=id_documento,
                operazione_avanzamento="703",
                riferimento_ordine=riferimento_ordine,
                codice_articolo=component.get("CodArt", ""),
                variante=component.get("VarianteArt", ""),
                quantita_principale=component.get("Quantita", ""),
                riga_saldata=salda_riga,
                riferimento_lotto=None,
                magazzino_principale=magazzino,
                codice_risorsa=risorsa,
                causale_prestazione="",
                ore_lavorate=str(ore_per_pezzo),
            )
            lines.append(component_line)

    return lines
