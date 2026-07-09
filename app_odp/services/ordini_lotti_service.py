from decimal import Decimal
from app_odp.models import GiacenzaLotti
from app_odp.services.order_helpers import (
    _component_udm,
    _decimal_to_text_for_udm,
    _norm_text,
    _parse_distinta_materiale,
    _parse_qty_decimal,
)


def _fase_attiva_int(ordine) -> int | None:
    try:
        return int(float(_norm_text(ordine.FaseAttiva)))
    except (ValueError, TypeError):
        return None


def _componenti_lotto_per_ordine(
    ordine,
    include_senza_lotti: bool = False,
    ignore_parent_gestione_lotto: bool = False,
    **_unused,
) -> list[dict]:
    if not ignore_parent_gestione_lotto:
        if _norm_text(ordine.GestioneLotto).lower() != "si":
            return []

    distinta = _parse_distinta_materiale(ordine)
    fase_attiva = _fase_attiva_int(ordine)

    componenti_lotto = []
    codici_visti = set()

    for comp in distinta:
        if not isinstance(comp, dict):
            continue

        if fase_attiva is not None:
            try:
                comp_fase = int(float(comp.get("NumFase", 0)))
            except (ValueError, TypeError):
                comp_fase = 0
            if comp_fase != fase_attiva:
                continue

        comp_gl = _norm_text(comp.get("GestioneLotto", "")).lower()
        if comp_gl != "si":
            continue

        cod_art = _norm_text(comp.get("CodArt", ""))
        variante_art = _norm_text(comp.get("VarianteArt", ""))

        chiave_componente = (cod_art, variante_art)

        if not cod_art or chiave_componente in codici_visti:
            continue

        codici_visti.add(chiave_componente)

        udm = _component_udm(comp)
        lotti_db = GiacenzaLotti.query.filter_by(CodArt=cod_art).all()
        lotti_list = []
        for lotto in lotti_db:
            try:
                giacenza_val = _parse_qty_decimal(lotto.Giacenza)
            except ValueError:
                giacenza_val = Decimal("0")

            if giacenza_val <= 0:
                continue

            lotti_list.append(
                {
                    "RifLottoAlfa": lotto.RifLottoAlfa,
                    "Giacenza": _decimal_to_text_for_udm(giacenza_val, udm),
                    "CodMag": lotto.CodMag,
                }
            )

        if include_senza_lotti or lotti_list:
            componenti_lotto.append(
                {
                    "CodArt": cod_art,
                    "DesArt": _norm_text(comp.get("DesArt", "")),
                    "Quantita": comp.get("Quantita", 0),
                    "NumFase": comp.get("NumFase", ""),
                    "GestioneLotto": "si",
                    "TecniciUm": udm,
                    "UdM": udm,
                    "VarianteArt": _norm_text(comp.get("VarianteArt", "")),
                    "lotti": lotti_list,
                }
            )

    return componenti_lotto
