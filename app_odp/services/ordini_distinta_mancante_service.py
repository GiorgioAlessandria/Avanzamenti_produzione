import json

from app_odp.models import OdpDistintaMancante, db
from app_odp.services.order_helpers import (
    _component_udm,
    _fase_to_int,
    _norm_text,
    _parse_distinta_materiale,
)


def component_key(component: dict) -> tuple[str, str]:
    return (
        _norm_text(component.get("CodArt")),
        _norm_text(component.get("VarianteArt")),
    )


def _normalize_component(component: dict, progressivo: int) -> dict:
    return {
        **component,
        "CodArt": _norm_text(component.get("CodArt")),
        "VarianteArt": _norm_text(component.get("VarianteArt")),
        "DesArt": _norm_text(component.get("DesArt")),
        "Quantita": component.get("Quantita", 0),
        "GestioneLotto": _norm_text(component.get("GestioneLotto")),
        "TecniciUm": _component_udm(component),
        "ProgressivoRiga": _norm_text(
            component.get("IdRigacomponente")
            or component.get("ProgressivoRiga")
            or progressivo
        ),
    }


def _current_phase_components(ordine, fase: str) -> list[dict]:
    fase_int = _fase_to_int(fase)
    result = []
    seen = set()

    for progressivo, component in enumerate(
        _parse_distinta_materiale(ordine), start=1
    ):
        if not isinstance(component, dict):
            continue
        if fase_int is not None and _fase_to_int(component.get("NumFase")) != fase_int:
            continue

        normalized = _normalize_component(component, progressivo)
        key = component_key(normalized)
        if not key[0] or key in seen:
            continue
        seen.add(key)
        result.append(normalized)

    return result


def distinta_pendente_per_ordine(ordine, fase: str) -> list[dict]:
    """Confronta la distinta ERP corrente con l'eventuale residuo salvato."""
    current = _current_phase_components(ordine, fase)
    current_by_key = {component_key(row): row for row in current}
    saved = (
        OdpDistintaMancante.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            Fase=_norm_text(fase),
        )
        .order_by(OdpDistintaMancante.ProgressivoRiga, OdpDistintaMancante.CodArt)
        .all()
    )
    if not saved:
        return current

    result = []
    for row in saved:
        stored = {
            "CodArt": row.CodArt,
            "VarianteArt": row.VarianteArt,
            "DesArt": row.DesArt,
            "Quantita": row.Quantita,
            "GestioneLotto": row.GestioneLotto,
            "TecniciUm": row.TecniciUm,
            "ProgressivoRiga": row.ProgressivoRiga,
            "NumFase": row.Fase,
        }
        # La distinta ERP corrente è autorevole anche per GestioneLotto.
        result.append(current_by_key.get(component_key(stored), stored))
    return result


def partition_distinta_step(
    pending: list[dict], missing_payload: list[dict]
) -> tuple[list[dict], list[dict]]:
    if not isinstance(missing_payload, list):
        raise ValueError("La distinta mancante deve essere una lista.")

    pending_by_key = {component_key(row): row for row in pending}
    missing_keys = set()
    for row in missing_payload:
        if not isinstance(row, dict):
            raise ValueError("Componente mancante non valido.")
        key = component_key(row)
        if key not in pending_by_key:
            raise ValueError(f"Il componente mancante {key[0]} non è nella distinta residua.")
        missing_keys.add(key)

    missing = [row for row in pending if component_key(row) in missing_keys]
    mounted = [row for row in pending if component_key(row) not in missing_keys]
    if missing and not mounted:
        raise ValueError(
            "Indicare almeno un componente montato prima della chiusura parziale."
        )
    return mounted, missing


def save_missing_components(ordine, fase: str, missing: list[dict]) -> None:
    OdpDistintaMancante.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        Fase=_norm_text(fase),
    ).delete(synchronize_session=False)

    for row in missing:
        db.session.add(
            OdpDistintaMancante(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                Fase=_norm_text(fase),
                CodArt=component_key(row)[0],
                VarianteArt=component_key(row)[1],
                DesArt=_norm_text(row.get("DesArt")),
                Quantita=str(row.get("Quantita", 0)),
                GestioneLotto=_norm_text(row.get("GestioneLotto")),
                TecniciUm=_component_udm(row),
                ProgressivoRiga=_norm_text(row.get("ProgressivoRiga")),
            )
        )


def filter_export_distinta(distinta_json: str, mounted: list[dict]) -> str:
    mounted_keys = {component_key(row) for row in mounted}
    distinta = json.loads(distinta_json or "[]")
    return json.dumps(
        [row for row in distinta if component_key(row) in mounted_keys],
        ensure_ascii=False,
    )
