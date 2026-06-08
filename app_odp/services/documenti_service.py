# app_odp/services/documenti_service.py

from decimal import Decimal
from pathlib import Path
from threading import Lock
from time import monotonic

from flask import current_app, url_for
from sqlalchemy import func

from app_odp.models import AcqArticoliLookup
from app_odp.operator_session import active_token

from app_odp.routes import (
    _base_odp_query,
    _decimal_to_text,
    _norm_text,
    _ordine_ref_label,
    _parse_distinta_materiale,
    _parse_qty_decimal,
    _normalize_indice_articolo_search,
    _normalize_variante_articolo_search,
)

MONTAGGIO_PDF_INDEX_TTL_SECONDS = 60
_montaggio_pdf_index_lock = Lock()
_montaggio_pdf_index_cache = {
    "directory": "",
    "expires_at": 0.0,
    "files": {},
}
MATERIALE_IMG_INDEX_TTL_SECONDS = 60
_materiale_img_index_lock = Lock()
_materiale_img_index_cache = {
    "directory": "",
    "expires_at": 0.0,
    "files": {},
}
_metodo_pdf_index_lock = Lock()
_metodo_pdf_index_cache = {}


def _build_montaggio_pdf_key(cod_art: str, indice_modifica: str = "") -> str:
    cod_art = _norm_text(cod_art)
    if not cod_art:
        return ""
    rev = _normalize_indice_modifica_for_pdf(indice_modifica)
    return f"{cod_art}.{rev}" if rev else cod_art


def _get_montaggio_pdf_dir() -> Path | None:
    raw = _norm_text(current_app.config.get("MONTAGGIO_PDF_DIR"))
    if not raw:
        return None

    pdf_dir = Path(raw).expanduser()
    try:
        pdf_dir = pdf_dir.resolve()
    except Exception:
        pass

    if not pdf_dir.exists() or not pdf_dir.is_dir():
        return None

    return pdf_dir


def _scan_montaggio_pdf_directory(pdf_dir: Path) -> dict[str, Path]:
    files = {}

    try:
        for entry in pdf_dir.iterdir():
            if not entry.is_file():
                continue
            if entry.suffix.lower() != ".pdf":
                continue

            stem = entry.stem.strip()
            if not stem:
                continue

            try:
                resolved = entry.resolve()
            except Exception:
                resolved = entry

            files.setdefault(stem.lower(), resolved)
    except Exception:
        return {}

    return files


def _get_montaggio_pdf_index(
    *, force_refresh: bool = False
) -> tuple[Path | None, dict[str, Path]]:
    pdf_dir = _get_montaggio_pdf_dir()
    if pdf_dir is None:
        return None, {}

    directory_key = str(pdf_dir).lower()
    now_monotonic = monotonic()

    with _montaggio_pdf_index_lock:
        cache_valid = (
            not force_refresh
            and _montaggio_pdf_index_cache["directory"] == directory_key
            and float(_montaggio_pdf_index_cache["expires_at"]) > now_monotonic
        )

        if cache_valid:
            return pdf_dir, dict(_montaggio_pdf_index_cache["files"])

        files = _scan_montaggio_pdf_directory(pdf_dir)
        _montaggio_pdf_index_cache["directory"] = directory_key
        _montaggio_pdf_index_cache["expires_at"] = (
            now_monotonic + MONTAGGIO_PDF_INDEX_TTL_SECONDS
        )
        _montaggio_pdf_index_cache["files"] = files

        return pdf_dir, dict(files)


def _find_montaggio_pdf_path(
    cod_art: str,
    indice_modifica: str = "",
    *,
    force_refresh: bool = False,
) -> Path | None:
    lookup_key = _build_montaggio_pdf_key(cod_art, indice_modifica)
    if not lookup_key:
        return None

    pdf_dir, pdf_index = _get_montaggio_pdf_index(force_refresh=force_refresh)
    if pdf_dir is None:
        return None

    candidate = pdf_index.get(lookup_key.lower())
    if candidate is not None and candidate.exists() and candidate.is_file():
        try:
            candidate.relative_to(pdf_dir)
        except ValueError:
            return None
        return candidate

    if not force_refresh:
        return _find_montaggio_pdf_path(
            cod_art,
            indice_modifica,
            force_refresh=True,
        )

    return None


def _get_metodo_pdf_dir(path_key: str) -> Path | None:
    path_key = _norm_text(path_key)
    if not path_key:
        return None

    base = _norm_text(current_app.config.get(path_key))
    if not base:
        return None

    path = Path(base).expanduser()
    try:
        path = path.resolve()
    except Exception:
        pass

    return path if path.exists() and path.is_dir() else None


def _get_metodo_pdf_index(
    path_key: str,
    *,
    force_refresh: bool = False,
) -> tuple[Path | None, dict[str, Path]]:
    pdf_dir = _get_metodo_pdf_dir(path_key)
    if pdf_dir is None:
        return None, {}

    directory_key = f"{path_key}:{str(pdf_dir).lower()}"
    now_monotonic = monotonic()

    with _metodo_pdf_index_lock:
        cache = _metodo_pdf_index_cache.get(directory_key)

        if (
            cache
            and not force_refresh
            and float(cache.get("expires_at") or 0) > now_monotonic
        ):
            return pdf_dir, dict(cache.get("files") or {})

        files = _scan_montaggio_pdf_directory(pdf_dir)

        _metodo_pdf_index_cache[directory_key] = {
            "expires_at": now_monotonic + MONTAGGIO_PDF_INDEX_TTL_SECONDS,
            "files": files,
        }

        return pdf_dir, dict(files)


def _normalize_indice_modifica_for_pdf(value) -> str:
    indice = _norm_text(value)

    if not indice:
        return ""

    if indice == "-" or indice.upper() in {"X", "NAN", "NONE", "NULL"}:
        return ""

    return indice


def _build_metodo_montaggio_lookup(odp_rows) -> dict[str, dict]:
    lookup = {}

    for ordine in odp_rows or []:
        cod_art = _norm_text(getattr(ordine, "CodArt", ""))
        indice_modifica = _normalize_indice_modifica_for_pdf(
            getattr(ordine, "IndiceModifica", "")
        )

        key = _build_montaggio_pdf_key(cod_art, indice_modifica)

        if not key or key in lookup:
            continue

        pdf_path = _find_montaggio_pdf_path(cod_art, indice_modifica)

        lookup[key] = {
            "found": pdf_path is not None,
            "url": (
                url_for(
                    "main.api_metodo_montaggio_pdf",
                    cod_art=cod_art,
                    indice_modifica=indice_modifica,
                    tab_session=active_token(),
                )
                if pdf_path is not None
                else ""
            ),
        }

    return lookup


def _build_metodo_pdf_key(
    cod_art: str,
    indice_modifica: str = "",
    *,
    prefisso: str = "",
) -> str:
    cod_art = _norm_text(cod_art)
    indice_modifica = _normalize_indice_modifica_for_pdf(indice_modifica)
    prefisso = _norm_text(prefisso)

    if not cod_art:
        return ""

    return (
        f"{prefisso}{cod_art}.{indice_modifica}"
        if indice_modifica
        else f"{prefisso}{cod_art}"
    )


def _find_metodo_pdf_path(
    *,
    cod_art: str,
    indice_modifica: str = "",
    path_key: str,
    prefisso: str = "",
    force_refresh: bool = False,
) -> Path | None:
    lookup_key = _build_metodo_pdf_key(
        cod_art,
        indice_modifica,
        prefisso=prefisso,
    )

    if not lookup_key:
        return None

    pdf_dir, pdf_index = _get_metodo_pdf_index(
        path_key,
        force_refresh=force_refresh,
    )

    if pdf_dir is None:
        return None

    candidate = pdf_index.get(lookup_key.lower())

    if candidate is not None and candidate.exists() and candidate.is_file():
        try:
            candidate.relative_to(pdf_dir)
        except ValueError:
            return None
        return candidate

    if not force_refresh:
        return _find_metodo_pdf_path(
            cod_art=cod_art,
            indice_modifica=indice_modifica,
            path_key=path_key,
            prefisso=prefisso,
            force_refresh=True,
        )

    return None


def _build_metodo_lookup(
    odp_rows,
    *,
    path_key: str = "MONTAGGIO_PDF_DIR",
    prefisso: str = "",
) -> dict[str, dict]:
    lookup = {}

    for ordine in odp_rows or []:
        cod_art = _norm_text(getattr(ordine, "CodArt", ""))
        indice_modifica = _normalize_indice_modifica_for_pdf(
            getattr(ordine, "IndiceModifica", "")
        )

        key = _build_metodo_pdf_key(
            cod_art,
            indice_modifica,
            prefisso=prefisso,
        )

        if not key or key in lookup:
            continue

        pdf_path = _find_metodo_pdf_path(
            cod_art=cod_art,
            indice_modifica=indice_modifica,
            path_key=path_key,
            prefisso=prefisso,
        )

        lookup[key] = {
            "found": pdf_path is not None,
            "url": (
                url_for(
                    "main.api_metodo_pdf",
                    cod_art=cod_art,
                    indice_modifica=indice_modifica,
                    path_key=path_key,
                    prefisso=prefisso,
                    tab_session=active_token(),
                )
                if pdf_path is not None
                else ""
            ),
        }

    return lookup


def _normalize_article_search_token(value) -> str:
    return _norm_text(value)


def _build_materiale_image_key(
    cod_art: str,
    variante_art: str = "",
    indice_modifica: str = "",
) -> str:
    cod_art = _normalize_article_search_token(cod_art)
    variante_art = _normalize_variante_articolo_search(variante_art)
    indice_modifica = _normalize_indice_articolo_search(indice_modifica)

    if not cod_art:
        return ""

    if variante_art and indice_modifica:
        return f"{cod_art}.{variante_art}.{indice_modifica}"
    if variante_art:
        return f"{cod_art}.{variante_art}"
    if indice_modifica:
        return f"{cod_art}..{indice_modifica}"
    return cod_art


def _get_materiale_image_dir() -> Path | None:
    raw = _norm_text(current_app.config.get("FOTOGRAFIE_MATERIALE"))
    if not raw:
        return None

    img_dir = Path(raw).expanduser()
    try:
        img_dir = img_dir.resolve()
    except Exception:
        pass

    if not img_dir.exists() or not img_dir.is_dir():
        return None

    return img_dir


def _scan_materiale_image_directory(img_dir: Path) -> dict[str, Path]:
    files = {}

    try:
        for entry in img_dir.iterdir():
            if not entry.is_file():
                continue
            if entry.suffix.lower() != ".png":
                continue

            stem = entry.stem.strip()
            if not stem:
                continue

            try:
                resolved = entry.resolve()
            except Exception:
                resolved = entry

            files.setdefault(stem.lower(), resolved)
    except Exception:
        return {}

    return files


def _get_materiale_image_index(
    *,
    force_refresh: bool = False,
) -> tuple[Path | None, dict[str, Path]]:
    img_dir = _get_materiale_image_dir()
    if img_dir is None:
        return None, {}

    directory_key = str(img_dir).lower()
    now_monotonic = monotonic()

    with _materiale_img_index_lock:
        cache_valid = (
            not force_refresh
            and _materiale_img_index_cache["directory"] == directory_key
            and float(_materiale_img_index_cache["expires_at"]) > now_monotonic
        )

        if cache_valid:
            return img_dir, dict(_materiale_img_index_cache["files"])

        files = _scan_materiale_image_directory(img_dir)
        _materiale_img_index_cache["directory"] = directory_key
        _materiale_img_index_cache["expires_at"] = (
            now_monotonic + MATERIALE_IMG_INDEX_TTL_SECONDS
        )
        _materiale_img_index_cache["files"] = files

        return img_dir, dict(files)


def _find_materiale_image_path(
    cod_art: str,
    variante_art: str = "",
    indice_modifica: str = "",
    *,
    force_refresh: bool = False,
) -> Path | None:
    lookup_key = _build_materiale_image_key(
        cod_art=cod_art,
        variante_art=variante_art,
        indice_modifica=indice_modifica,
    )
    if not lookup_key:
        return None

    img_dir, img_index = _get_materiale_image_index(force_refresh=force_refresh)
    if img_dir is None:
        return None

    candidate = img_index.get(lookup_key.lower())
    if candidate is not None and candidate.exists() and candidate.is_file():
        try:
            candidate.relative_to(img_dir)
        except ValueError:
            return None
        return candidate

    if not force_refresh:
        return _find_materiale_image_path(
            cod_art=cod_art,
            variante_art=variante_art,
            indice_modifica=indice_modifica,
            force_refresh=True,
        )

    return None


def _norm_articolo_search_value(value) -> str:
    return str(value or "").strip().upper()


def _norm_articolo_revisione(value) -> str:
    raw = _norm_articolo_search_value(value)

    if raw in {"", "X", "-", "NONE", "NULL", "NAN"}:
        return ""

    return raw


def _same_articolo_variante(db_value, search_value) -> bool:
    return _norm_articolo_search_value(db_value) == _norm_articolo_search_value(
        search_value
    )


def _same_articolo_revisione(db_value, search_value) -> bool:
    return _norm_articolo_revisione(db_value) == _norm_articolo_revisione(search_value)


def _find_articolo_lookup(
    cod_art: str,
    variante_art: str = "",
):
    cod_art_norm = _norm_articolo_search_value(cod_art)
    variante_norm = _norm_articolo_search_value(variante_art)

    if not cod_art_norm:
        return None

    candidates = AcqArticoliLookup.query.filter(
        func.upper(func.trim(AcqArticoliLookup.CodArt)) == cod_art_norm
    ).all()

    if variante_norm:
        candidates = [
            row
            for row in candidates
            if _same_articolo_variante(row.VarianteArt, variante_norm)
        ]

    if not candidates:
        return None

    # Preferisce il candidato più completo
    candidates.sort(
        key=lambda row: (
            0 if _norm_articolo_search_value(getattr(row, "VarianteArt", "")) else 1,
            0 if _norm_articolo_revisione(getattr(row, "IndiceModifica", "")) else 1,
        )
    )

    return candidates[0]


def _is_articolo_search_state(stato: str) -> bool:
    s = _norm_text(stato).lower()
    if s == "chiusa":
        return False
    return "pianificat" in s or "attiv" in s or "sospes" in s


def _component_matches_search(
    comp: dict,
    *,
    cod_art: str,
    variante_art: str,
    indice_modifica: str,
) -> bool:
    return (
        _normalize_article_search_token(comp.get("CodArt")) == cod_art
        and _normalize_variante_articolo_search(comp.get("VarianteArt")) == variante_art
        and _normalize_indice_articolo_search(comp.get("IndiceModifica"))
        == indice_modifica
    )


def _build_articolo_ordini_attivi_rows(
    *,
    cod_art: str,
    variante_art: str,
    indice_modifica: str,
) -> list[dict]:
    rows = []

    for ordine in _base_odp_query().all():
        stato = _norm_text(getattr(ordine, "StatoOrdine", ""))
        if not _is_articolo_search_state(stato):
            continue

        distinta = _parse_distinta_materiale(ordine)
        qty_tot = Decimal("0")

        for comp in distinta:
            if not isinstance(comp, dict):
                continue

            if not _component_matches_search(
                comp,
                cod_art=cod_art,
                variante_art=variante_art,
                indice_modifica=indice_modifica,
            ):
                continue

            try:
                qty_tot += _parse_qty_decimal(comp.get("Quantita"))
            except ValueError:
                continue

        if qty_tot <= 0:
            continue

        rows.append(
            {
                "Ordine": _ordine_ref_label(ordine),
                "CodArtProdotto": _norm_text(getattr(ordine, "CodArt", "")),
                "VarianteProdotto": _norm_text(getattr(ordine, "VarianteArt", "")),
                "IndiceModificaProdotto": _norm_text(
                    getattr(ordine, "IndiceModifica", "")
                ),
                "DescrizioneProdotto": _norm_text(getattr(ordine, "DesArt", "")),
                "Stato": stato,
                "QuantitaComponente": _decimal_to_text(qty_tot),
            }
        )

    rows.sort(
        key=lambda x: (
            (x.get("Ordine") or "").lower(),
            (x.get("CodArtProdotto") or "").lower(),
            (x.get("VarianteProdotto") or "").lower(),
            (x.get("IndiceModificaProdotto") or "").lower(),
        )
    )
    return rows
