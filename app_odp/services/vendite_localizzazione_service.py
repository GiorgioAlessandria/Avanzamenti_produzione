import hashlib
import json
import threading
import time
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from flask import current_app
from sqlalchemy import func
from sqlalchemy.orm import selectinload

from app_odp.models import AcqClienteFornitore, db
from app_odp.vendite_models import (
    VenditeClienteGeocodifica,
    VenditeOrdineCliente,
)


ROME_TZ = ZoneInfo("Europe/Rome")
_GEOCODE_LOCK = threading.Lock()
_LAST_GEOCODE_AT = 0.0
_GEOCODING_VERSION = "v2"
_COUNTRY_NAMES = {
    "CN": "China", "GB": "United Kingdom", "JP": "Japan",
    "KR": "South Korea", "US": "United States",
}


class VenditeGeocodificaError(RuntimeError):
    pass


def _text(value) -> str:
    return str(value or "").strip()


def _address(client) -> str:
    locality = " ".join(filter(None, (_text(client.Cap), _text(client.Localita))))
    province = _text(client.Provincia)
    if province:
        locality = f"{locality} ({province})" if locality else province
    return ", ".join(filter(None, (
        _text(client.Indirizzo), locality, _text(client.CodStato),
    )))


def _address_key(address: str) -> str:
    value = f"{_GEOCODING_VERSION}|{address.casefold()}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _legacy_address_key(address: str) -> str:
    return hashlib.sha256(address.casefold().encode("utf-8")).hexdigest()


def _geocode_attempts(client) -> list[tuple[str, str]]:
    street = _text(client.Indirizzo)
    postal_code = _text(client.Cap)
    locality = _text(client.Localita)
    province = _text(client.Provincia)
    country_code = _text(client.CodStato).upper()
    country = _COUNTRY_NAMES.get(country_code, country_code)
    candidates = [
        (_address(client), "indirizzo"),
        (", ".join(filter(None, (street, locality, province, postal_code, country))), "indirizzo"),
        (", ".join(filter(None, (street, postal_code, country))), "strada"),
        (", ".join(filter(None, (postal_code, locality, province, country))), "CAP"),
        (", ".join(filter(None, (locality, province, country))), "città"),
    ]
    attempts = []
    seen = set()
    for query, precision in candidates:
        key = query.casefold()
        if query and key not in seen:
            seen.add(key)
            attempts.append((query, precision))
    return attempts


def _automatic_orders():
    return (
        VenditeOrdineCliente.query.options(selectinload(VenditeOrdineCliente.righe))
        .filter(VenditeOrdineCliente.gestionale_cod_cliente.is_not(None))
        .order_by(VenditeOrdineCliente.cliente_nome.asc())
        .all()
    )


def _clients_by_code(codes: set[str]) -> dict[str, AcqClienteFornitore]:
    if not codes:
        return {}
    return {
        _text(client.CodCliFor): client
        for client in AcqClienteFornitore.query.filter(
            func.trim(func.coalesce(AcqClienteFornitore.TipoAnagrafica, "")) == "1",
            AcqClienteFornitore.CodCliFor.in_(codes),
        ).all()
    }


def _machine_payload(row) -> dict:
    return {
        "order": _text(row.gestionale_id_documento),
        "model_code": row.modello_codice,
        "variant": row.modello_variante or "",
        "description": row.modello_descrizione or "",
        "serial_number": row.odp_matricola or "",
        "delivery_date": row.data_consegna.isoformat(),
    }


def build_customer_locations() -> dict:
    orders = _automatic_orders()
    codes = {_text(order.gestionale_cod_cliente) for order in orders}
    clients = _clients_by_code(codes)
    cached = {
        item.cliente_codice: item
        for item in VenditeClienteGeocodifica.query.filter(
            VenditeClienteGeocodifica.cliente_codice.in_(codes)
        ).all()
    } if codes else {}

    locations = []
    pending = []
    unlocated = []
    for order in orders:
        code = _text(order.gestionale_cod_cliente)
        client = clients.get(code)
        address = _address(client) if client else ""
        cache = cached.get(code)
        address_key = _address_key(address) if address else ""
        machines = sorted(
            (_machine_payload(row) for row in order.righe),
            key=lambda item: (
                item["delivery_date"], item["model_code"].casefold(),
                item["variant"].casefold(),
            ),
        )
        base = {
            "customer_code": code,
            "customer_name": order.cliente_nome,
            "address": address,
            "machines": machines,
        }
        if not address:
            unlocated.append({**base, "reason": "Indirizzo non disponibile"})
        elif cache is None or (
            cache.indirizzo_chiave != address_key
            and not (
                cache.latitudine is not None
                and cache.longitudine is not None
                and cache.indirizzo_chiave == _legacy_address_key(address)
            )
        ):
            pending.append(base)
        elif cache.latitudine is None or cache.longitudine is None:
            unlocated.append({**base, "reason": "Indirizzo non trovato"})
        else:
            locations.append({
                **base,
                "latitude": cache.latitudine,
                "longitude": cache.longitudine,
                "place_name": cache.nome_luogo or address,
                "precision": cache.precisione or "indirizzo",
            })

    return {
        "generated_at": datetime.now(ROME_TZ).isoformat(timespec="seconds"),
        "locations": locations,
        "pending": pending,
        "unlocated": unlocated,
    }


def _geocode(address: str, country_code: str = "") -> tuple[float | None, float | None, str]:
    global _LAST_GEOCODE_AT
    params = {"q": address, "format": "jsonv2", "limit": 1}
    if country_code:
        params["countrycodes"] = country_code.casefold()
    query = urlencode(params)
    url = f"{current_app.config['VENDITE_GEOCODER_URL']}?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": current_app.config["VENDITE_GEOCODER_USER_AGENT"],
            "Accept": "application/json",
            "Accept-Language": "it",
        },
    )
    try:
        with _GEOCODE_LOCK:
            # ponytail: limite per processo; passare a un rate limiter condiviso
            # solo se l'applicazione verrà eseguita con più processi WSGI.
            wait = 1.05 - (time.monotonic() - _LAST_GEOCODE_AT)
            if wait > 0:
                time.sleep(wait)
            try:
                with urlopen(request, timeout=12) as response:
                    result = json.loads(response.read().decode("utf-8"))
            finally:
                _LAST_GEOCODE_AT = time.monotonic()
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        raise VenditeGeocodificaError(
            "Il servizio di localizzazione non è disponibile. Riprovare più tardi."
        ) from exc

    if not isinstance(result, list) or not result:
        return None, None, ""
    try:
        return float(result[0]["lat"]), float(result[0]["lon"]), _text(
            result[0].get("display_name")
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise VenditeGeocodificaError(
            "Il servizio di localizzazione ha restituito dati non validi."
        ) from exc


def geocode_next_customer() -> dict:
    data = build_customer_locations()
    if not data["pending"]:
        return data

    customer = data["pending"][0]
    client = _clients_by_code({customer["customer_code"]}).get(
        customer["customer_code"]
    )
    latitude = longitude = None
    place_name = ""
    precision = None
    if client is not None:
        country_code = _text(client.CodStato)
        for query, attempt_precision in _geocode_attempts(client):
            latitude, longitude, place_name = _geocode(query, country_code)
            if latitude is not None and longitude is not None:
                precision = attempt_precision
                break
    item = db.session.get(VenditeClienteGeocodifica, customer["customer_code"])
    if item is None:
        item = VenditeClienteGeocodifica(cliente_codice=customer["customer_code"])
        db.session.add(item)
    item.indirizzo_chiave = _address_key(customer["address"])
    item.latitudine = latitude
    item.longitudine = longitude
    item.nome_luogo = place_name or None
    item.precisione = precision
    item.aggiornato_il = datetime.now(ROME_TZ).isoformat(timespec="seconds")
    db.session.flush()
    return build_customer_locations()
