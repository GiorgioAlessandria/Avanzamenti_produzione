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
    return hashlib.sha256(address.casefold().encode("utf-8")).hexdigest()


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
        elif cache is None or cache.indirizzo_chiave != address_key:
            pending.append(base)
        elif cache.latitudine is None or cache.longitudine is None:
            unlocated.append({**base, "reason": "Indirizzo non trovato"})
        else:
            locations.append({
                **base,
                "latitude": cache.latitudine,
                "longitude": cache.longitudine,
                "place_name": cache.nome_luogo or address,
            })

    return {
        "generated_at": datetime.now(ROME_TZ).isoformat(timespec="seconds"),
        "locations": locations,
        "pending": pending,
        "unlocated": unlocated,
    }


def _geocode(address: str) -> tuple[float | None, float | None, str]:
    global _LAST_GEOCODE_AT
    query = urlencode({"q": address, "format": "jsonv2", "limit": 1})
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
    latitude, longitude, place_name = _geocode(customer["address"])
    item = db.session.get(VenditeClienteGeocodifica, customer["customer_code"])
    if item is None:
        item = VenditeClienteGeocodifica(cliente_codice=customer["customer_code"])
        db.session.add(item)
    item.indirizzo_chiave = _address_key(customer["address"])
    item.latitudine = latitude
    item.longitudine = longitude
    item.nome_luogo = place_name or None
    item.aggiornato_il = datetime.now(ROME_TZ).isoformat(timespec="seconds")
    db.session.flush()
    return build_customer_locations()
