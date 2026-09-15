import json
from unittest.mock import patch

from flask import Flask

from app_odp.services.vendite_localizzazione_service import _address, _geocode


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps([{
            "lat": "44.4056", "lon": "8.9463", "display_name": "Genova, Italia",
        }]).encode()


def test_address_and_geocoder_headers():
    client = type("Client", (), {
        "Indirizzo": "VIA MARINA NEL MONDO, 62", "Cap": "70043",
        "Localita": "MONOPOLI", "Provincia": "BA", "CodStato": "IT",
    })()
    assert _address(client) == "VIA MARINA NEL MONDO, 62, 70043 MONOPOLI (BA), IT"

    app = Flask(__name__)
    app.config.update(
        VENDITE_GEOCODER_URL="https://example.test/search",
        VENDITE_GEOCODER_USER_AGENT="AvanzamentiProduzione/Test",
    )
    with app.app_context(), patch(
        "app_odp.services.vendite_localizzazione_service.urlopen",
        return_value=_Response(),
    ) as mocked:
        assert _geocode(_address(client)) == (44.4056, 8.9463, "Genova, Italia")
        request = mocked.call_args.args[0]
        assert request.headers["User-agent"] == "AvanzamentiProduzione/Test"
        assert "format=jsonv2" in request.full_url
