from io import BytesIO

import pytest
from werkzeug.datastructures import FileStorage

from app_odp.services.tarature_service import (
    _numero_seriale,
    _read_certificato_pdf,
    _save_certificato_pdf,
)


def test_numero_seriale_assente_usa_il_codice_interno():
    seriale_s11 = _numero_seriale("-", "S11")
    seriale_s12 = _numero_seriale("-", "S12")

    assert seriale_s11 == "__NO_SERIAL__:S11"
    assert seriale_s12 == "__NO_SERIAL__:S12"
    assert _numero_seriale(" ab-1 ", "S11") == "AB-1"


def test_certificato_accetta_solo_pdf_valido():
    upload = FileStorage(
        stream=BytesIO(b"%PDF-1.7\ncontenuto"),
        filename="Certificato 123.PDF",
    )

    filename, payload = _read_certificato_pdf(upload)

    assert filename == "Certificato_123.PDF"
    assert payload.startswith(b"%PDF-")

    with pytest.raises(ValueError, match="obbligatorio"):
        _read_certificato_pdf(None)
    with pytest.raises(ValueError, match="PDF valido"):
        _read_certificato_pdf(
            FileStorage(stream=BytesIO(b"non un pdf"), filename="falso.pdf")
        )


def test_certificato_viene_salvato_fuori_dal_database(tmp_path):
    payload = b"%PDF-1.7\ncontenuto"

    filename = _save_certificato_pdf(tmp_path, payload)

    assert filename.endswith(".pdf")
    assert (tmp_path / filename).read_bytes() == payload
