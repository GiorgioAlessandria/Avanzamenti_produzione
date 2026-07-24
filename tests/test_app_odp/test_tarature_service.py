from app_odp.services.tarature_service import _numero_seriale


def test_numero_seriale_assente_usa_il_codice_interno():
    seriale_s11 = _numero_seriale("-", "S11")
    seriale_s12 = _numero_seriale("-", "S12")

    assert seriale_s11 == "__NO_SERIAL__:S11"
    assert seriale_s12 == "__NO_SERIAL__:S12"
    assert _numero_seriale(" ab-1 ", "S11") == "AB-1"
