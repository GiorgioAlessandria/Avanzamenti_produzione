from datetime import date

import pytest

from app_odp.services import (
    manutenzioni_giorni_non_lavorativi_service as service,
)
from app_odp.services.manutenzioni_service import ManutenzioniServiceError


def test_festivita_anticipata_o_posticipata_in_base_a_oggi():
    data_festiva = date(2026, 8, 15)

    anticipata, _ = service.normalizza_data_manutenzione(
        data_festiva,
        reference_date=date(2026, 8, 13),
    )
    posticipata, _ = service.normalizza_data_manutenzione(
        data_festiva,
        reference_date=date(2026, 8, 14),
    )

    assert anticipata == date(2026, 8, 14)
    assert posticipata == date(2026, 8, 17)


def test_creazione_intervallo_inclusivo(monkeypatch):
    giorni_creati = []

    def fake_create(payload, policy, *, commit):
        assert commit is False
        giorni_creati.append(payload["data"])
        return payload["data"]

    monkeypatch.setattr(
        service,
        "create_giorno_non_lavorativo",
        fake_create,
    )

    result = service.create_giorni_non_lavorativi(
        {
            "data": "2026-08-10",
            "data_fine": "2026-08-12",
        },
        object(),
        commit=False,
    )

    assert result == giorni_creati == [
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 12),
    ]

    with pytest.raises(
        ManutenzioniServiceError,
        match="data finale",
    ):
        service.create_giorni_non_lavorativi(
            {
                "data": "2026-08-12",
