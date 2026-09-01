from decimal import Decimal
from types import SimpleNamespace

from openpyxl import load_workbook
import pytest

from app_odp.services import rifiuti_service as service
from app_odp.services.rifiuti_service import build_rifiuti_stock_export


def test_stock_export_raggruppa_per_codice_cer_e_somma_il_peso():
    cer_1 = SimpleNamespace(codice="17 04 05", descrizione="Ferro e acciaio")
    cer_2 = SimpleNamespace(codice="15 01 01", descrizione="Imballaggi in carta")
    carichi = [
        SimpleNamespace(cer=cer_1, peso_kg=Decimal("12.250")),
        SimpleNamespace(cer=cer_2, peso_kg=Decimal("3.000")),
        SimpleNamespace(cer=cer_1, peso_kg=Decimal("7.750")),
    ]

    sheet = load_workbook(build_rifiuti_stock_export(carichi)).active

    assert list(sheet.values) == [
        ("Codice CER", "Descrizione CER", "Peso totale kg"),
        ("15 01 01", "Imballaggi in carta", 3),
        ("17 04 05", "Ferro e acciaio", 20),
    ]


def test_delete_carico_rifiuto_elimina_un_carico_presente(monkeypatch):
    carico = SimpleNamespace(id=7, stato="PRESENTE")
    deleted = []
    session = SimpleNamespace(
        get=lambda model, carico_id: carico if carico_id == 7 else None,
        delete=deleted.append,
        commit=lambda: None,
    )
    monkeypatch.setattr(service, "db", SimpleNamespace(session=session))

    result = service.delete_carico_rifiuto("7")

    assert result is carico
    assert deleted == [carico]

    deleted.clear()
    carico.stato = "SMALTITO"
    with pytest.raises(service.CaricoRifiutoNonValidoError):
        service.delete_carico_rifiuto("7")

    assert deleted == []
