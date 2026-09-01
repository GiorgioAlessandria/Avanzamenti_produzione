import importlib
from decimal import Decimal
from types import SimpleNamespace

from openpyxl import load_workbook
from sqlalchemy import create_engine, inspect

from app_odp.services.rifiuti_service import build_rifiuti_stock_export


def test_stock_export_separa_descrizioni_dello_stesso_codice_cer():
    cer_inox = SimpleNamespace(codice="12 01 01", descrizione="Trucioli inox")
    cer_ferro = SimpleNamespace(codice="12 01 01", descrizione="Trucioli ferro")
    carichi = [
        SimpleNamespace(cer=cer_inox, peso_kg=Decimal("60.000")),
        SimpleNamespace(cer=cer_ferro, peso_kg=Decimal("125.000")),
        SimpleNamespace(cer=cer_inox, peso_kg=Decimal("40.000")),
    ]

    sheet = load_workbook(build_rifiuti_stock_export(carichi)).active

    assert list(sheet.values) == [
        ("Codice CER", "Descrizione CER", "Peso totale kg"),
        ("12 01 01", "Trucioli ferro", 125),
        ("12 01 01", "Trucioli inox", 100),
    ]


def test_schema_migra_unicita_da_codice_a_codice_descrizione(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE rifiuti_cer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codice TEXT NOT NULL UNIQUE,
                descrizione TEXT NOT NULL,
                attivo BOOLEAN NOT NULL DEFAULT 1,
                creato_il TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                aggiornato_il TEXT
            )
            """
        )
        connection.exec_driver_sql(
            "INSERT INTO rifiuti_cer (codice, descrizione) "
            "VALUES ('12 01 01', 'Trucioli inox')"
        )

    app_module = importlib.import_module("app_odp.app")
    monkeypatch.setattr(
        app_module,
        "db",
        SimpleNamespace(engines={"rifiuti": engine}),
    )
    app_module._ensure_rifiuti_schema()

    unique_columns = {
        tuple(constraint["column_names"])
        for constraint in inspect(engine).get_unique_constraints("rifiuti_cer")
    }
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO rifiuti_cer (codice, descrizione) "
            "VALUES ('12 01 01', 'Trucioli ferro')"
        )
        count = connection.exec_driver_sql(
            "SELECT COUNT(*) FROM rifiuti_cer WHERE codice = '12 01 01'"
        ).scalar_one()

    assert unique_columns == {("codice", "descrizione")}
    assert count == 2
