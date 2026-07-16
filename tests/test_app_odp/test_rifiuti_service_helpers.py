from decimal import Decimal
from types import SimpleNamespace

import pytest
from flask import Flask

from app_odp.models import db
from app_odp.rifiuti_models import RifiutiCarico, RifiutiCer
from app_odp.services.rifiuti_service import (
    CaricoRifiutoNonValidoError,
    CodiceCerNonValidoError,
    PesoRifiutoNonValidoError,
    _normalize_carico_ids,
    _parse_peso_kg,
    calculate_totale_presente,
    create_carico_rifiuto,
    create_codice_cer,
    deactivate_codice_cer,
    format_peso_kg,
    smaltisci_carichi,
    update_codice_cer,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", Decimal("1.000")),
        ("1.2", Decimal("1.200")),
        ("1,25", Decimal("1.250")),
        (12.5, Decimal("12.500")),
    ],
)
def test_parse_peso_kg_accepts_valid_values(
    raw,
    expected,
):
    assert _parse_peso_kg(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "abc",
        "0",
        "-1",
        "NaN",
        "Infinity",
        "12,3456",
        "1e2",
    ],
)
def test_parse_peso_kg_rejects_invalid_values(
    raw,
):
    with pytest.raises(
        PesoRifiutoNonValidoError
    ):
        _parse_peso_kg(raw)


def test_format_peso_kg_uses_italian_separator():
    assert format_peso_kg(
        Decimal("12.5")
    ) == "12,500"


def test_normalize_carico_ids_removes_duplicates_and_rejects_empty():
    assert _normalize_carico_ids(["2", 1, "2"]) == [2, 1]

    with pytest.raises(CaricoRifiutoNonValidoError):
        _normalize_carico_ids([])


@pytest.fixture
def rifiuti_app():
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"rifiuti": "sqlite://"},
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)

    with app.app_context():
        db.create_all(bind_key="rifiuti")
        yield app
        db.session.remove()


def test_smaltisci_carichi_updates_stock_and_audit_fields(rifiuti_app):
    cer = RifiutiCer(codice="15 01 01", descrizione="Carta e cartone")
    db.session.add(cer)
    db.session.commit()
    user = SimpleNamespace(id=7, username="Mario Rossi")
    carico = create_carico_rifiuto(
        codice_cer_id=cer.id,
        peso_kg="12,500",
        user=user,
    )

    smaltisci_carichi(carico_ids=[carico.id], user=user)

    saved = db.session.get(RifiutiCarico, carico.id)
    assert saved.stato == "SMALTITO"
    assert saved.smaltito_il
    assert saved.smaltito_da_id == 7
    assert saved.smaltito_da_nome == "Mario Rossi"
    assert calculate_totale_presente() == Decimal("0.000")

    with pytest.raises(CaricoRifiutoNonValidoError):
        smaltisci_carichi(carico_ids=[carico.id], user=user)


def test_smaltisci_carichi_validates_all_ids_before_changes(rifiuti_app):
    cer = RifiutiCer(codice="15 01 02", descrizione="Plastica")
    db.session.add(cer)
    db.session.commit()
    user = SimpleNamespace(id=8, username="Anna Bianchi")
    carico = create_carico_rifiuto(
        codice_cer_id=cer.id,
        peso_kg="2",
        user=user,
    )

    with pytest.raises(CaricoRifiutoNonValidoError):
        smaltisci_carichi(carico_ids=[carico.id, 999], user=user)

    assert db.session.get(RifiutiCarico, carico.id).stato == "PRESENTE"


def test_codici_cer_create_update_deactivate_and_reactivate(rifiuti_app):
    cer = create_codice_cer(
        codice="15 01 01",
        descrizione="Carta",
    )
    cer_id = cer.id

    updated = update_codice_cer(
        codice_cer_id=cer_id,
        codice="15 01 01",
        descrizione="Carta e cartone",
    )
    assert updated.descrizione == "Carta e cartone"

    deactivate_codice_cer(cer_id)
    assert db.session.get(RifiutiCer, cer_id).attivo is False

    reactivated = create_codice_cer(
        codice="15 01 01",
        descrizione="Carta riciclata",
    )
    assert reactivated.id == cer_id
    assert reactivated.attivo is True
    assert reactivated.descrizione == "Carta riciclata"

    with pytest.raises(CodiceCerNonValidoError):
        create_codice_cer(
            codice="15 01 01",
            descrizione="Duplicato",
        )
