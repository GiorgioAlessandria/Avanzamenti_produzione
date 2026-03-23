import importlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

MODULE_PATH = "app_odp.routes"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


@pytest.fixture()
def app_ctx(tmp_path):
    app = Flask(__name__, instance_path=str(tmp_path / "instance"))
    app.config["ERP_EXPORT_DIR"] = str(tmp_path / "exports")
    with app.app_context():
        yield app


def test_parse_qty_decimal_accepts_blank_comma_and_invalid(mod):
    assert mod._parse_qty_decimal("") == Decimal("0")
    assert mod._parse_qty_decimal("1,25") == Decimal("1.25")
    assert mod._parse_qty_decimal("7") == Decimal("7")

    with pytest.raises(ValueError, match="Quantità non valida"):
        mod._parse_qty_decimal("abc")


def test_parse_qty_integer_decimal_requires_integral(mod):
    assert mod._parse_qty_integer_decimal("5") == Decimal("5")
    assert mod._parse_qty_integer_decimal("5.0") == Decimal("5.0")

    with pytest.raises(ValueError, match="pezzi deve essere un numero intero"):
        mod._parse_qty_integer_decimal("5.5", field_name="pezzi")


def test_parse_bool_flag_and_decimal_to_text(mod):
    assert mod._parse_bool_flag(True) is True
    assert mod._parse_bool_flag("Sì") is True
    assert mod._parse_bool_flag("on") is True
    assert mod._parse_bool_flag("0") is False
    assert mod._parse_bool_flag(None) is False

    assert mod._decimal_to_text(Decimal("0")) == "0"
    assert mod._decimal_to_text(Decimal("12.3400")) == "12.34"
    assert mod._decimal_to_text(5) == "5"


def test_qty_da_lavorare_helpers_fallback_to_quantita(mod):
    ordine = SimpleNamespace(QtyDaLavorare="", Quantita="12,5")
    assert mod._qty_da_lavorare_text(ordine) == "12,5"
    assert mod._qty_da_lavorare_decimal(ordine) == Decimal("12.5")

    ordine2 = SimpleNamespace(QtyDaLavorare="7", Quantita="12")
    assert mod._qty_da_lavorare_text(ordine2) == "7"
    assert mod._qty_da_lavorare_decimal(ordine2) == Decimal("7")


def test_parse_distinta_materiale_handles_plain_double_encoded_and_bad_json(mod):
    plain = SimpleNamespace(DistintaMateriale='[{"CodArt": "A1"}]')
    double_encoded = SimpleNamespace(DistintaMateriale='"[{\\"CodArt\\": \\"B2\\"}]"')
    broken = SimpleNamespace(DistintaMateriale="not-json")
    missing = SimpleNamespace(DistintaMateriale=None)

    assert mod._parse_distinta_materiale(plain) == [{"CodArt": "A1"}]
    assert mod._parse_distinta_materiale(double_encoded) == [{"CodArt": "B2"}]
    assert mod._parse_distinta_materiale(broken) == []
    assert mod._parse_distinta_materiale(missing) == []


def test_phase_helpers_parse_and_current_phase_export(mod):
    ordine = SimpleNamespace(FaseAttiva="2.0", NumFase='["1", "2", "3"]')
    stato = SimpleNamespace(Fase="")

    assert mod._fase_attiva_int(ordine) == 2
    assert mod._fase_to_int("3.0") == 3
    assert mod._fase_to_int("abc") is None
    assert mod._fase_corrente_for_export(ordine, stato=stato) == "2"
    assert mod._fase_corrente_for_export(ordine, fase_override="4") == "4"

    single_phase_order = SimpleNamespace(FaseAttiva="", NumFase='["7"]')
    assert mod._fase_corrente_for_export(single_phase_order) == "7"


def test_parse_jsonish_list_parse_phase_list_and_active_value_for_phase(mod):
    assert mod._parse_jsonish_list(None) == []
    assert mod._parse_jsonish_list('["A", "B"]') == ["A", "B"]
    assert mod._parse_jsonish_list("VAL") == ["VAL"]

    assert mod._parse_phase_list('["1", "2"]') == ["1", "2"]
    assert mod._parse_phase_list("3") == ["3"]

    assert mod._active_value_for_phase('["R1", "R2"]', '["1", "2"]', "2") == "R2"
    assert mod._active_value_for_phase('["R1", "R2"]', '["1", "2"]', "3") == "R1"
    assert mod._active_value_for_phase(None, '["1"]', "1") == ""


def test_sync_active_fields_for_phase_updates_order(mod):
    ordine = SimpleNamespace(
        FaseAttiva="2",
        NumFase='["1", "2"]',
        CodLavorazione='["LAV1", "LAV2"]',
        CodRisorsaProd='["RIS1", "RIS2"]',
        LavorazioneAttiva="",
        RisorsaAttiva="",
    )

    mod._sync_active_fields_for_phase(ordine)

    assert ordine.LavorazioneAttiva == "LAV2"
    assert ordine.RisorsaAttiva == "RIS2"


def test_same_decimal_qty_and_scaled_component_qty(mod):
    assert mod._same_decimal_qty(Decimal("1.0000"), Decimal("1.00005")) is True
    assert mod._same_decimal_qty(Decimal("1"), Decimal("1.01")) is False

    assert mod._scaled_component_qty("10", Decimal("2"), Decimal("4")) == Decimal(
        "5.0000"
    )
    assert mod._scaled_component_qty("bad", Decimal("2"), Decimal("4")) == Decimal("0")
    assert mod._scaled_component_qty("10", Decimal("2"), Decimal("0")) == Decimal("10")


def test_normalize_payload_helpers_and_build_phase_payload(monkeypatch, mod):
    monkeypatch.setattr(mod, "current_user", SimpleNamespace(username="gio"))

    lotti = mod._normalize_lotti_for_payload(
        [
            {"CodArt": "A1", "RifLottoAlfa": "LOT1", "Quantita": 3, "Esito": "ko"},
            {"CodArt": "A2", "RifLottoAlfa": "LOT2"},
        ]
    )
    assert lotti == [
        {"CodArt": "A1", "RifLottoAlfa": "LOT1", "Quantita": "3", "Esito": "ko"},
        {"CodArt": "A2", "RifLottoAlfa": "LOT2", "Quantita": "0", "Esito": "ok"},
    ]

    lotto_prodotto = mod._normalize_lotto_prodotto_for_payload(
        {
            "CodArt": "PF1",
            "RifLottoAlfa": "LP1",
            "Quantita": 5,
            "Fase": 2,
            "ParentLotti": [{"CodArt": "C1", "RifLottoAlfa": "LC1", "Quantita": 2}],
        }
    )
    assert lotto_prodotto == {
        "cod_art": "PF1",
        "rif_lotto_alfa": "LP1",
        "quantita": "5",
        "fase": "2",
        "parent_lotti": [{"cod_art": "C1", "rif_lotto_alfa": "LC1", "quantita": "2"}],
    }

    ordine = SimpleNamespace(
        IdDocumento="100",
        IdRiga="10",
        RifRegistraz="RIF100",
        CodArt="PF1",
        DesArt="Prodotto",
        CodReparto="20",
        Quantita="12",
        QtyDaLavorare="7",
    )
    payload = mod._build_phase_payload(
        ordine=ordine,
        fase_corrente="2",
        q_ok=Decimal("3"),
        q_nok=Decimal("1"),
        tempo_finale="1.5",
        lotti_input=[{"CodArt": "A1", "RifLottoAlfa": "LOT1", "Quantita": 2}],
        lotto_prodotto={
            "CodArt": "PF1",
            "RifLottoAlfa": "LP1",
            "Quantita": 5,
            "Fase": 2,
        },
        note="note test",
        now_iso="2026-03-19T12:00:00+01:00",
        chiusura_parziale=True,
    )

    assert payload["created_by"] == "gio"
    assert payload["chiusura_parziale"] is True
    assert payload["quantita_da_lavorare"] == "7"
    assert payload["lotto_prodotto"]["rif_lotto_alfa"] == "LP1"
    assert payload["lotti"][0]["RifLottoAlfa"] == "LOT1"


def test_safe_txt_suffix_build_export_txt_path_and_write_txt_content(
    mod,
    monkeypatch,
    tmp_path,
):
    from datetime import datetime

    fixed_dt = datetime(2024, 1, 2, 13, 45, 56)

    monkeypatch.setattr(mod, "_now_rome_dt", lambda: fixed_dt)
    monkeypatch.setattr(mod, "_get_erp_export_dir", lambda: tmp_path)

    # 1) _safe_txt_suffix
    assert mod._safe_txt_suffix(" ordine / prova 01 ") == "ordine___prova_01"
    assert mod._safe_txt_suffix("___") == "export"
    assert mod._safe_txt_suffix("", "fallback") == "fallback"
    assert mod._safe_txt_suffix("abc-DEF_01") == "abc-DEF_01"

    # 2) _build_export_txt_path
    path_txt = mod._build_export_txt_path(prefix="AVPB", suffix=" ordine / prova 01 ")
    assert path_txt == tmp_path / "AVPB_ordine___prova_01_20240102_134556.txt"

    # 3) _write_txt_content
    written = mod._write_txt_content(
        "riga1\nriga2",
        prefix="AVPB",
        suffix=" ordine / prova 01 ",
        encoding="utf-8-sig",
    )

    assert written == path_txt
    assert written.exists()
    assert written.read_text(encoding="utf-8-sig") == "riga1\nriga2"

    # 4) fallback suffix -> export
    fallback_path = mod._build_export_txt_path(prefix="AVPB", suffix="___")
    assert fallback_path == tmp_path / "AVPB_export_20240102_134556.txt"


def test_json_loads_safe_and_first_not_blank(mod):
    assert mod._json_loads_safe('{"a": 1}', {}) == {"a": 1}
    assert mod._json_loads_safe("broken", {"fallback": True}) == {"fallback": True}

    assert mod._first_not_blank(None, "", "  ok  ", default="x") == "ok"
    assert mod._first_not_blank(None, "", default="x") == "x"


def test_formatters_for_avp_and_build_rif_orp_modes(monkeypatch, mod):
    fixed_dt = datetime(2026, 3, 19, 8, 9, 10, tzinfo=mod.ROME_TZ)
    monkeypatch.setattr(mod, "_now_rome_dt", lambda: fixed_dt)

    assert mod._format_datetime_for_avp("") == "19/03/2026 08:09:10"
    assert mod._format_datetime_for_avp("2026-03-19T08:09:10") == "19/03/2026 08:09:10"
    assert mod._format_datetime_for_avp("2026-03-19") == "19/03/2026 00:00:00"
    assert mod._format_datetime_for_avp("not-a-date") == "not-a-date"

    assert mod._format_decimal_it("12.345", places=2) == "12,35"
    assert mod._format_decimal_it("bad", places=2) == "0,00"
    assert mod._zero_fill_digits("AB-12", 5) == "00012"

    payload = {
        "rif_registraz": "RIF",
        "id_documento": "12",
        "id_riga": "3",
        "fase": "4",
    }
    assert mod._build_rif_orp(payload, {"rif90_mode": "raw_rif_registraz"}) == "RIF"
    assert mod._build_rif_orp(payload, {"rif90_mode": "riga"}) == "RIF.3"
    assert mod._build_rif_orp(payload, {"rif90_mode": "riga_fase"}) == "RIF.3.4"
    assert (
        mod._build_rif_orp(payload, {"rif90_mode": "barcode17"}) == "00000001200030004"
    )
    assert (
        mod._build_rif_orp(payload, {"rif90_mode": "barcode22"})
        == "0000000120000000030004"
    )


def test_pick_qta_export_and_serialize_helpers(mod):
    assert mod._pick_qta_export(
        {"quantita_ok": "2", "quantita_ko": "1"}, {"qta_mode": "ok"}
    ) == Decimal("2")
    assert mod._pick_qta_export(
        {"quantita_ok": "2", "quantita_ko": "1"}, {"qta_mode": "worked"}
    ) == Decimal("3")
    assert mod._pick_qta_export(
        {"quantita_ok": "bad", "quantita_ko": "1"}, {"qta_mode": "worked"}
    ) == Decimal("1")

    assert mod._serialize_avp_cell('A"B') == '"A""B"'
    assert mod._serialize_avp_cell(5, numeric=True) == "5"
    assert (
        mod._serialize_avp_row(["A", 5, 'B"C'], numeric_indexes={1}) == '"A";5;"B""C"'
    )


def test_row_key_norm_text_parse_iso_and_tempo_helpers(mod):
    assert mod._row_key("10", "20") == "10|20"
    assert mod._norm_text("  abc  ") == "abc"
    assert mod._norm_text(None) == ""

    naive = mod._parse_iso_dt("2026-03-19T10:00:00")
    aware = mod._parse_iso_dt("2026-03-19T10:00:00+01:00")
    assert naive is not None and naive.tzinfo == mod.ROME_TZ
    assert aware is not None and aware.tzinfo is not None
    assert mod._parse_iso_dt("bad") is None

    assert mod._tempo_to_seconds("1,5") == 5400
    assert mod._tempo_to_seconds("bad") == 0
    assert mod._seconds_to_tempo_text(0) == "0"
    assert mod._seconds_to_tempo_text(5400) == "1.5"


def test_extract_codes_from_cell_handles_nested_json_and_raw_text(mod):
    assert mod._extract_codes_from_cell(None) == []
    assert mod._extract_codes_from_cell("10") == ["10"]
    assert mod._extract_codes_from_cell('["10", ["20"], {"x": "30"}, "10"]') == [
        "10",
        "20",
        "30",
    ]
    assert mod._extract_codes_from_cell({"a": ["10", {"b": "20"}]}) == ["10", "20"]


import json
from dataclasses import dataclass

import pytest

MODULE_PATH = "app_odp.routes"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


class _Field:
    def __init__(self, name):
        self.name = name

    def in_(self, values):
        return ("in", self.name, tuple(values))

    def __eq__(self, other):
        return ("eq", self.name, other)

    def asc(self):
        return ("asc", self.name)

    def desc(self):
        return ("desc", self.name)


class _FakeResult:
    def __init__(self, scalar_value):
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value


class _FakeSession:
    def __init__(self):
        self.added = []
        self.flushed = 0
        self.query_arg = None
        self.scalar_value = None

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flushed += 1

    def query(self, arg):
        self.query_arg = arg
        return _FakeResult(self.scalar_value)


class _FakeQuery:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []
        self.kwargs = None

    def filter_by(self, **kwargs):
        self.calls.append(("filter_by", kwargs))
        self.kwargs = kwargs
        return self

    def filter(self, *args):
        self.calls.append(("filter", args))
        return self

    def order_by(self, *args):
        self.calls.append(("order_by", args))
        return self

    def first(self):
        self.calls.append(("first", None))
        return self.rows[0] if self.rows else None

    def all(self):
        self.calls.append(("all", None))
        return list(self.rows)


class _LookupQuery:
    def __init__(self, mapping):
        self.mapping = mapping
        self.current_key = None

    def filter_by(self, **kwargs):
        self.current_key = kwargs.get("CodArt")
        return self

    def all(self):
        return list(self.mapping.get(self.current_key, []))


class _FilterByQuery:
    def __init__(self, by_key):
        self.by_key = by_key
        self.kwargs = None
        self.calls = []

    def filter_by(self, **kwargs):
        self.kwargs = kwargs
        self.calls.append(("filter_by", kwargs))
        return self

    def order_by(self, *args):
        self.calls.append(("order_by", args))
        return self

    def filter(self, *args):
        self.calls.append(("filter", args))
        return self

    def first(self):
        return self.by_key.get(tuple(sorted((self.kwargs or {}).items())))

    def all(self):
        return list(self.by_key.get(tuple(sorted((self.kwargs or {}).items())), []))


class _ChainQuery:
    def __init__(self, first_value=None):
        self.first_value = first_value
        self.calls = []
        self.filter_kwargs = None

    def filter_by(self, **kwargs):
        self.calls.append(("filter_by", kwargs))
        self.filter_kwargs = kwargs
        return self

    def first(self):
        self.calls.append(("first", None))
        return self.first_value


class _QueryForVisible:
    def __init__(self, first_value):
        self.first_value = first_value
        self.calls = []

    def filter_by(self, **kwargs):
        self.calls.append(("filter_by", kwargs))
        return self

    def first(self):
        self.calls.append(("first", None))
        return self.first_value


class _AbortCalled(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


class _FakeErpOutbox:
    status = _Field("status")
    outbox_id = _Field("outbox_id")
    kind = _Field("kind")

    query = None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@dataclass
class _DummyRow:
    IdDocumento: str = "1"
    IdRiga: str = "10"
    RifRegistraz: str = "RIF-1"
    CodArt: str = "ART-1"
    DesArt: str = "Prodotto"
    CodReparto: str = "20"
    Quantita: str = "5"
    QtyDaLavorare: str = "5"
    StatoOrdine: str = "Pianificata"
    FaseAttiva: str = "1"
    NumFase: str = '["1", "2", "3"]'
    CodLavorazione: str = '["L1", "L2", "L3"]'
    CodRisorsaProd: str = '["R1", "R2", "R3"]'
    RisorsaAttiva: str = "RA"
    CodMagPrincipale: str = "MAG1"
    CodTipoDoc: str = "ODP"
    GestioneLotto: str = "si"
    DistintaMateriale: str = "[]"


def test_get_phase_transition_handles_last_next_and_empty_sequence(mod):
    ordine = _DummyRow(NumFase='["1", "2", "3"]')
    assert mod._get_phase_transition(ordine, "2") == (False, "3")
    assert mod._get_phase_transition(ordine, "3") == (True, None)

    ordine_senza_fasi = _DummyRow(NumFase="", FaseAttiva="")
    assert mod._get_phase_transition(ordine_senza_fasi, "1") == (True, None)


def test_set_runtime_helpers_update_runtime_state(mod):
    stato = SimpleNamespace(
        Stato_odp="", Utente_operazione="", Fase="1", data_ultima_attivazione="x"
    )

    mod._set_runtime_pianificata(stato, "gio")
    assert stato.Stato_odp == "Pianificata"
    assert stato.Utente_operazione == "gio"
    assert stato.data_ultima_attivazione is None

    mod._set_runtime_sospeso(stato, "mario", "2")
    assert stato.Stato_odp == "In Sospeso"
    assert stato.Utente_operazione == "mario"
    assert stato.Fase == "2"
    assert stato.data_ultima_attivazione is None


def test_advance_or_finalize_phase_handles_partial_same_phase(monkeypatch, mod):
    ordine = _DummyRow(FaseAttiva="2", StatoOrdine="Pianificata", QtyDaLavorare="5")
    stato = SimpleNamespace(
        Stato_odp="", Utente_operazione="", Fase="2", data_ultima_attivazione="x"
    )
    sync_calls = []

    monkeypatch.setattr(
        mod,
        "_sync_active_fields_for_phase",
        lambda ordine, fase: sync_calls.append(fase),
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="2",
        q_ok=Decimal("3"),
        q_nok=Decimal("0"),
        qty_residua=Decimal("2"),
        qty_residua_text="2",
        qty_lavorata_text="3",
        chiusura_parziale=True,
        username="gio",
    )

    assert result == {
        "tipo": "parziale_stessa_fase",
        "fase_corrente": "2",
        "fase_successiva": "2",
    }
    assert ordine.FaseAttiva == "2"
    assert ordine.StatoOrdine == "In Sospeso"
    assert ordine.QtyDaLavorare == "2"
    assert stato.Stato_odp == "In Sospeso"
    assert sync_calls == ["2"]


def test_advance_or_finalize_phase_handles_last_phase_closure(monkeypatch, mod):
    ordine = _DummyRow(
        FaseAttiva="3",
        NumFase='["1", "2", "3"]',
        StatoOrdine="Pianificata",
        QtyDaLavorare="5",
    )
    stato = SimpleNamespace(
        Stato_odp="", Utente_operazione="", Fase="3", data_ultima_attivazione="x"
    )
    sync_calls = []

    monkeypatch.setattr(
        mod,
        "_sync_active_fields_for_phase",
        lambda ordine, fase: sync_calls.append(fase),
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="3",
        q_ok=Decimal("5"),
        q_nok=Decimal("0"),
        qty_residua=Decimal("0"),
        qty_residua_text="0",
        qty_lavorata_text="5",
        chiusura_parziale=False,
        username="gio",
    )

    assert result == {
        "tipo": "finale",
        "fase_corrente": "3",
        "fase_successiva": None,
    }
    assert ordine.FaseAttiva == "3"
    assert ordine.StatoOrdine == "Chiusa"
    assert ordine.QtyDaLavorare == "0"
    assert sync_calls == ["3"]


def test_advance_or_finalize_phase_advances_to_next_phase(monkeypatch, mod):
    ordine = _DummyRow(
        FaseAttiva="1",
        NumFase='["1", "2", "3"]',
        StatoOrdine="Pianificata",
        QtyDaLavorare="5",
    )
    stato = SimpleNamespace(
        Stato_odp="", Utente_operazione="", Fase="1", data_ultima_attivazione="x"
    )
    sync_calls = []

    monkeypatch.setattr(
        mod,
        "_sync_active_fields_for_phase",
        lambda ordine, fase: sync_calls.append(fase),
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="1",
        q_ok=Decimal("4"),
        q_nok=Decimal("1"),
        qty_residua=Decimal("0"),
        qty_residua_text="0",
        qty_lavorata_text="5",
        chiusura_parziale=False,
        username="gio",
    )

    assert result == {
        "tipo": "avanzata",
        "fase_corrente": "1",
        "fase_successiva": "2",
    }
    assert ordine.FaseAttiva == "2"
    assert ordine.StatoOrdine == "Pianificata"
    assert ordine.QtyDaLavorare == "4"
    assert stato.Stato_odp == "Pianificata"
    assert stato.Utente_operazione == "gio"
    assert sync_calls == ["2"]


def test_queue_phase_export_adds_outbox_and_flushes(monkeypatch, mod):
    fake_session = _FakeSession()
    monkeypatch.setattr(mod, "ErpOutbox", _FakeErpOutbox)
    monkeypatch.setattr(mod, "db", SimpleNamespace(session=fake_session))

    ordine = _DummyRow(
        IdDocumento="55",
        IdRiga="7",
        RifRegistraz="RIF55",
        CodArt="ART55",
        CodReparto='["20"]',
    )
    payload = {"fase": "2", "quantita_ok": "5"}

    outbox = mod._queue_phase_export(ordine, "2", payload)

    assert fake_session.flushed == 1
    assert fake_session.added == [outbox]
    assert outbox.kind == "consuntivo_fase"
    assert outbox.status == "pending"
    assert outbox.IdDocumento == "55"
    assert outbox.IdRiga == "7"
    assert outbox.Fase == "2"
    assert outbox.CodReparto == '["20"]'
    assert json.loads(outbox.payload_json) == payload


def test_get_blocking_outbox_for_phase_returns_latest_pending_or_error(
    monkeypatch, mod
):
    expected = SimpleNamespace(outbox_id=99)
    fake_query = _FakeQuery(rows=[expected])
    fake_cls = type(
        "FakeErpOutboxModel",
        (),
        {
            "query": fake_query,
            "status": _Field("status"),
            "outbox_id": _Field("outbox_id"),
        },
    )
    monkeypatch.setattr(mod, "ErpOutbox", fake_cls)

    outbox = mod._get_blocking_outbox_for_phase("1", "10", "2")

    assert outbox is expected
    assert fake_query.calls[0] == (
        "filter_by",
        {"IdDocumento": "1", "IdRiga": "10", "Fase": "2"},
    )
    assert fake_query.calls[1][0] == "filter"
    assert fake_query.calls[2][0] == "order_by"


def test_componenti_lotto_per_ordine_filters_phase_duplicates_and_positive_stock(
    monkeypatch, mod
):
    distinta = json.dumps(
        [
            {
                "CodArt": "C1",
                "DesArt": "Comp 1",
                "NumFase": 2,
                "GestioneLotto": "si",
                "Quantita": 3,
            },
            {
                "CodArt": "C1",
                "DesArt": "Comp 1 dup",
                "NumFase": 2,
                "GestioneLotto": "si",
                "Quantita": 3,
            },
            {
                "CodArt": "C2",
                "DesArt": "Comp 2",
                "NumFase": 1,
                "GestioneLotto": "si",
                "Quantita": 4,
            },
            {
                "CodArt": "C3",
                "DesArt": "Comp 3",
                "NumFase": 2,
                "GestioneLotto": "no",
                "Quantita": 1,
            },
        ]
    )
    ordine = _DummyRow(GestioneLotto="si", FaseAttiva="2", DistintaMateriale=distinta)

    lots_by_code = {
        "C1": [
            SimpleNamespace(RifLottoAlfa="L1", Giacenza="5", CodMag="M1"),
            SimpleNamespace(RifLottoAlfa="L2", Giacenza="0", CodMag="M2"),
        ]
    }
    fake_cls = type("FakeGiacenza", (), {"query": _LookupQuery(lots_by_code)})
    monkeypatch.setattr(mod, "GiacenzaLotti", fake_cls)

    rows = mod._componenti_lotto_per_ordine(ordine)

    assert rows == [
        {
            "CodArt": "C1",
            "DesArt": "Comp 1",
            "Quantita": 3,
            "NumFase": 2,
            "GestioneLotto": "si",
            "lotti": [{"RifLottoAlfa": "L1", "Giacenza": 5, "CodMag": "M1"}],
        }
    ]


def test_componenti_lotto_per_ordine_include_senza_lotti_keeps_empty_components(
    monkeypatch, mod
):
    distinta = json.dumps(
        [
            {
                "CodArt": "C9",
                "DesArt": "Comp 9",
                "NumFase": 1,
                "GestioneLotto": "si",
                "Quantita": 2,
            }
        ]
    )
    ordine = _DummyRow(GestioneLotto="si", FaseAttiva="1", DistintaMateriale=distinta)
    fake_cls = type("FakeGiacenza", (), {"query": _LookupQuery({"C9": []})})
    monkeypatch.setattr(mod, "GiacenzaLotti", fake_cls)

    rows = mod._componenti_lotto_per_ordine(ordine, include_senza_lotti=True)

    assert rows == [
        {
            "CodArt": "C9",
            "DesArt": "Comp 9",
            "Quantita": 2,
            "NumFase": 1,
            "GestioneLotto": "si",
            "lotti": [],
        }
    ]


def test_pick_resource_code_prefers_phase_then_active_then_raw(mod):
    row_phase = _DummyRow(
        CodRisorsaProd='["R1", "R2"]', NumFase='["1", "2"]', RisorsaAttiva="RA"
    )
    assert mod._pick_resource_code(row_phase, "2") == "R2"

    row_active = _DummyRow(CodRisorsaProd="", NumFase='["1"]', RisorsaAttiva="RA")
    assert mod._pick_resource_code(row_active, "1") == "RA"

    row_raw = _DummyRow(CodRisorsaProd="RIS-SINGOLA", NumFase="", RisorsaAttiva="")
    assert mod._pick_resource_code(row_raw, "1") == "RIS-SINGOLA"
    assert mod._pick_resource_code(None, "1") == ""


def test_pick_magazzino_principale_and_tipo_documento_use_fallbacks(mod):
    cfg = {"rig_magazzino_principale": "MAG-FALLBACK", "tes_tipo_documento": 704}

    row = _DummyRow(CodMagPrincipale="MAG1", CodTipoDoc="ODP")
    assert mod._pick_magazzino_principale(row, cfg) == "MAG1"
    assert mod._pick_tipo_documento(row, cfg) == "ODP"

    row_blank = _DummyRow(CodMagPrincipale="", CodTipoDoc="")
    assert mod._pick_magazzino_principale(row_blank, cfg) == "MAG-FALLBACK"
    assert mod._pick_tipo_documento(row_blank, cfg) == 704
    assert mod._pick_magazzino_principale(None, cfg) == "MAG-FALLBACK"


def test_get_pending_avp_outbox_filters_pending_consuntivo_fase(monkeypatch, mod):
    rows = [SimpleNamespace(outbox_id=1), SimpleNamespace(outbox_id=2)]
    fake_query = _FakeQuery(rows=rows)
    fake_cls = type(
        "FakeErpOutboxModel",
        (),
        {
            "query": fake_query,
            "kind": _Field("kind"),
            "status": _Field("status"),
            "outbox_id": _Field("outbox_id"),
        },
    )
    monkeypatch.setattr(mod, "ErpOutbox", fake_cls)

    result = mod._get_pending_avp_outbox()

    assert result == rows
    assert fake_query.calls[0][0] == "filter"
    assert fake_query.calls[1] == ("order_by", (("asc", "outbox_id"),))


def test_get_outbox_payload_handles_valid_invalid_and_non_dict_payloads(mod):
    assert mod._get_outbox_payload(SimpleNamespace(payload_json='{"a": 1}')) == {"a": 1}
    assert mod._get_outbox_payload(SimpleNamespace(payload_json="[1, 2, 3]")) == {}
    assert mod._get_outbox_payload(SimpleNamespace(payload_json='{"a":')) == {}
    assert mod._get_outbox_payload(SimpleNamespace(payload_json=None)) == {}


def test_get_export_source_row_prefers_current_row_then_log_fallback(monkeypatch, mod):
    current_row = SimpleNamespace(IdDocumento="1", IdRiga="10")
    log_row = SimpleNamespace(log_id=9, IdDocumento="1", IdRiga="10")

    input_q = _FilterByQuery({(("IdDocumento", "1"), ("IdRiga", "10")): current_row})
    log_q = _FilterByQuery({(("IdDocumento", "1"), ("IdRiga", "10")): log_row})

    monkeypatch.setattr(mod, "InputOdp", type("InputOdpFake", (), {"query": input_q}))
    monkeypatch.setattr(
        mod,
        "InputOdpLog",
        type("InputOdpLogFake", (), {"query": log_q, "log_id": _Field("log_id")}),
    )

    outbox = SimpleNamespace(IdDocumento="1", IdRiga="10")
    assert mod._get_export_source_row(outbox) is current_row

    input_q_empty = _FilterByQuery({})
    monkeypatch.setattr(
        mod, "InputOdp", type("InputOdpFake2", (), {"query": input_q_empty})
    )
    assert mod._get_export_source_row(outbox) is log_row


def test_get_erp_export_dir_and_erp_avp_cfg_use_app_config_and_fallback(tmp_path, mod):
    app = Flask(__name__)
    app.config["ERP_EXPORT_DIR"] = str(tmp_path / "exports_cfg")
    app.config["ERP_AVP_DEFAULTS"] = {
        "qta_mode": "worked",
        "rig_magazzino_principale": "M99",
    }

    with app.app_context():
        export_dir = mod._get_erp_export_dir()
        cfg = mod._erp_avp_cfg()

    assert export_dir == Path(app.config["ERP_EXPORT_DIR"])
    assert export_dir.exists()
    assert cfg["qta_mode"] == "worked"
    assert cfg["rig_magazzino_principale"] == "M99"
    assert cfg["tes_tipo_documento"] == mod.AVP_DEFAULTS["tes_tipo_documento"]

    app2 = Flask(__name__, instance_path=str(tmp_path / "instance_fallback"))
    with app2.app_context():
        export_dir2 = mod._get_erp_export_dir()
    assert export_dir2 == Path(app2.instance_path) / "erp_exports"
    assert export_dir2.exists()


def test_build_tes_row_and_build_rig_row_cover_core_export_fields(mod):
    cfg = {
        "tes_numero_registrazione": 123,
        "tes_appendice": "APP",
        "rig_tipo_op_qta": 702,
        "rig_causale_prestazione": 0,
        "rig_magazzino_principale": "MAG-F",
        "qta_mode": "worked",
        "rif90_mode": "riga_fase",
    }
    source_row = _DummyRow(
        CodMagPrincipale="MAG1",
        CodTipoDoc="ODP",
        CodRisorsaProd='["R1", "R2"]',
        NumFase='["1", "2"]',
    )
    payload = {
        "created_at": "2024-01-02T13:45:56+01:00",
        "quantita_ok": "2",
        "quantita_ko": "1",
        "tempo_funzionamento": "1.5",
        "rif_registraz": "RIF1",
        "id_documento": "10",
        "id_riga": "2",
        "fase": "2",
        "cod_art": "ART10",
    }

    tes = mod._build_tes_row(payload, source_row, cfg)
    rig = mod._build_rig_row(payload, source_row, cfg)

    assert tes[0] == "TES"
    assert tes[1] == "ODP"
    assert tes[3] == 123
    assert tes[4] == "APP"

    assert rig[0] == "RIG"
    assert rig[1] == "ODP"
    assert rig[3] == 123
    assert rig[4] == "APP"
    assert rig[5] == 702
    assert rig[6] == "RIF1.2.2"
    assert rig[7] == "ART10"
    assert rig[8] == "3,00"
    assert rig[9] == "MAG1"
    assert rig[10] == "R2"
    assert rig[12] == "1,500"

    assert (
        mod._build_rig_row(
            {"quantita_ok": "0", "quantita_ko": "0", "tempo_funzionamento": "0"},
            source_row,
            cfg,
        )
        is None
    )


def test_build_avp_txt_content_builds_header_and_rows(monkeypatch, mod):
    outbox_rows = [SimpleNamespace(outbox_id=1), SimpleNamespace(outbox_id=2)]
    monkeypatch.setattr(
        mod, "_erp_avp_cfg", lambda: {**mod.AVP_DEFAULTS, "include_header": True}
    )
    monkeypatch.setattr(
        mod, "_get_outbox_payload", lambda outbox: {"id": outbox.outbox_id}
    )
    monkeypatch.setattr(mod, "_get_export_source_row", lambda outbox: _DummyRow())

    def fake_build_rig_row(payload, source_row, cfg):
        if payload["id"] == 1:
            return [
                "RIG",
                704,
                "02/01/2024 13:00:00",
                0,
                "",
                702,
                "RIF",
                "ART",
                "1,00",
                "MAG",
                "RIS",
                0,
                "0,500",
            ]
        return None

    monkeypatch.setattr(mod, "_build_rig_row", fake_build_rig_row)

    content = mod._build_avp_txt_content(outbox_rows)
    lines = content.splitlines()

    assert lines[0].startswith('"TipoRecord";')
    assert lines[1].startswith('"RIG";704;')
    assert len(lines) == 2


def test_last_change_event_id_returns_scalar_or_zero(monkeypatch, mod):
    fake_session = _FakeSession()
    monkeypatch.setattr(mod, "db", SimpleNamespace(session=fake_session))
    monkeypatch.setattr(
        mod, "ChangeEvent", type("ChangeEventFake", (), {"id": _Field("id")})
    )
    monkeypatch.setattr(mod, "func", SimpleNamespace(max=lambda x: ("max", x)))

    fake_session.scalar_value = 7
    assert mod._last_change_event_id() == 7

    fake_session.scalar_value = None
    assert mod._last_change_event_id() == 0


def test_get_visible_odp_by_key_returns_visible_or_aborts_403_404(monkeypatch, mod):
    visible = SimpleNamespace(IdDocumento="1", IdRiga="10")

    policy = SimpleNamespace(filter_input_odp=lambda q: _QueryForVisible(visible))
    monkeypatch.setattr(mod, "_base_odp_query", lambda: _QueryForVisible(None))
    monkeypatch.setattr(
        mod, "abort", lambda code: (_ for _ in ()).throw(_AbortCalled(code))
    )

    assert mod._get_visible_odp_by_key(policy, "1", "10") is visible

    policy_hidden = SimpleNamespace(filter_input_odp=lambda q: _QueryForVisible(None))
    monkeypatch.setattr(
        mod,
        "_base_odp_query",
        lambda: _QueryForVisible(SimpleNamespace(IdDocumento="1", IdRiga="10")),
    )
    with pytest.raises(_AbortCalled) as exc_403:
        mod._get_visible_odp_by_key(policy_hidden, "1", "10")
    assert exc_403.value.code == 403

    monkeypatch.setattr(mod, "_base_odp_query", lambda: _QueryForVisible(None))
    with pytest.raises(_AbortCalled) as exc_404:
        mod._get_visible_odp_by_key(policy_hidden, "1", "10")
    assert exc_404.value.code == 404


def test_tab_from_ordine_and_fragments_for_ordine_tab(monkeypatch, mod):
    ordine = _DummyRow(CodReparto='["20"]')
    assert mod._tab_from_ordine(ordine) == "officina"
    assert mod._tab_from_ordine(_DummyRow(CodReparto='["999"]')) is None

    fake_query = SimpleNamespace(all=lambda: [ordine])
    monkeypatch.setattr(mod, "_query_for_tab", lambda policy, reparto_code: fake_query)
    monkeypatch.setattr(
        mod,
        "RENDERERS",
        {**mod.RENDERERS, "officina": lambda odp: {"tbody": f"{len(odp)} rows"}},
    )

    tab, fragments = mod._fragments_for_ordine_tab(SimpleNamespace(), ordine)
    assert tab == "officina"
    assert fragments == {"tbody": "1 rows"}

    tab_none, fragments_none = mod._fragments_for_ordine_tab(
        SimpleNamespace(), _DummyRow(CodReparto="")
    )
    assert tab_none is None
    assert fragments_none == {}


import pytest

MODULE_PATH = "app_odp.routes"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


def test_get_phase_transition_handles_empty_missing_and_next_phase(mod):
    ordine_senza_fasi = SimpleNamespace(NumFase="", FaseAttiva="")
    assert mod._get_phase_transition(ordine_senza_fasi, "1") == (True, None)

    ordine = SimpleNamespace(NumFase='["1", "2", "3"]', FaseAttiva="2")
    assert mod._get_phase_transition(ordine, "9") == (True, None)
    assert mod._get_phase_transition(ordine, "2") == (False, "3")
    assert mod._get_phase_transition(ordine, "3") == (True, None)


def test_set_runtime_helpers_update_state_fields(mod):
    stato = SimpleNamespace(
        Stato_odp="In corso",
        Utente_operazione="old",
        Fase="1",
        data_ultima_attivazione="2024-01-01T10:00:00",
    )

    mod._set_runtime_pianificata(stato, "gio")
    assert stato.Stato_odp == "Pianificata"
    assert stato.Utente_operazione == "gio"
    assert stato.data_ultima_attivazione is None
    assert stato.Fase == "1"

    mod._set_runtime_sospeso(stato, "anna", "3")
    assert stato.Stato_odp == "In Sospeso"
    assert stato.Utente_operazione == "anna"
    assert stato.Fase == "3"
    assert stato.data_ultima_attivazione is None

    # branch stato is None
    mod._set_runtime_pianificata(None, "x")
    mod._set_runtime_sospeso(None, "x", "1")


def test_advance_or_finalize_phase_partial_keeps_same_phase_and_suspends_runtime(
    monkeypatch,
    mod,
):
    ordine = SimpleNamespace(
        NumFase='["1", "2", "3"]',
        FaseAttiva="2",
        StatoOrdine="In corso",
        QtyDaLavorare="10",
        CodLavorazione='["L1", "L2", "L3"]',
        CodRisorsaProd='["R1", "R2", "R3"]',
        LavorazioneAttiva="",
        RisorsaAttiva="",
    )
    stato = SimpleNamespace(
        Stato_odp="In corso",
        Utente_operazione="old",
        Fase="2",
        data_ultima_attivazione="2024-01-01",
    )

    synced = []
    monkeypatch.setattr(
        mod, "_sync_active_fields_for_phase", lambda o, f: synced.append((o, f))
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="2",
        q_ok=Decimal("4"),
        q_nok=Decimal("1"),
        qty_residua=Decimal("5"),
        qty_residua_text="5",
        qty_lavorata_text="5",
        chiusura_parziale=True,
        username="gio",
    )

    assert result == {
        "tipo": "parziale_stessa_fase",
        "fase_corrente": "2",
        "fase_successiva": "2",
    }
    assert ordine.FaseAttiva == "2"
    assert ordine.StatoOrdine == "In Sospeso"
    assert ordine.QtyDaLavorare == "5"
    assert synced == [(ordine, "2")]
    assert stato.Stato_odp == "In Sospeso"
    assert stato.Utente_operazione == "gio"
    assert stato.Fase == "2"
    assert stato.data_ultima_attivazione is None


def test_advance_or_finalize_phase_final_closes_order(monkeypatch, mod):
    ordine = SimpleNamespace(
        NumFase='["1", "2"]',
        FaseAttiva="2",
        StatoOrdine="In corso",
        QtyDaLavorare="3",
        CodLavorazione='["L1", "L2"]',
        CodRisorsaProd='["R1", "R2"]',
        LavorazioneAttiva="",
        RisorsaAttiva="",
    )
    stato = SimpleNamespace(
        Stato_odp="In corso",
        Utente_operazione="old",
        Fase="2",
        data_ultima_attivazione="2024-01-01",
    )

    synced = []
    monkeypatch.setattr(
        mod, "_sync_active_fields_for_phase", lambda o, f: synced.append((o, f))
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="2",
        q_ok=Decimal("3"),
        q_nok=Decimal("0"),
        qty_residua=Decimal("0"),
        qty_residua_text="0",
        qty_lavorata_text="3",
        chiusura_parziale=False,
        username="gio",
    )

    assert result == {
        "tipo": "finale",
        "fase_corrente": "2",
        "fase_successiva": None,
    }
    assert ordine.FaseAttiva == "2"
    assert ordine.StatoOrdine == "Chiusa"
    assert ordine.QtyDaLavorare == "0"
    assert synced == [(ordine, "2")]
    # nel ramo finale il runtime non viene modificato dagli helper runtime
    assert stato.Stato_odp == "In corso"
    assert stato.Utente_operazione == "old"


def test_advance_or_finalize_phase_moves_to_next_phase_and_plans_runtime(
    monkeypatch,
    mod,
):
    ordine = SimpleNamespace(
        NumFase='["1", "2", "3"]',
        FaseAttiva="2",
        StatoOrdine="In corso",
        QtyDaLavorare="9",
        CodLavorazione='["L1", "L2", "L3"]',
        CodRisorsaProd='["R1", "R2", "R3"]',
        LavorazioneAttiva="",
        RisorsaAttiva="",
    )
    stato = SimpleNamespace(
        Stato_odp="In corso",
        Utente_operazione="old",
        Fase="2",
        data_ultima_attivazione="2024-01-01",
    )

    synced = []
    monkeypatch.setattr(
        mod, "_sync_active_fields_for_phase", lambda o, f: synced.append((o, f))
    )

    result = mod._advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente="2",
        q_ok=Decimal("4.5"),
        q_nok=Decimal("0.5"),
        qty_residua=Decimal("0"),
        qty_residua_text="0",
        qty_lavorata_text="5",
        chiusura_parziale=False,
        username="gio",
    )

    assert result == {
        "tipo": "avanzata",
        "fase_corrente": "2",
        "fase_successiva": "3",
    }
    assert ordine.FaseAttiva == "3"
    assert ordine.StatoOrdine == "Pianificata"
    assert ordine.QtyDaLavorare == "4.5"
    assert synced == [(ordine, "3")]
    assert stato.Stato_odp == "Pianificata"
    assert stato.Utente_operazione == "gio"
    assert stato.data_ultima_attivazione is None


def test_queue_phase_export_builds_outbox_and_flushes_session(monkeypatch, mod):
    created = {}

    class FakeOutbox:
        def __init__(self, **kwargs):
            created.update(kwargs)
            self.__dict__.update(kwargs)

    calls = []
    fake_session = SimpleNamespace(
        add=lambda obj: calls.append(("add", obj)),
        flush=lambda: calls.append(("flush", None)),
    )

    monkeypatch.setattr(mod, "ErpOutbox", FakeOutbox)
    monkeypatch.setattr(mod.db, "session", fake_session)

    ordine = SimpleNamespace(
        IdDocumento="100",
        IdRiga="10",
        RifRegistraz="RIF100",
        CodArt="ART100",
        CodReparto="20",
    )

    outbox = mod._queue_phase_export(
        ordine=ordine,
        fase_corrente="2",
        payload={"kind": "consuntivo_fase", "fase": "2"},
    )

    assert created["kind"] == "consuntivo_fase"
    assert created["status"] == "pending"
    assert created["IdDocumento"] == "100"
    assert created["IdRiga"] == "10"
    assert created["RifRegistraz"] == "RIF100"
    assert created["CodArt"] == "ART100"
    assert created["Fase"] == "2"
    assert created["CodReparto"] == "20"
    assert '"fase": "2"' in created["payload_json"]
    assert calls[0] == ("add", outbox)
    assert calls[1] == ("flush", None)


def test_get_blocking_outbox_for_phase_returns_none_for_blank_phase(mod):
    assert mod._get_blocking_outbox_for_phase("100", "10", "") is None
    assert mod._get_blocking_outbox_for_phase("100", "10", None) is None


def test_get_blocking_outbox_for_phase_builds_query_chain(monkeypatch, mod):
    captured = {}
    result = object()

    class FakeColumn:
        def in_(self, values):
            captured["status_values"] = list(values)
            return ("status_in", tuple(values))

        def desc(self):
            captured["desc_called"] = True
            return "desc(status)"

    class FakeQuery:
        def filter_by(self, **kwargs):
            captured["filter_by"] = kwargs
            return self

        def filter(self, expr):
            captured["filter"] = expr
            return self

        def order_by(self, expr):
            captured["order_by"] = expr
            return self

        def first(self):
            captured["first_called"] = True
            return result

    fake_model = SimpleNamespace(
        query=FakeQuery(),
        status=FakeColumn(),
        outbox_id=FakeColumn(),
    )
    monkeypatch.setattr(mod, "ErpOutbox", fake_model)

    got = mod._get_blocking_outbox_for_phase("100", "10", "2")

    assert got is result
    assert captured["filter_by"] == {
        "IdDocumento": "100",
        "IdRiga": "10",
        "Fase": "2",
    }
    assert captured["status_values"] == ["pending", "error"]
    assert captured["desc_called"] is True
    assert captured["first_called"] is True


import pytest
from flask_login import LoginManager, UserMixin

MODULE_PATH = "app_odp.routes"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


class _User(UserMixin):
    def __init__(self, user_id="1", username="tester"):
        self.id = user_id
        self.username = username


class FakeSession:
    def __init__(self):
        self.added = []
        self.deleted = []
        self.flush_count = 0
        self.commit_count = 0
        self.rollback_count = 0

    def add(self, obj):
        self.added.append(obj)

    def delete(self, obj):
        self.deleted.append(obj)

    def flush(self):
        self.flush_count += 1

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class FakeQuery:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def filter_by(self, **kwargs):
        filtered = [
            row
            for row in self._rows
            if all(getattr(row, key, None) == value for key, value in kwargs.items())
        ]
        return FakeQuery(filtered)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalar(self):
        return self._rows[0] if self._rows else None


class DummyExpr:
    def isnot(self, other):
        return self

    def __eq__(self, other):
        return self


class FakePolicy:
    def __init__(self, user):
        self.user = user

    def can(self, code):
        return True


@pytest.fixture()
def app(mod, monkeypatch):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY="test-secret",
        DIMENSIONI=[50, 80],
        DPI=300,
        FONT_PATH="fake-font.ttf",
    )

    login_manager = LoginManager(app)
    user = _User()

    @login_manager.request_loader
    def _load_user_from_request(request):
        return user

    monkeypatch.setattr(mod, "RbacPolicy", FakePolicy)
    app.register_blueprint(mod.main_bp)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def fake_db(mod, monkeypatch):
    session = FakeSession()
    monkeypatch.setattr(mod, "db", SimpleNamespace(session=session))
    return session


@pytest.fixture()
def common_api_patches(monkeypatch, mod):
    from app_odp.policy import decorator as policy_decorator

    class FakePolicy:
        def __init__(self, user):
            self.user = user

        def can(self, code):
            return True

        def filter_input_odp(self, q):
            return q

        def filter_input_odp_for_reparto(self, q, reparto_code):
            return q

    monkeypatch.setattr(policy_decorator, "RbacPolicy", FakePolicy)
    monkeypatch.setattr(mod, "RbacPolicy", FakePolicy, raising=False)

    monkeypatch.setattr(
        mod,
        "current_user",
        SimpleNamespace(is_authenticated=True, id=1, username="tester"),
        raising=False,
    )

    monkeypatch.setattr(
        mod, "_fragments_for_ordine_tab", lambda policy, ordine: ("", {})
    )
    monkeypatch.setattr(mod, "_get_blocking_outbox_for_phase", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_last_change_event_id", lambda: 0)


@pytest.fixture()
def light_factories(mod, monkeypatch):
    factory = lambda **kwargs: SimpleNamespace(**kwargs)
    monkeypatch.setattr(mod, "InputOdpLog", factory)
    monkeypatch.setattr(mod, "StatoOdpLog", factory)
    monkeypatch.setattr(mod, "LottiUsatiLog", factory)
    monkeypatch.setattr(mod, "LottiGeneratiLog", factory)
    monkeypatch.setattr(mod, "ChangeEventLog", factory)


@pytest.fixture()
def empty_change_events(mod, monkeypatch):
    ce_type = type(
        "FakeChangeEvent",
        (),
        {
            "payload_json": DummyExpr(),
            "id": DummyExpr(),
            "query": FakeQuery([]),
        },
    )
    monkeypatch.setattr(mod, "ChangeEvent", ce_type)


def make_ordine(**overrides):
    data = {
        "IdDocumento": "100",
        "IdRiga": "1",
        "RifRegistraz": "RIF-100",
        "CodArt": "ART-100",
        "DesArt": "Ordine test",
        "Quantita": "10",
        "QtyDaLavorare": "10",
        "StatoOrdine": "Pianificata",
        "CodReparto": "20",
        "NumFase": '["1"]',
        "FaseAttiva": "1",
        "CodLavorazione": '["LAV1"]',
        "CodRisorsaProd": '["RIS1"]',
        "GestioneLotto": "no",
        "GestioneMatricola": "no",
        "DistintaMateriale": "[]",
        "CodMatricola": "",
        "StatoRiga": "A",
        "CodFamiglia": "F1",
        "CodMacrofamiglia": "MF1",
        "CodMagPrincipale": "MAG1",
        "TempoPrevistoLavoraz": "60",
        "CodClassifTecnica": "CT1",
        "CodTipoDoc": "ODP",
        "DataInizioSched": "2024-01-01",
        "DataFineSched": "2024-01-02",
        "Note": "",
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_api_prendi_ordine_requires_ids(client, common_api_patches):
    resp = client.post("/api/ordini/presa", json={})

    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False
    assert "IdDocumento e IdRiga" in resp.get_json()["error"]


def test_api_prendi_ordine_returns_409_when_blocked_by_outbox(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Pianificata")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_get_blocking_outbox_for_phase",
        lambda **kwargs: SimpleNamespace(status="pending", outbox_id=321),
    )

    resp = client.post(
        "/api/ordini/presa",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["changed"] is False
    assert payload["outbox_status"] == "pending"
    assert payload["outbox_id"] == 321
    assert payload["active_tab"] == ""


def test_api_prendi_ordine_activates_pianificata_order(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Pianificata")
    stato = SimpleNamespace(
        data_ultima_attivazione="2024-01-02T13:45:56",
        Tempo_funzionamento="0",
    )
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([])}),
    )
    monkeypatch.setattr(
        mod,
        "_ensure_stato_attivo",
        lambda **kwargs: stato,
    )
    monkeypatch.setattr(
        mod,
        "_push_change_event",
        lambda **kwargs: pushed.append(kwargs),
    )

    resp = client.post(
        "/api/ordini/presa",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine preso in carico"
    assert payload["stato_ordine"] == "Attivo"
    assert ordine.StatoOrdine == "Attivo"
    assert fake_db.commit_count == 1
    assert pushed and pushed[0]["topic"] == "ordine_preso"


def test_api_sospendi_ordine_returns_409_when_runtime_row_is_missing(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/sospendi",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert "odp_in_carico" in payload["error"]
    assert fake_db.rollback_count == 1


def test_api_sospendi_ordine_accumulates_runtime_and_commits(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        Stato_odp="Attivo",
        Utente_operazione="",
        Tempo_funzionamento="15",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "75"
        stato_obj.data_ultima_attivazione = None
        return 60

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(
        mod, "_push_change_event", lambda **kwargs: pushed.append(kwargs)
    )

    resp = client.post(
        "/api/ordini/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine sospeso"
    assert payload["tempo_funzionamento"] == "75"
    assert payload["elapsed_seconds"] == 60
    assert ordine.StatoOrdine == "In Sospeso"
    assert stato.Stato_odp == "In Sospeso"
    assert fake_db.commit_count == 1
    assert pushed and pushed[0]["topic"] == "ordine_sospeso"


def test_api_riattiva_ordine_returns_409_when_blocked_by_outbox(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_get_blocking_outbox_for_phase",
        lambda **kwargs: SimpleNamespace(status="error", outbox_id=99),
    )

    resp = client.post(
        "/api/ordini/riattiva",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["changed"] is False
    assert payload["outbox_status"] == "error"
    assert payload["outbox_id"] == 99


def test_api_riattiva_ordine_reactivates_suspended_order(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        data_ultima_attivazione="2024-01-02T13:45:56",
        Tempo_funzionamento="75",
    )
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_ensure_stato_attivo", lambda **kwargs: stato)
    monkeypatch.setattr(
        mod, "_push_change_event", lambda **kwargs: pushed.append(kwargs)
    )

    resp = client.post(
        "/api/ordini/riattiva",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine riattivato"
    assert payload["stato_ordine"] == "Attivo"
    assert ordine.StatoOrdine == "Attivo"
    assert fake_db.commit_count == 1
    assert pushed and pushed[0]["topic"] == "ordine_riattivato"


def test_api_chiudi_ordine_rejects_pianificata_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Pianificata")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert "ancora Pianificata" in payload["error"]


def test_api_chiudi_ordine_rejects_invalid_partial_quantities(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "10",
            "quantita_non_conforme": "0",
            "chiusura_parziale": True,
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "strettamente minore" in payload["error"]


def test_api_chiudi_ordine_requires_component_lots_when_needed(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo")
    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda *a, **k: [{"CodArt": "CMP-1"}],
    )

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "5",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "assegnazione dei lotti materiale" in payload["error"]


def test_api_chiudi_ordine_final_success_writes_outbox_logs_and_response(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    pushed = []
    outbox = SimpleNamespace(outbox_id=555, status="pending")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "120"
        stato_obj.data_ultima_attivazione = None
        return 120

    def fake_queue_export(ordine, fase_corrente, payload):
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "Chiusa"
        kwargs["ordine"].QtyDaLavorare = "0"
        kwargs["ordine"].FaseAttiva = kwargs["fase_corrente"]
        return {
            "tipo": "finale",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": None,
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(
        mod, "_push_change_event", lambda **kwargs: pushed.append(kwargs)
    )
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "10",
            "quantita_non_conforme": "0",
            "note": "chiusura test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine chiuso definitivamente"
    assert payload["stato_ordine"] == "Chiusa"
    assert payload["chiusura_parziale"] is False
    assert payload["outbox_id"] == 555
    assert payload["outbox_status"] == "pending"
    assert ordine.StatoOrdine == "Chiusa"
    assert ordine.QtyDaLavorare == "0"
    assert fake_db.flush_count == 1
    assert fake_db.commit_count == 1
    assert fake_db.deleted == [stato]
    assert len(fake_db.added) >= 2
    assert pushed and pushed[0]["topic"] == "fase_consuntivata"


import pytest

MODULE_PATH = "app_odp.routes"


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


class HtmlFakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)


class FakeScalarResult:
    def __init__(self, values):
        self._values = list(values)

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


@pytest.fixture()
def html_env(monkeypatch, mod):
    from app_odp.policy import decorator as policy_decorator

    state = {
        "allowed_reparti": {"20"},
        "allowed_reparti_menu": [("20", "Officina")],
        "perm_map": {"home": True},
    }

    class FakePolicy:
        def __init__(self, user):
            self.user = user

        @property
        def allowed_reparti(self):
            return set(state["allowed_reparti"])

        @property
        def allowed_reparti_menu(self):
            return list(state["allowed_reparti_menu"])

        def can(self, code):
            return bool(state["perm_map"].get(code, False))

        def filter_input_odp(self, q):
            return q

        def filter_input_odp_for_reparto(self, q, reparto_code):
            return q

    def set_policy(*, allowed_reparti=None, allowed_reparti_menu=None, perm_map=None):
        if allowed_reparti is not None:
            state["allowed_reparti"] = set(allowed_reparti)
        if allowed_reparti_menu is not None:
            state["allowed_reparti_menu"] = list(allowed_reparti_menu)
        if perm_map is not None:
            state["perm_map"] = dict(perm_map)

    monkeypatch.setattr(policy_decorator, "RbacPolicy", FakePolicy)
    monkeypatch.setattr(mod, "RbacPolicy", FakePolicy, raising=False)
    monkeypatch.setattr(
        mod,
        "current_user",
        SimpleNamespace(is_authenticated=True, id=1, username="tester"),
        raising=False,
    )
    monkeypatch.setattr(mod, "_last_change_event_id", lambda: 7, raising=False)
    monkeypatch.setattr(
        mod, "_tab_scoped_odp", lambda policy, reparto_code: HtmlFakeQuery([])
    )
    monkeypatch.setattr(
        mod, "_query_for_tab", lambda policy, reparto_code: HtmlFakeQuery([])
    )
    monkeypatch.setattr(
        mod,
        "db",
        SimpleNamespace(
            session=SimpleNamespace(
                execute=lambda stmt: FakeScalarResult(["PAUSA", "SETUP"])
            )
        ),
        raising=False,
    )

    app = Flask("test_routes_html")
    app.config.update(TESTING=True, SECRET_KEY="test", LOGIN_DISABLED=True)
    app.register_blueprint(mod.main_bp)

    return {"app": app, "set_policy": set_policy, "state": state}


@pytest.fixture()
def html_client(html_env):
    return html_env["app"].test_client()


def test_inject_policy_and_nav_returns_empty_for_anonymous_user(
    mod, html_env, monkeypatch
):
    monkeypatch.setattr(
        mod,
        "current_user",
        SimpleNamespace(is_authenticated=False),
        raising=False,
    )

    with html_env["app"].test_request_context("/"):
        result = mod.inject_policy_and_nav()

    assert result == {}


def test_inject_policy_and_nav_builds_menu_from_allowed_reparti(
    mod, html_env, monkeypatch
):
    html_env["set_policy"](
        allowed_reparti={"20", "10", "999"},
        allowed_reparti_menu=[
            ("20", "Officina"),
            ("10", "Montaggio"),
            ("999", "Ignorato"),
        ],
        perm_map={"home": True},
    )
    monkeypatch.setattr(mod, "url_for", lambda endpoint, tab: f"/?tab={tab}")

    with html_env["app"].test_request_context("/"):
        result = mod.inject_policy_and_nav()

    assert "policy" in result
    assert result["home_switch_items"] == [
        {"label": "Officina", "url": "/?tab=officina", "tab": "officina"},
        {"label": "Montaggio", "url": "/?tab=montaggio", "tab": "montaggio"},
    ]


def test_home_defaults_to_first_allowed_tab_and_renders_context(
    mod, html_client, html_env, monkeypatch
):
    html_env["set_policy"](
        allowed_reparti={"20", "10"},
        allowed_reparti_menu=[("20", "Officina"), ("10", "Montaggio")],
        perm_map={"home": True},
    )

    captured = {}

    def fake_render(template_name, **ctx):
        captured["template_name"] = template_name
        captured["ctx"] = ctx
        return "OK-HOME"

    monkeypatch.setattr(mod, "render_template", fake_render)
    (
        monkeypatch.setattr(
            mod,
            "_tab_scoped_odp",
            lambda policy, reparto_code: HtmlFakeQuery(
                [SimpleNamespace(IdDocumento="1", IdRiga="10", CodReparto=reparto_code)]
            ),
        ),
    )

    resp = html_client.get("/")

    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == "OK-HOME"
    assert captured["template_name"] == "home.j2"
    assert captured["ctx"]["active_partial"] == "partials/_home_montaggio.j2"
    assert captured["ctx"]["active_tab"] == "montaggio"
    assert captured["ctx"]["causali_attivita"] == ["PAUSA", "SETUP"]
    assert captured["ctx"]["bridge_url"].endswith("/api/home/montaggio/bridge")
    assert captured["ctx"]["bridge_last_event_id"] == 7
    assert len(captured["ctx"]["odp"]) == 1


def test_home_returns_404_for_unknown_tab(html_client):
    resp = html_client.get("/?tab=inesistente")
    assert resp.status_code == 404


def test_home_returns_403_when_reparto_not_allowed(client, html_env):
    html_env["set_policy"](
        allowed_reparti={"10"},
        allowed_reparti_menu=[("10", "Montaggio")],
        perm_map={"home": True},
    )

    resp = client.get("/?tab=officina")
    assert resp.status_code == 403


def test_home_returns_403_when_permission_is_missing(client, html_env):
    html_env["set_policy"](
        allowed_reparti={"20"},
        allowed_reparti_menu=[("20", "Officina")],
        perm_map={"home": False},
    )

    resp = client.get("/?tab=officina")
    assert resp.status_code == 403


def test_api_home_bridge_returns_404_for_unknown_tab(html_client):
    resp = html_client.get("/api/home/inesistente/bridge")
    assert resp.status_code == 404


def test_api_home_bridge_returns_changed_false_when_after_is_current(
    client, html_env, monkeypatch
):
    html_env["set_policy"](
        allowed_reparti={"20"},
        allowed_reparti_menu=[("20", "Officina")],
        perm_map={"home": True},
    )
    monkeypatch.setattr(
        mod := importlib.import_module(MODULE_PATH), "_last_change_event_id", lambda: 7
    )

    resp = client.get("/api/home/officina/bridge?after=7")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload == {"changed": False, "last_event_id": 7}


def test_api_home_bridge_returns_fragments_when_changed(
    mod, client, html_env, monkeypatch
):
    html_env["set_policy"](
        allowed_reparti={"20"},
        allowed_reparti_menu=[("20", "Officina")],
        perm_map={"home": True},
    )

    monkeypatch.setattr(mod, "_last_change_event_id", lambda: 9)
    monkeypatch.setattr(
        mod,
        "_query_for_tab",
        lambda policy, reparto_code: HtmlFakeQuery(
            [SimpleNamespace(IdDocumento="1", IdRiga="10", CodReparto=reparto_code)]
        ),
    )

    def fake_renderer(odp):
        assert len(odp) == 1
        return {"tbody_ordini_da_eseguire": "<tr>r1</tr>"}

    monkeypatch.setattr(mod, "RENDERERS", {**mod.RENDERERS, "officina": fake_renderer})

    resp = client.get("/api/home/officina/bridge")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["changed"] is True
    assert payload["last_event_id"] == 9
    assert payload["fragments"] == {"tbody_ordini_da_eseguire": "<tr>r1</tr>"}


def test_api_home_bridge_returns_403_when_reparto_not_allowed(client, html_env):
    html_env["set_policy"](
        allowed_reparti={"10"},
        allowed_reparti_menu=[("10", "Montaggio")],
        perm_map={"home": True},
    )

    resp = client.get("/api/home/officina/bridge")
    assert resp.status_code == 403


def test_api_home_bridge_returns_403_when_permission_is_missing(client, html_env):
    html_env["set_policy"](
        allowed_reparti={"20"},
        allowed_reparti_menu=[("20", "Officina")],
        perm_map={"home": False},
    )

    resp = client.get("/api/home/officina/bridge")
    assert resp.status_code == 403


def test_api_lotti_componenti_requires_ids(client, common_api_patches):
    resp = client.post("/api/ordini/lotti-componenti", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_lotti_componenti_modalita_macchina_returns_force_show_section(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(GestioneLotto="no", GestioneMatricola="si")
    componenti = [
        {
            "CodArt": "CMP-001",
            "Descrizione": "Componente 1",
            "lotti": [{"RifLottoAlfa": "LOT-01", "Giacenza": "5"}],
        },
        {
            "CodArt": "CMP-002",
            "Descrizione": "Componente 2",
            "lotti": [],
        },
    ]

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda ordine, include_senza_lotti=True, ignore_parent_gestione_lotto=True: (
            componenti
        ),
    )

    resp = client.post(
        "/api/ordini/lotti-componenti",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "modalita": "m",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["gestioneLotto"] is True
    assert payload["force_show_section"] is True
    assert payload["haComponentiLotto"] is True
    assert payload["componenti"] == componenti


def test_api_lotti_componenti_returns_empty_when_order_has_no_gestione_lotto(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(GestioneLotto="no")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/lotti-componenti",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "modalita": "sl",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload == {
        "ok": True,
        "gestioneLotto": False,
        "haComponentiLotto": False,
        "componenti": [],
    }


def test_api_lotti_componenti_standard_returns_componenti_and_flags(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(GestioneLotto="si")
    componenti = [
        {
            "CodArt": "CMP-010",
            "Descrizione": "Vite",
            "lotti": [{"RifLottoAlfa": "LV-01", "Giacenza": "100"}],
        }
    ]

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda ordine, include_senza_lotti=True: componenti,
    )

    resp = client.post(
        "/api/ordini/lotti-componenti",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["gestioneLotto"] is True
    assert payload["force_show_section"] is True
    assert payload["haComponentiLotto"] is True
    assert payload["componenti"] == componenti


def test_api_export_avp_txt_returns_404_when_no_pending_rows(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    monkeypatch.setattr(mod, "_get_pending_avp_outbox", lambda: [])

    resp = client.post("/api/erp/export/avp", json={"suffix": "manuale"})

    payload = resp.get_json()
    assert resp.status_code == 404
    assert payload["ok"] is False
    assert "Nessun record ERP pending" in payload["error"]


def test_api_export_avp_txt_exports_file_and_updates_rows(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    row1 = SimpleNamespace(
        status="pending", exported_at=None, last_error="x", attempts=0
    )
    row2 = SimpleNamespace(
        status="pending", exported_at=None, last_error="y", attempts=2
    )

    monkeypatch.setattr(mod, "_get_pending_avp_outbox", lambda: [row1, row2])
    monkeypatch.setattr(mod, "_build_avp_txt_content", lambda rows: "TES\nRIG")
    monkeypatch.setattr(
        mod,
        "_write_txt_content",
        lambda content, prefix, suffix, encoding: Path(f"/tmp/{prefix}_{suffix}.txt"),
    )
    monkeypatch.setattr(
        mod,
        "_now_rome_dt",
        lambda: datetime.fromisoformat("2026-03-20T11:20:00+01:00"),
    )

    resp = client.post("/api/erp/export/avp", json={"suffix": "test-avp"})

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["file_name"] == "AVPB_test-avp.txt"
    assert payload["file_path"].endswith("AVPB_test-avp.txt")
    assert payload["records"] == 2

    assert row1.status == "exported"
    assert row2.status == "exported"
    assert row1.last_error is None
    assert row2.last_error is None
    assert row1.exported_at == "2026-03-20T11:20:00+01:00"
    assert row2.exported_at == "2026-03-20T11:20:00+01:00"
    assert row1.attempts == 1
    assert row2.attempts == 3
    assert fake_db.commit_count == 1


def test_api_export_avp_txt_marks_rows_error_when_write_fails(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    row1 = SimpleNamespace(
        status="pending", exported_at=None, last_error=None, attempts=0
    )
    row2 = SimpleNamespace(
        status="pending", exported_at=None, last_error=None, attempts=5
    )

    monkeypatch.setattr(mod, "_get_pending_avp_outbox", lambda: [row1, row2])
    monkeypatch.setattr(mod, "_build_avp_txt_content", lambda rows: "TES\nRIG")

    def fake_write(*args, **kwargs):
        raise RuntimeError("disco pieno")

    monkeypatch.setattr(mod, "_write_txt_content", fake_write)

    resp = client.post("/api/erp/export/avp", json={"suffix": "errore"})

    payload = resp.get_json()
    assert resp.status_code == 500
    assert payload["ok"] is False
    assert "Errore generazione file AVP" in payload["error"]
    assert "disco pieno" in payload["error"]

    assert row1.status == "error"
    assert row2.status == "error"
    assert row1.last_error == "disco pieno"
    assert row2.last_error == "disco pieno"
    assert row1.attempts == 1
    assert row2.attempts == 6
    assert fake_db.commit_count == 1


def test_api_sospendi_ordine_montaggio_macchina_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/montaggio/macchina/sospendi", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_sospendi_ordine_montaggio_macchina_rejects_non_montaggio_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "officina")

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "vista montaggio" in payload["error"]


def test_api_sospendi_ordine_montaggio_macchina_accumulates_runtime_and_commits(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        GestioneMatricola="si",
        CodReparto="10",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="15",
        Utente_operazione="",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "75"
        stato_obj.data_ultima_attivazione = None
        return 60

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(
        mod,
        "_push_change_event",
        lambda **kwargs: pushed.append(kwargs),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine macchina sospeso"
    assert payload["stato_ordine"] == "In Sospeso"
    assert payload["tempo_funzionamento"] == "75"
    assert payload["elapsed_seconds"] == 60

    assert ordine.StatoOrdine == "In Sospeso"
    assert stato.Stato_odp == "In Sospeso"
    assert stato.Utente_operazione == "tester"
    assert stato.Tempo_funzionamento == "75"
    assert stato.data_ultima_attivazione is None
    assert fake_db.commit_count == 1
    assert pushed
    assert pushed[0]["topic"] == "ordine_sospeso_montaggio_macchina"
    assert pushed[0]["extra_payload"]["matricola"] == "MAT-001"
    assert pushed[0]["extra_payload"]["fase"] == "1"


def test_api_riattiva_ordine_montaggio_macchina_returns_409_when_blocked_by_outbox(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_get_blocking_outbox_for_phase",
        lambda **kwargs: SimpleNamespace(status="error", outbox_id=321),
    )
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["changed"] is False
    assert payload["outbox_status"] == "error"
    assert payload["outbox_id"] == 321
    assert payload["fase"] == "1"


def test_api_riattiva_ordine_montaggio_macchina_reactivates_suspended_order(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso", GestioneMatricola="si")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        data_ultima_attivazione="2024-01-02T13:45:56",
        Tempo_funzionamento="75",
    )
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_ensure_stato_attivo", lambda **kwargs: stato)
    monkeypatch.setattr(
        mod,
        "_push_change_event",
        lambda **kwargs: pushed.append(kwargs),
    )
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine macchina riattivato"
    assert payload["stato_ordine"] == "Attivo"
    assert payload["fase"] == "1"
    assert ordine.StatoOrdine == "Attivo"
    assert fake_db.commit_count == 1
    assert pushed
    assert pushed[0]["topic"] == "ordine_riattivato_montaggio_macchina"
    assert pushed[0]["extra_payload"]["matricola"] == "MAT-001"
    assert pushed[0]["extra_payload"]["fase"] == "1"


def test_api_riattiva_ordine_montaggio_macchina_returns_noop_when_already_active(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine macchina già attivo"
    assert payload["stato_ordine"] == "Attivo"
    assert fake_db.commit_count == 0


def test_api_chiudi_ordine_montaggio_macchina_rejects_non_machine_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="no")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "ordini macchina" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_pianificata_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Pianificata", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["error"] == "Ordine non chiudibile: è ancora Pianificata"


def test_api_chiudi_ordine_montaggio_macchina_requires_component_lots_when_needed(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda *a, **k: [{"CodArt": "CMP-001", "lotti": [{"RifLottoAlfa": "LOT-01"}]}],
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "assegnazione dei lotti materiale" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_final_success_writes_outbox_logs_and_response(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        GestioneMatricola="si",
        Quantita="1",
        QtyDaLavorare="1",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=777, status="pending")
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    def fake_tab_from_ordine(obj):
        if obj is ordine:
            return "montaggio"
        return ""

    monkeypatch.setattr(mod, "_tab_from_ordine", fake_tab_from_ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "120"
        stato_obj.data_ultima_attivazione = None
        return 120

    def fake_queue_export(ordine, fase_corrente, payload):
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "Chiusa"
        kwargs["ordine"].QtyDaLavorare = "0"
        kwargs["ordine"].FaseAttiva = kwargs["fase_corrente"]
        return {
            "tipo": "finale",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": None,
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(
        mod,
        "_push_change_event",
        lambda **kwargs: pushed.append(kwargs),
    )
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)
    monkeypatch.setattr(
        mod,
        "_query_for_tab",
        lambda *a, **k: type("FakeTabQuery", (), {"all": lambda self: []})(),
    )
    monkeypatch.setitem(mod.RENDERERS, "montaggio", lambda odp: {})
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == "Ordine macchina chiuso definitivamente"
    assert payload["stato_ordine"] == "Chiusa"
    assert payload["fase"] == "1"
    assert payload["fase_successiva"] is None
    assert payload["outbox_id"] == 777
    assert payload["outbox_status"] == "pending"

    assert ordine.StatoOrdine == "Chiusa"
    assert ordine.QtyDaLavorare == "0"
    assert fake_db.commit_count == 1
    assert fake_db.deleted == [stato]
    assert pushed
    assert pushed[0]["topic"] == "fase_consuntivata_montaggio_macchina"
    assert pushed[0]["extra_payload"]["matricola"] == "MAT-001"
    assert pushed[0]["extra_payload"]["fase"] == "1"


def test_api_chiudi_ordine_montaggio_macchina_advance_success_returns_next_phase_message(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        GestioneMatricola="si",
        Quantita="1",
        QtyDaLavorare="1",
        FasiOrdine='["1", "2"]',
        FaseAttiva="1",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=778, status="pending")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    def fake_tab_from_ordine(obj):
        if obj is ordine:
            return "montaggio"
        return ""

    monkeypatch.setattr(mod, "_tab_from_ordine", fake_tab_from_ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "90"
        stato_obj.data_ultima_attivazione = None
        return 90

    def fake_queue_export(ordine, fase_corrente, payload):
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "Pianificata"
        kwargs["ordine"].QtyDaLavorare = "1"
        kwargs["ordine"].FaseAttiva = "2"
        return {
            "tipo": "avanzata",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": "2",
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(mod, "_push_change_event", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)
    monkeypatch.setattr(
        mod,
        "_query_for_tab",
        lambda *a, **k: type("FakeTabQuery", (), {"all": lambda self: []})(),
    )
    monkeypatch.setitem(mod.RENDERERS, "montaggio", lambda odp: {})
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "avanzamento macchina test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == (
        "Fase macchina 1 consuntivata. Ordine riportato in pianificata sulla fase 2."
    )
    assert payload["stato_ordine"] == "Pianificata"
    assert payload["fase"] == "1"
    assert payload["fase_successiva"] == "2"
    assert payload["outbox_id"] == 778
    assert payload["outbox_status"] == "pending"

    assert ordine.StatoOrdine == "Pianificata"
    assert ordine.FaseAttiva == "2"
    assert fake_db.commit_count == 1
    assert fake_db.deleted == [stato]


def test_api_prendi_ordine_returns_noop_when_already_active(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/presa",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine già attivo"
    assert payload["stato_ordine"] == "Attivo"
    assert fake_db.commit_count == 0


def test_api_prendi_ordine_returns_noop_when_suspended(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/presa",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine in sospeso: usare la riattivazione"
    assert payload["stato_ordine"] == "In Sospeso"
    assert fake_db.commit_count == 0


def test_api_prendi_ordine_returns_noop_when_state_not_allowed(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Chiusa")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/presa",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine non prendibile: stato attuale 'Chiusa'"
    assert payload["stato_ordine"] == "Chiusa"
    assert fake_db.commit_count == 0


def test_api_sospendi_ordine_returns_noop_when_already_suspended(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine già in sospeso"
    assert payload["stato_ordine"] == "In Sospeso"
    assert payload["tempo_funzionamento"] == "0"
    assert payload["elapsed_seconds"] == 0
    assert fake_db.commit_count == 0


def test_api_sospendi_ordine_returns_noop_when_state_not_suspendable(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Chiusa")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine non sospendibile: stato attuale 'Chiusa'"
    assert payload["stato_ordine"] == "Chiusa"
    assert payload["tempo_funzionamento"] == "0"
    assert payload["elapsed_seconds"] == 0
    assert fake_db.commit_count == 0


def test_api_riattiva_ordine_returns_noop_when_already_active(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/riattiva",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine già attivo"
    assert payload["stato_ordine"] == "Attivo"
    assert fake_db.commit_count == 0


def test_api_riattiva_ordine_returns_noop_when_state_not_reactivable(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Chiusa")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("sl", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/riattiva",
        json={"id_documento": ordine.IdDocumento, "id_riga": ordine.IdRiga},
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine non riattivabile: stato attuale 'Chiusa'"
    assert payload["stato_ordine"] == "Chiusa"
    assert fake_db.commit_count == 0


def test_api_chiudi_ordine_returns_409_when_blocked_by_outbox(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_get_blocking_outbox_for_phase",
        lambda **kwargs: SimpleNamespace(status="pending", outbox_id=555),
    )

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "10",
            "quantita_non_conforme": "0",
            "note": "chiusura test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["outbox_status"] == "pending"
    assert payload["outbox_id"] == 555
    assert "attesa di sincronizzazione" in payload["error"]


def test_api_chiudi_ordine_rejects_lotto_not_found(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "10",
            "quantita_non_conforme": "0",
            "note": "chiusura test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-404",
                    "Quantita": "2",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert payload["error"] == "Lotto LOT-404 non trovato per CMP-001."


def test_api_chiudi_ordine_rejects_lotto_qty_above_giacenza(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")
    lotto_db = SimpleNamespace(
        CodArt="CMP-001",
        RifLottoAlfa="LOT-01",
        Giacenza="3",
    )

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([lotto_db])}),
    )

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "10",
            "quantita_non_conforme": "0",
            "note": "chiusura test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "5",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "CMP-001 lotto LOT-01" in payload["error"]
    assert "giacenza 3" in payload["error"]


def test_api_chiudi_ordine_partial_success_writes_outbox_logs_and_keeps_runtime_row(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        Quantita="10",
        QtyDaLavorare="10",
        GestioneLotto="no",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="15",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=901, status="pending")
    pushed = []

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "75"
        stato_obj.data_ultima_attivazione = None
        return 60

    def fake_queue_export(ordine, fase_corrente, payload):
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "In Sospeso"
        kwargs["ordine"].QtyDaLavorare = "6"
        kwargs["ordine"].FaseAttiva = kwargs["fase_corrente"]
        kwargs["stato"].Stato_odp = "In Sospeso"
        kwargs["stato"].Fase = kwargs["fase_corrente"]
        kwargs["stato"].data_ultima_attivazione = None
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": kwargs["fase_corrente"],
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(
        mod,
        "_push_change_event",
        lambda **kwargs: pushed.append(kwargs),
    )
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "4",
            "quantita_non_conforme": "0",
            "note": "parziale test",
            "chiusura_parziale": True,
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == (
        "Fase 1 chiusa parzialmente. Ordine messo in sospeso sulla stessa fase."
    )
    assert payload["stato_ordine"] == "In Sospeso"
    assert payload["chiusura_parziale"] is True
    assert payload["fase"] == "1"
    assert payload["fase_successiva"] == "1"
    assert payload["qty_da_lavorare"] == "6"
    assert payload["outbox_id"] == 901
    assert payload["outbox_status"] == "pending"

    assert ordine.StatoOrdine == "In Sospeso"
    assert ordine.QtyDaLavorare == "6"
    assert fake_db.flush_count == 1
    assert fake_db.commit_count == 1
    assert fake_db.deleted == []
    assert pushed
    assert pushed[0]["topic"] == "fase_consuntivata_parziale"
    assert pushed[0]["extra_payload"]["qty_da_lavorare_pre"] == "10"
    assert pushed[0]["extra_payload"]["qty_da_lavorare_post"] == "6"
    assert pushed[0]["extra_payload"]["chiusura_parziale"] is True

    input_logs = [obj for obj in fake_db.added if hasattr(obj, "NoteChiusura")]
    assert input_logs
    assert input_logs[-1].NoteChiusura.startswith("[PARZIALE] residuo=6;")


def test_api_chiudi_ordine_partial_generates_lotto_prodotto_with_only_ok_parent_lots(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        Quantita="10",
        QtyDaLavorare="10",
        GestioneLotto="si",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    lotto_ok = SimpleNamespace(CodArt="CMP-001", RifLottoAlfa="LOT-OK", Giacenza="10")
    lotto_ko = SimpleNamespace(CodArt="CMP-002", RifLottoAlfa="LOT-KO", Giacenza="10")
    outbox = SimpleNamespace(outbox_id=902, status="pending")
    captured = {}

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([lotto_ok, lotto_ko])}),
    )
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")
    monkeypatch.setattr(mod, "generazione_lotti", lambda dt=None: "20260320")

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "30"
        stato_obj.data_ultima_attivazione = None
        return 30

    def fake_queue_export(ordine, fase_corrente, payload):
        captured["payload"] = payload
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "In Sospeso"
        kwargs["ordine"].QtyDaLavorare = "6"
        kwargs["ordine"].FaseAttiva = kwargs["fase_corrente"]
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": kwargs["fase_corrente"],
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(mod, "_push_change_event", lambda **kwargs: None)
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "4",
            "quantita_non_conforme": "0",
            "note": "parziale con lotto",
            "chiusura_parziale": True,
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-OK",
                    "Quantita": "2",
                    "Esito": "ok",
                },
                {
                    "CodArt": "CMP-002",
                    "RifLottoAlfa": "LOT-KO",
                    "Quantita": "1",
                    "Esito": "ko",
                },
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["chiusura_parziale"] is True
    assert payload["outbox_id"] == 902

    export_payload = captured["payload"]
    assert export_payload["lotto_prodotto"]["rif_lotto_alfa"] == "20260320"
    assert export_payload["lotto_prodotto"]["quantita"] == "4"
    assert (
        export_payload["lotto_prodotto"]["parent_lotti"][0]["rif_lotto_alfa"]
        == "LOT-OK"
    )
    assert len(export_payload["lotto_prodotto"]["parent_lotti"]) == 1

    generated_logs = [obj for obj in fake_db.added if hasattr(obj, "ParentLottiJson")]
    assert generated_logs
    assert generated_logs[-1].RifLottoAlfa == "20260320"
    assert '"LOT-OK"' in generated_logs[-1].ParentLottiJson
    assert '"LOT-KO"' not in generated_logs[-1].ParentLottiJson


def test_api_lotti_componenti_standard_returns_force_show_true_but_no_available_lots(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(GestioneLotto="si")
    componenti = [
        {
            "CodArt": "CMP-020",
            "Descrizione": "Componente senza lotti",
            "lotti": [],
        }
    ]

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda ordine, include_senza_lotti=True: componenti,
    )

    resp = client.post(
        "/api/ordini/lotti-componenti",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["gestioneLotto"] is True
    assert payload["force_show_section"] is True
    assert payload["haComponentiLotto"] is False
    assert payload["componenti"] == componenti


def test_api_lotti_componenti_macchina_returns_false_flags_when_components_empty(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(GestioneLotto="no", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(
        mod,
        "_componenti_lotto_per_ordine",
        lambda ordine, include_senza_lotti=True, ignore_parent_gestione_lotto=True: [],
    )

    resp = client.post(
        "/api/ordini/lotti-componenti",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "modalita": "m",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload == {
        "ok": True,
        "gestioneLotto": True,
        "force_show_section": False,
        "haComponentiLotto": False,
        "componenti": [],
    }


def test_api_export_avp_txt_uses_default_suffix_when_blank(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    row = SimpleNamespace(
        status="pending", exported_at=None, last_error=None, attempts=0
    )
    captured = {}

    monkeypatch.setattr(mod, "_get_pending_avp_outbox", lambda: [row])
    monkeypatch.setattr(mod, "_build_avp_txt_content", lambda rows: "TES\nRIG")

    def fake_write(content, prefix, suffix, encoding):
        captured["prefix"] = prefix
        captured["suffix"] = suffix
        captured["encoding"] = encoding
        return Path("/tmp/AVPB_manuale.txt")

    monkeypatch.setattr(mod, "_write_txt_content", fake_write)
    monkeypatch.setattr(
        mod,
        "_now_rome_dt",
        lambda: datetime.fromisoformat("2026-03-20T11:40:00+01:00"),
    )

    resp = client.post("/api/erp/export/avp", json={"suffix": "   "})

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert captured["prefix"] == "AVPB"
    assert captured["suffix"] == "manuale"
    assert captured["encoding"] == "utf-8-sig"


def test_api_export_avp_txt_rolls_back_when_error_commit_also_fails(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    row = SimpleNamespace(
        status="pending", exported_at=None, last_error=None, attempts=0
    )

    monkeypatch.setattr(mod, "_get_pending_avp_outbox", lambda: [row])
    monkeypatch.setattr(mod, "_build_avp_txt_content", lambda rows: "TES\nRIG")

    def fake_write(*args, **kwargs):
        raise RuntimeError("disco pieno")

    def failing_commit():
        fake_db.commit_count += 1
        raise RuntimeError("commit failure")

    monkeypatch.setattr(mod, "_write_txt_content", fake_write)
    monkeypatch.setattr(fake_db, "commit", failing_commit)

    resp = client.post("/api/erp/export/avp", json={"suffix": "errore"})

    payload = resp.get_json()
    assert resp.status_code == 500
    assert payload["ok"] is False
    assert "Errore generazione file AVP: disco pieno" == payload["error"]

    assert row.status == "error"
    assert row.last_error == "disco pieno"
    assert row.attempts == 1
    assert fake_db.commit_count == 1
    assert fake_db.rollback_count == 1


def test_api_chiudi_ordine_rejects_negative_quantities(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "-1",
            "quantita_non_conforme": "0",
            "note": "errore quantità",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert payload["error"] == "Le quantità non possono essere negative"


def test_api_sospendi_ordine_montaggio_macchina_rejects_non_machine_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="no")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "ordini macchina" in payload["error"]


def test_api_sospendi_ordine_montaggio_macchina_returns_409_when_runtime_row_is_missing(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert "odp_in_carico" in payload["error"]
    assert fake_db.rollback_count == 1


def test_api_sospendi_ordine_montaggio_macchina_returns_noop_when_already_suspended(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert payload["message"] == "Ordine macchina già in sospeso"
    assert payload["stato_ordine"] == "In Sospeso"
    assert payload["tempo_funzionamento"] == "0"
    assert payload["elapsed_seconds"] == 0
    assert fake_db.commit_count == 0


def test_api_sospendi_ordine_montaggio_macchina_returns_noop_when_state_not_suspendable(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Chiusa", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/sospendi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "causale": "PAUSA",
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert (
        payload["message"] == "Ordine macchina non sospendibile: stato attuale 'Chiusa'"
    )
    assert payload["stato_ordine"] == "Chiusa"
    assert fake_db.commit_count == 0


def test_api_riattiva_ordine_montaggio_macchina_rejects_non_machine_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso", GestioneMatricola="no")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "ordini macchina" in payload["error"]


def test_api_riattiva_ordine_montaggio_macchina_returns_noop_when_state_not_reactivable(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Chiusa", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_fragments_for_ordine_tab",
        lambda *a, **k: ("montaggio", {"html": "<div />"}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is False
    assert (
        payload["message"] == "Ordine macchina non riattivabile: stato attuale 'Chiusa'"
    )
    assert payload["stato_ordine"] == "Chiusa"
    assert fake_db.commit_count == 0


def test_api_chiudi_ordine_montaggio_macchina_returns_409_when_blocked_by_outbox(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(
        mod,
        "_get_blocking_outbox_for_phase",
        lambda **kwargs: SimpleNamespace(status="pending", outbox_id=999),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 409
    assert payload["ok"] is False
    assert payload["outbox_status"] == "pending"
    assert payload["outbox_id"] == 999
    assert "attesa di sincronizzazione" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_non_montaggio_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "officina")

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "vista montaggio" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_missing_cod_or_lotto(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
            "lotti": [
                {
                    "CodArt": "",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "1",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Codice e lotto sono obbligatori" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_qty_not_positive(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "0",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "quantità deve essere > 0" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_lotto_not_found(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-404",
                    "Quantita": "1",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert payload["error"] == "Lotto LOT-404 non trovato per CMP-001."


def test_api_chiudi_ordine_montaggio_macchina_rejects_lotto_qty_above_giacenza(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")
    lotto_db = SimpleNamespace(
        CodArt="CMP-001",
        RifLottoAlfa="LOT-01",
        Giacenza="3",
    )

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([lotto_db])}),
    )

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "5",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "CMP-001 lotto LOT-01" in payload["error"]
    assert "giacenza 3" in payload["error"]


def test_api_chiudi_ordine_advance_success_returns_next_phase_message(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        Quantita="10",
        QtyDaLavorare="5",
        NumFase='["1", "2"]',
        FaseAttiva="1",
    )
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=903, status="pending")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")
    monkeypatch.setattr(mod, "_push_change_event", lambda **kwargs: None)
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "45"
        stato_obj.data_ultima_attivazione = None
        return 45

    def fake_queue_export(ordine, fase_corrente, payload):
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "Pianificata"
        kwargs["ordine"].QtyDaLavorare = "4"
        kwargs["ordine"].FaseAttiva = "2"
        return {
            "tipo": "avanzata",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": "2",
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "4",
            "quantita_non_conforme": "1",
            "note": "avanzamento test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["message"] == (
        "Fase 1 consuntivata. Ordine riportato in pianificata sulla fase 2."
    )
    assert payload["fase"] == "1"
    assert payload["fase_successiva"] == "2"
    assert payload["stato_ordine"] == "Pianificata"
    assert payload["chiusura_parziale"] is False
    assert payload["qty_da_lavorare"] == "4"
    assert payload["outbox_id"] == 903
    assert payload["outbox_status"] == "pending"

    assert ordine.StatoOrdine == "Pianificata"
    assert ordine.FaseAttiva == "2"
    assert ordine.QtyDaLavorare == "4"
    assert fake_db.commit_count == 1
    assert fake_db.deleted == [stato]


def test_api_chiudi_ordine_rejects_partial_zero_worked_qty(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "0",
            "quantita_non_conforme": "0",
            "chiusura_parziale": True,
            "note": "parziale zero",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "quantità lavorata > 0" in payload["error"]


def test_api_chiudi_ordine_rejects_invalid_qty_da_lavorare_value(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        Quantita="10",
        QtyDaLavorare="abc",
    )

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "1",
            "quantita_non_conforme": "0",
            "note": "qty invalid",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità non valida" in payload["error"]


def test_api_chiudi_ordine_rejects_non_integral_quantita_conforme(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "4.5",
            "quantita_non_conforme": "0",
            "note": "q_ok frazionaria",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità conforme" in payload["error"]
    assert "numero intero" in payload["error"]


def test_api_chiudi_ordine_rejects_non_integral_quantita_non_conforme(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "4",
            "quantita_non_conforme": "0.5",
            "note": "q_nok frazionaria",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità KO" in payload["error"]
    assert "numero intero" in payload["error"]


def test_api_chiudi_ordine_rejects_non_integral_lotto_qty(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="10")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "GiacenzaLotti",
        type("FakeGiacenzaLotti", (), {"query": FakeQuery([])}),
    )

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "quantita_conforme": "5",
            "quantita_non_conforme": "0",
            "note": "lotto frazionario",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "1.5",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità lotto non valida" in payload["error"]
    assert "numero intero" in payload["error"]


def test_api_riattiva_ordine_montaggio_macchina_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/montaggio/macchina/riattiva", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_riattiva_ordine_montaggio_macchina_rejects_non_montaggio_order(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="In Sospeso", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "officina")

    resp = client.post(
        "/api/ordini/montaggio/macchina/riattiva",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "vista montaggio" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/montaggio/macchina/chiudi", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_chiudi_ordine_montaggio_macchina_rejects_invalid_lotto_qty_format(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(StatoOrdine="Attivo", GestioneMatricola="si")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
            "lotti": [
                {
                    "CodArt": "CMP-001",
                    "RifLottoAlfa": "LOT-01",
                    "Quantita": "abc",
                }
            ],
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità lotto non valida" in payload["error"]


def test_api_chiudi_ordine_defaults_quantita_when_missing(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="10", QtyDaLavorare="6")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=904, status="pending")
    captured = {}

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")
    monkeypatch.setattr(mod, "_push_change_event", lambda **kwargs: None)
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    def fake_accumulate(stato_obj, now_dt):
        stato_obj.Tempo_funzionamento = "20"
        stato_obj.data_ultima_attivazione = None
        return 20

    def fake_queue_export(ordine, fase_corrente, payload):
        captured["payload"] = payload
        return outbox

    def fake_transition(**kwargs):
        kwargs["ordine"].StatoOrdine = "Chiusa"
        kwargs["ordine"].QtyDaLavorare = "0"
        kwargs["ordine"].FaseAttiva = kwargs["fase_corrente"]
        return {
            "tipo": "finale",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": None,
        }

    monkeypatch.setattr(mod, "_accumulate_runtime_until", fake_accumulate)
    monkeypatch.setattr(mod, "_queue_phase_export", fake_queue_export)
    monkeypatch.setattr(mod, "_advance_or_finalize_phase", fake_transition)

    resp = client.post(
        "/api/ordini/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "note": "chiusura senza quantità esplicite",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["outbox_id"] == 904

    export_payload = captured["payload"]
    assert export_payload["quantita_ok"] == "6"
    assert export_payload["quantita_ko"] == "0"
    assert export_payload["quantita_da_lavorare"] == "6"
    assert fake_db.commit_count == 1


def test_api_chiudi_ordine_defaults_quantita_when_missing_and_generates_final_message(
    client,
    mod,
    monkeypatch,
    fake_db,
    common_api_patches,
    light_factories,
    empty_change_events,
):
    ordine = make_ordine(StatoOrdine="Attivo", Quantita="8", QtyDaLavorare="3")
    stato = SimpleNamespace(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Stato_odp="Attivo",
        Data_in_carico="2024-01-02T12:00:00",
        Tempo_funzionamento="0",
        Utente_operazione="tester",
        Fase="1",
        data_ultima_attivazione="2024-01-02T13:00:00",
    )
    outbox = SimpleNamespace(outbox_id=905, status="pending")

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])
    monkeypatch.setattr(
        mod,
        "StatoOdp",
        type("FakeStatoOdp", (), {"query": FakeQuery([stato])}),
    )
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "")
    monkeypatch.setattr(mod, "_push_change_event", lambda **kwargs: None)
    monkeypatch.setattr(mod, "gen_etichette", lambda *a, **k: None)

    monkeypatch.setattr(
        mod,
        "_accumulate_runtime_until",
        lambda stato_obj, now_dt: 0,
    )
    monkeypatch.setattr(
        mod,
        "_queue_phase_export",
        lambda ordine, fase_corrente, payload: outbox,
    )
    monkeypatch.setattr(
        mod,
        "_advance_or_finalize_phase",
        lambda **kwargs: {
            kwargs["ordine"].__setattr__("StatoOrdine", "Chiusa")
            or kwargs["ordine"].__setattr__("QtyDaLavorare", "0")
            or kwargs["ordine"].__setattr__("FaseAttiva", kwargs["fase_corrente"])
            or "tipo": "finale",
            "fase_corrente": kwargs["fase_corrente"],
            "fase_successiva": None,
        },
    )


def test_api_sospendi_ordine_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/sospendi", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_riattiva_ordine_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/riattiva", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_api_chiudi_ordine_requires_ids(
    client,
    common_api_patches,
):
    resp = client.post("/api/ordini/chiudi", json={})

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "IdDocumento e IdRiga" in payload["error"]


def test_home_returns_404_when_no_allowed_tabs_and_no_query_tab(
    html_client,
    html_env,
):
    html_env["set_policy"](
        allowed_reparti=set(),
        allowed_reparti_menu=[],
        perm_map={"home": True},
    )

    resp = html_client.get("/")

    assert resp.status_code == 404


def test_api_chiudi_ordine_montaggio_macchina_rejects_invalid_qty_da_lavorare_value(
    client,
    mod,
    monkeypatch,
    common_api_patches,
):
    ordine = make_ordine(
        StatoOrdine="Attivo",
        GestioneMatricola="si",
        QtyDaLavorare="abc",
    )

    monkeypatch.setattr(mod, "_get_visible_odp_by_key", lambda *a, **k: ordine)
    monkeypatch.setattr(mod, "_tab_from_ordine", lambda ordine: "montaggio")
    monkeypatch.setattr(mod, "_componenti_lotto_per_ordine", lambda *a, **k: [])

    resp = client.post(
        "/api/ordini/montaggio/macchina/chiudi",
        json={
            "id_documento": ordine.IdDocumento,
            "id_riga": ordine.IdRiga,
            "matricola": "MAT-001",
            "fase": "1",
            "note": "chiusura macchina test",
        },
    )

    payload = resp.get_json()
    assert resp.status_code == 400
    assert payload["ok"] is False
    assert "Quantità non valida" in payload["error"]


def test_ensure_stato_attivo_creates_runtime_row_when_missing(
    mod,
    monkeypatch,
    fake_db,
):
    monkeypatch.setattr(mod, "StatoOdp", lambda **kwargs: SimpleNamespace(**kwargs))

    ordine = make_ordine(StatoOrdine="Pianificata")
    when_dt = datetime(2026, 3, 20, 11, 0, 0, tzinfo=mod.ROME_TZ)

    stato = mod._ensure_stato_attivo(
        ordine=ordine,
        stato=None,
        username="tester",
        when_dt=when_dt,
        fase_corrente="2",
    )

    now_iso = when_dt.isoformat(timespec="seconds")
    assert stato.IdDocumento == ordine.IdDocumento
    assert stato.IdRiga == ordine.IdRiga
    assert stato.RifRegistraz == ordine.RifRegistraz
    assert stato.Stato_odp == "Attivo"
    assert stato.Data_in_carico == now_iso
    assert stato.Tempo_funzionamento == "0"
    assert stato.Utente_operazione == "tester"
    assert stato.Fase == "2"
    assert stato.data_ultima_attivazione == now_iso
    assert fake_db.added == [stato]


def test_ensure_stato_attivo_updates_existing_runtime_row_and_initializes_blank_fields(
    mod,
):
    ordine = make_ordine(StatoOrdine="In Sospeso")
    when_dt = datetime(2026, 3, 20, 12, 15, 0, tzinfo=mod.ROME_TZ)
    stato = SimpleNamespace(
        Stato_odp="In Sospeso",
        Utente_operazione="old-user",
        Fase="",
        Data_in_carico="",
        Tempo_funzionamento="",
        data_ultima_attivazione=None,
    )

    result = mod._ensure_stato_attivo(
        ordine=ordine,
        stato=stato,
        username="tester",
        when_dt=when_dt,
        fase_corrente="3",
    )

    now_iso = when_dt.isoformat(timespec="seconds")
    assert result is stato
    assert stato.Stato_odp == "Attivo"
    assert stato.Utente_operazione == "tester"
    assert stato.Fase == "3"
    assert stato.Data_in_carico == now_iso
    assert stato.Tempo_funzionamento == "0"
    assert stato.data_ultima_attivazione == now_iso


def test_accumulate_runtime_until_updates_total_and_clears_last_activation(mod):
    end_dt = datetime(2026, 3, 20, 11, 0, 0, tzinfo=mod.ROME_TZ)
    stato = SimpleNamespace(
        data_ultima_attivazione="2026-03-20T10:30:00+01:00",
        Tempo_funzionamento="1.5",
    )

    elapsed_seconds = mod._accumulate_runtime_until(stato, end_dt)

    assert elapsed_seconds == 1800
    assert stato.Tempo_funzionamento == "2"
    assert stato.data_ultima_attivazione is None


def test_accumulate_runtime_until_resets_blank_state_when_start_is_missing(mod):
    end_dt = datetime(2026, 3, 20, 11, 0, 0, tzinfo=mod.ROME_TZ)
    stato = SimpleNamespace(
        data_ultima_attivazione="",
        Tempo_funzionamento="",
    )

    elapsed_seconds = mod._accumulate_runtime_until(stato, end_dt)

    assert elapsed_seconds == 0
    assert stato.Tempo_funzionamento == "0"
    assert stato.data_ultima_attivazione is None


def test_push_change_event_builds_payload_and_uses_first_reparto_scope(
    mod,
    monkeypatch,
    fake_db,
):
    monkeypatch.setattr(mod, "ChangeEvent", lambda **kwargs: SimpleNamespace(**kwargs))

    ordine = make_ordine(
        StatoOrdine="Attivo",
        CodReparto='["20", "30"]',
    )

    evt = mod._push_change_event(
        topic="ordine_test",
        ordine=ordine,
        extra_payload={"azione": "debug", "fase": "1"},
    )

    payload = mod.json.loads(evt.payload_json)
    assert evt.topic == "ordine_test"
    assert evt.scope == "20"
    assert payload["id_documento"] == ordine.IdDocumento
    assert payload["id_riga"] == ordine.IdRiga
    assert payload["row_key"] == f"{ordine.IdDocumento}|{ordine.IdRiga}"
    assert payload["rif_registraz"] == ordine.RifRegistraz
    assert payload["cod_reparto"] == ordine.CodReparto
    assert payload["stato_ordine"] == ordine.StatoOrdine
    assert payload["azione"] == "debug"
    assert payload["fase"] == "1"
    assert fake_db.added == [evt]


def test_generazione_lotti_formats_explicit_and_default_datetime(
    mod,
    monkeypatch,
):
    assert mod.generazione_lotti(datetime(2026, 3, 20, 8, 0, 0)) == "20260320"

    monkeypatch.setattr(
        mod,
        "_now_rome_dt",
        lambda: datetime(2027, 1, 2, 9, 30, 0, tzinfo=mod.ROME_TZ),
    )

    assert mod.generazione_lotti() == "20270102"
