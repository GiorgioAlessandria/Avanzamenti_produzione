from datetime import datetime, time
import json
from types import SimpleNamespace

import pytest
from flask import Flask
from sqlalchemy.schema import CreateTable

from app_odp.models import AcqArticoli, AcqArticoliLookup, GiacenzaLotti, GiacenzaMateriale, LottiGeneratiLog, LottiUsatiLog, db
from app_odp.models import InputOdpLog, OdpRuntimeLog, OdpWorkGroup, OdpWorkGroupMember
from app_odp.services import storico_ordini_service as service


@pytest.fixture()
def storico_app():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"log": "sqlite://", "acq": "sqlite://"},
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        GiacenzaLotti.__table__.create(db.engine)
        GiacenzaMateriale.__table__.create(db.engine)
        AcqArticoli.__table__.create(db.engines["acq"])
        AcqArticoliLookup.__table__.create(db.engines["acq"])
        InputOdpLog.__table__.create(db.engines["log"])
        LottiUsatiLog.__table__.create(db.engines["log"])
        LottiGeneratiLog.__table__.create(db.engines["log"])
    yield app
    with app.app_context():
        db.session.remove()
        LottiGeneratiLog.__table__.drop(db.engines["log"])
        LottiUsatiLog.__table__.drop(db.engines["log"])
        InputOdpLog.__table__.drop(db.engines["log"])
        GiacenzaMateriale.__table__.drop(db.engine)
        AcqArticoliLookup.__table__.drop(db.engines["acq"])
        AcqArticoli.__table__.drop(db.engines["acq"])
        GiacenzaLotti.__table__.drop(db.engine)


def test_parse_date_returns_day_boundaries_or_none():
    assert service._parse_date("2026-07-09") == datetime.combine(
        datetime(2026, 7, 9).date(),
        time.min,
    )
    assert service._parse_date("2026-07-09", end_of_day=True) == datetime.combine(
        datetime(2026, 7, 9).date(),
        time.max,
    )
    assert service._parse_date("") is None
    assert service._parse_date("not-date") is None


def test_parse_dt_and_format_dt_handle_valid_and_invalid_values():
    parsed = service._parse_dt("2026-07-09T14:30:10")

    assert parsed == datetime(2026, 7, 9, 14, 30, 10)
    assert service._parse_dt("bad") is None
    assert service._format_dt("2026-07-09T14:30:10") == "09/07/2026 14:30:10"
    assert service._format_dt("bad") == "bad"
    assert service._format_dt("") == "-"


def test_payload_returns_dict_raw_or_wrapped_value():
    assert service._payload(SimpleNamespace(PayloadJson='{"a": 1}')) == {"a": 1}
    assert service._payload(SimpleNamespace(PayloadJson="[1, 2]")) == {"value": [1, 2]}
    assert service._payload(SimpleNamespace(PayloadJson="{bad")) == {"_raw": "{bad"}
    assert service._payload(SimpleNamespace(PayloadJson="")) == {}


def test_storico_hides_non_working_seconds_from_payload_and_notes():
    assert service._payload(
        SimpleNamespace(
            PayloadJson='{"tempo_non_funzionamento_minuti": 10, "tempo_non_funzionamento_secondi": 600}'
        )
    ) == {"tempo_non_funzionamento_minuti": 10}
    assert service._visible_note(
        "Sospensione | Tempo non funzionamento minuti: 10 | Tempo non funzionamento secondi: 600"
    ) == "Sospensione | Tempo non funzionamento minuti: 10"


def test_bounded_int_clamps_or_falls_back_to_default():
    assert service._bounded_int("25", 50, 10, 100) == 25
    assert service._bounded_int("5", 50, 10, 100) == 10
    assert service._bounded_int("500", 50, 10, 100) == 100
    assert service._bounded_int("bad", 50, 10, 100) == 50


def test_non_working_time_is_exposed_only_in_minutes():
    assert service._non_working_minutes(
        SimpleNamespace(
            TempoNonFunzionamentoMinuti="7",
            TempoNonFunzionamentoSecondi="420",
        )
    ) == "7"
    assert service._non_working_minutes(
        SimpleNamespace(
            TempoNonFunzionamentoMinuti="",
            TempoNonFunzionamentoSecondi="90",
        )
    ) == "1.5"
    assert service._non_working_minutes(
        SimpleNamespace(
            TempoNonFunzionamentoMinuti="0",
            TempoNonFunzionamentoSecondi="0",
        )
    ) == ""


def test_num_progr_riga_is_mapped_by_order():
    rows = [
        SimpleNamespace(
            IdDocumento="DOC",
            IdRiga="1",
            NumProgrRiga="20",
        )
    ]

    assert service._num_progr_by_order(rows) == {
        ("DOC", "1"): "20",
    }


def test_order_key_and_row_event_at_normalize_fields():
    row = SimpleNamespace(
        IdDocumento=" DOC ",
        IdRiga=" 10 ",
        EventAt="",
        ClosedAt="2026-07-09T14:30:10",
    )

    assert service._order_key(row) == ("DOC", "10")
    assert service._row_event_at(row) == "2026-07-09T14:30:10"


def test_event_in_group_window_respects_start_and_five_minute_end_tolerance():
    group = SimpleNamespace(
        CreatedAt="2026-07-09T08:00:00",
        ClosedAt="2026-07-09T10:00:00",
        DissolvedAt="",
    )

    assert service._event_in_group_window("2026-07-09T09:00:00", group) is True
    assert service._event_in_group_window("2026-07-09T07:59:59", group) is False
    assert service._event_in_group_window("2026-07-09T10:05:00", group) is True
    assert service._event_in_group_window("2026-07-09T10:05:01", group) is False
    assert service._event_in_group_window("bad", group) is False
    assert service._event_in_group_window("2026-07-09T09:00:00", None) is False


def test_group_for_event_prefers_latest_matching_group():
    row = SimpleNamespace(
        IdDocumento="DOC",
        IdRiga="1",
        EventAt="2026-07-09T10:04:00",
    )
    old_group = SimpleNamespace(
        GroupUid="OLD",
        CreatedAt="2026-07-09T08:00:00",
        ClosedAt="2026-07-09T10:00:00",
        DissolvedAt="",
    )
    new_group = SimpleNamespace(
        GroupUid="NEW",
        CreatedAt="2026-07-09T10:03:00",
        ClosedAt="",
        DissolvedAt="",
    )
    members = {
        ("DOC", "1"): [
            SimpleNamespace(GroupUid="OLD"),
            SimpleNamespace(GroupUid="NEW"),
        ]
    }

    assert service._group_for_event(
        row,
        {},
        members,
        {"OLD": old_group, "NEW": new_group},
    ) == "NEW"


def test_effective_group_type_detects_zero_time_member_as_mixed():
    group = SimpleNamespace(
        GroupType="MULTIPLO",
        Note="",
        members=[
            SimpleNamespace(TimeShareMode="SPLIT"),
            SimpleNamespace(TimeShareMode="ZERO"),
        ],
    )

    assert service._effective_group_type(group) == "MISTO"
    assert service._group_label(service._effective_group_type(group)) == "Misto"


def test_operation_ids_returns_non_blank_unique_operation_ids():
    rows = [
        SimpleNamespace(OperationGroupId=" op-1 "),
        SimpleNamespace(OperationGroupId="op-1"),
        SimpleNamespace(OperationGroupId=""),
        SimpleNamespace(OperationGroupId="op-2"),
    ]

    assert set(service._operation_ids(rows)) == {"op-1", "op-2"}


def test_input_rows_with_runtime_counterpart_are_not_counted_twice():
    duplicate = SimpleNamespace(OperationGroupId="op-1")
    input_only = SimpleNamespace(OperationGroupId="op-2")
    legacy_without_id = SimpleNamespace(OperationGroupId="")

    assert service._input_rows_without_runtime_duplicates(
        [duplicate, input_only, legacy_without_id],
        ["op-1"],
    ) == [input_only, legacy_without_id]


def test_event_description_formats_sections_and_omits_empty_ones():
    row = SimpleNamespace(
        Azione="sospensione",
        UtenteOperazione="mario",
        StatoOrdinePre="Attivo",
        StatoOdpPre="",
        StatoOrdinePost="Sospeso",
        StatoOdpPost="",
        QuantitaConforme="3",
        QuantitaNonConforme="",
        Note="Cambio utensile",
        Motivo="",
    )

    assert service._event_description(row, {}) == (
        "mario - Lavorazione sospesa | stato Attivo -> Sospeso | "
        "OK 3 / KO 0 | Cambio utensile"
    )

    minimal = SimpleNamespace(
        Azione="riattivazione",
        UtenteOperazione="",
        StatoOrdinePre="",
        StatoOdpPre="",
        StatoOrdinePost="",
        StatoOdpPost="",
        QuantitaConforme="",
        QuantitaNonConforme="",
        Note="",
        Motivo="",
    )

    assert service._event_description(minimal, {"utente": "luigi"}) == (
        "luigi - Lavorazione ripresa"
    )


def test_row_matches_python_filters_combines_resource_and_exact_group_type():
    entry = {
        "risorse": ["R-01", "Pressa Nord"],
        "group_type": "multiplo",
    }

    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "PRESSA", "tipo_gruppo": "MULTIPLO"},
    ) is True
    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "taglio", "tipo_gruppo": "MULTIPLO"},
    ) is False
    assert service._row_matches_python_filters(
        entry,
        {"risorsa": "pressa", "tipo_gruppo": "MISTO"},
    ) is False


def test_lot_history_search_returns_used_and_generated_order_assignments(storico_app):
    common = {
        "IdDocumento": "DOC-1",
        "IdRiga": "10",
        "RifRegistraz": "2026.123",
        "CodArt": "ART-1",
        "ClosedBy": "mario",
        "Fase": "20",
    }
    with storico_app.app_context():
        db.session.add_all(
            [
                LottiUsatiLog(
                    **common,
                    RifLottoAlfa="20260918",
                    Quantita="4",
                    ClosedAt="2026-09-18T10:00:00",
                ),
                LottiGeneratiLog(
                    **common,
                    RifLottoAlfa="20260918",
                    Quantita="10",
                    ClosedAt="2026-09-18T11:00:00",
                ),
                LottiUsatiLog(
                    **common,
                    RifLottoAlfa="ALTRO",
                    Quantita="2",
                    ClosedAt="2026-09-18T12:00:00",
                ),
                GiacenzaLotti(
                    CodArt="ART-1", RifLottoAlfa="20260918",
                    CodMag="A", Giacenza="3",
                ),
                GiacenzaLotti(
                    CodArt="ART-1", RifLottoAlfa="20260918",
                    CodMag="B", Giacenza="2",
                ),
            ]
        )
        db.session.commit()

        result = service.build_storico_lotti_search({"lotto": "20260918", "articolo": "ART-1"})

        assert result["ok"] is True
        assert result["total"] == 2
        assert [(row["kind"], row["lotto"]) for row in result["rows"]] == [
            ("generated", "20260918"),
            ("used", "20260918"),
        ]
        assert {row["ordine"] for row in result["rows"]} == {"2026.123"}
        assert result["quantity_summary"] == {
            "original_quantity": "10",
            "available_quantity": "5",
            "used_quantity": "4",
            "original_source": "generated",
        }


@pytest.fixture()
def history_app(storico_app):
    tables = [OdpRuntimeLog, OdpWorkGroup, OdpWorkGroupMember]
    with storico_app.app_context():
        for model in tables:
            engine = db.engines[getattr(model, "__bind_key__", None)]
            # Gli indici legacy di Scope differiscono solo per maiuscole in SQLite.
            with engine.begin() as connection:
                connection.execute(CreateTable(model.__table__))
    yield storico_app
    with storico_app.app_context():
        for model in reversed(tables):
            model.__table__.drop(db.engines[getattr(model, "__bind_key__", None)])


def runtime_event(action, hour, user="mario", row="1", phase="20", **payload):
    return OdpRuntimeLog(
        IdDocumento="DOC", IdRiga=row, RifRegistraz="2026.123", CodArt="ART",
        Azione=action, EventAt=f"2026-10-30T{hour}:00:00", UtenteOperazione=user,
        OperationGroupId=f"{action}-{hour}-{row}", FasePre=phase, FasePost=phase,
        StatoOrdinePre="Attivo", StatoOrdinePost="Attivo",
        PayloadJson=json.dumps({"fase": phase, **payload}),
    )


@pytest.mark.parametrize("action,state,payload,expected", [
    ("chiusura_finale", "Pianificata", {"fase_successiva": "30"}, "Fase completata"),
    ("chiusura_finale", "Chiusa", {}, "Ordine completato"),
    ("chiusura_macchina", "", {"is_last_phase": True}, "Ordine completato"),
    ("chiusura_parziale_macchina", "In Sospeso", {}, "Avanzamento parziale"),
])
def test_completion_labels_follow_actual_outcome(action, state, payload, expected):
    assert service._event_label(action, state, payload) == expected


@pytest.mark.parametrize("note,state_pre,expected", [
    ("Riattivazione ordine macchina | Matricola: 123", "In Sospeso", "Lavorazione ripresa"),
    ("", "In Sospeso", "Lavorazione ripresa"),
    ("Presa in carico ordine", "Pianificata", "Presa in carico"),
    ("Presa in carico bloccata: distinta materiali assente", "Attivo", "Presa in carico bloccata: distinta materiali assente"),
])
def test_legacy_logs_keep_resume_and_block_distinct_from_takeover(note, state_pre, expected):
    row = InputOdpLog(NoteChiusura=note, StatoOrdinePre=state_pre, StatoOrdinePost="Attivo")
    assert service._input_event_label(row) == expected


def test_history_separates_event_author_owner_phase_and_documents(history_app):
    with history_app.app_context():
        take = runtime_event("presa_in_carico", "08")
        suspend = runtime_event("sospensione", "09", "admin", operatore_in_carico="mario", causale="Materiale mancante")
        suspend.StatoOrdinePost = "In Sospeso"
        resume = runtime_event("riattivazione", "10", "luigi")
        close = runtime_event("chiusura_finale", "11", "admin", fase_successiva="30")
        close.FasePost, close.StatoOrdinePost, close.TempoFunzionamentoPost = "30", "Pianificata", "0"
        close.QuantitaConforme = "5"
        document = InputOdpLog(
            IdDocumento="DOC", IdRiga="1", RifRegistraz="2026.123",
            OperationGroupId=close.OperationGroupId, ClosedAt=close.EventAt,
            FaseConsuntivata="20", FaseAttiva="30", TempoFunzionamentoFinale="1.5",
            QuantitaConforme="5", StatoOrdinePost="Pianificata", ClosedBy="admin",
        )
        db.session.add_all([take, suspend, resume, close, document])
        db.session.commit()
        result = service.build_storico_ordini_detail({"id_documento": "DOC", "id_riga": "1"})
        assert len(result["timeline"]) == 4  # Il documento non duplica la chiusura.
        paused, closed = result["timeline"][1], result["timeline"][-1]
        assert paused["utente"] == "admin"
        assert paused["operatore_in_carico"] == "mario"
        assert paused["causale"] == "Materiale mancante"
        assert closed["utente"] == "admin"
        assert closed["operatore_in_carico"] == "luigi"
        assert closed["presa_in_carico_ricostruita"] is True
        assert closed["fase"] == "20"
        assert closed["fase_successiva"] == "30"
        assert closed["tempo_lavorazione_ore"] == "1.5"
        assert closed["azione_label"] == "Fase completata"
        assert closed["documenti"][0]["fase"] == "20"
        assert result["header"]["title"] == "2026.123"


def test_event_filters_match_runtime_and_legacy_using_same_categories(history_app):
    with history_app.app_context():
        runtime = runtime_event("riattivazione_macchina", "10")
        legacy = InputOdpLog(
            IdDocumento="OLD", IdRiga="1", RifRegistraz="2026.100", CodArt="OLD-ART",
            OperationGroupId="",
            ClosedAt="2026-10-30T09:00:00", NoteChiusura="Riattivazione ordine",
            StatoOrdinePre="In Sospeso", StatoOrdinePost="Attivo", ClosedBy="luigi",
        )
        db.session.add_all([runtime, legacy])
        db.session.commit()
        result = service.build_storico_ordini_list({"evento": "riattivazione"})
        assert result["total"] == 2
        assert {row["last_event"] for row in result["rows"]} == {"Lavorazione ripresa"}
        assert service.build_storico_ordini_list({"evento": "presa_in_carico"})["total"] == 0


def test_group_passages_combine_orders_but_keep_causes_and_time_records(history_app):
    with history_app.app_context():
        rows = [runtime_event("sospensione_gruppo", "09", row=str(n), group_uid="GROUP", causale=f"Causale {n}") for n in (1, 2)]
        rows.append(runtime_event("runtime_gruppo", "09", group_uid="GROUP"))
        db.session.add_all(rows)
        db.session.commit()
        timeline = [service._runtime_to_dict(row) for row in rows]
        service._prepare_timeline(timeline, [], [], [])
        steps = service._timeline_steps(timeline)
        assert len(steps) == 1
        assert len(steps[0]["eventi"]) == 2
        assert {e["causale"] for e in steps[0]["eventi"]} == {"Causale 1", "Causale 2"}
        assert all(e["operatore_in_carico"] == "" for e in timeline)
        assert timeline[-1]["tecnico"] is True
        assert len(service._timeline_steps(timeline[:1] * 2)) == 2


def test_latest_passage_does_not_get_replaced_by_technical_time_record(history_app):
    with history_app.app_context():
        db.session.add_all([runtime_event("sospensione", "09"), runtime_event("runtime_gruppo", "10")])
        db.session.commit()
        row = service.build_storico_ordini_list({})["rows"][0]
        assert row["last_event"] == "Lavorazione sospesa"
        assert row["last_operator"] == "mario"
        assert row["last_phase"] == "20"


def test_group_window_does_not_import_events_or_documents_of_next_group(history_app):
    with history_app.app_context():
        group = OdpWorkGroup(GroupUid="OLD", GroupType="MULTIPLO", CreatedAt="2026-10-30T08:00:00", ClosedAt="2026-10-30T10:00:00")
        member = OdpWorkGroupMember(GroupUid="OLD", IdDocumento="DOC", IdRiga="1", CreatedAt=group.CreatedAt)
        own = runtime_event("sospensione_gruppo", "09", group_uid="OLD")
        following = runtime_event("presa_in_carico_gruppo", "10", group_uid="NEW")
        following.EventAt = "2026-10-30T10:03:00"
        foreign_document = InputOdpLog(IdDocumento="DOC", IdRiga="1", OperationGroupId=following.OperationGroupId, ClosedAt=following.EventAt, StatoOrdinePost="Attivo")
        foreign_lot = LottiGeneratiLog(IdDocumento="DOC", IdRiga="1", OperationGroupId=following.OperationGroupId, ClosedAt=following.EventAt, RifLottoAlfa="NEW-LOT", CodArt="ART", Quantita="1", Fase="20")
        db.session.add_all([group, member, own, following, foreign_document, foreign_lot])
        db.session.commit()
        result = service.build_storico_ordini_detail({"kind": "group", "group_uid": "OLD"})
        assert [event["operation_group_id"] for event in result["timeline"]] == [own.OperationGroupId]
        assert result["input_logs"] == []
        assert result["lotti_generati"] == []


def test_legacy_suspension_cause_is_visible_without_opening_technical_data():
    row = InputOdpLog(IdDocumento="DOC", IdRiga="1", StatoOrdinePost="In Sospeso", NoteChiusura="Sospensione ordine | Causale: Materiale mancante")
    assert service._input_timeline_to_dict(row)["causale"] == "Materiale mancante"


def test_lot_search_lists_materials_before_showing_history(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            GiacenzaLotti(CodArt="ART-A", RifLottoAlfa="LOT-1", CodMag="A", Giacenza="2"),
            GiacenzaLotti(CodArt="ART-A", RifLottoAlfa="LOT-1", CodMag="B", Giacenza="3"),
            GiacenzaMateriale(CodArt="ART-A", CodMag="A", Giacenza="5", RifLottoAlfa="", DesArt="Materiale A"),
            LottiUsatiLog(OperationGroupId="used-b", IdDocumento="DOC", IdRiga="1", CodArt="ART-B", RifLottoAlfa="LOT-1", Quantita="4"),
            InputOdpLog(OperationGroupId="old-b", IdDocumento="DOC", IdRiga="2", CodArt="ART-B", DesArt="Materiale B archiviato", ClosedAt="2026-10-30T09:00:00"),
            LottiGeneratiLog(OperationGroupId="generated-c", IdDocumento="DOC", IdRiga="3", CodArt="ART-C", RifLottoAlfa="LOT-1", Quantita="10"),
            GiacenzaLotti(CodArt="OTHER", RifLottoAlfa="LOT-10", CodMag="A", Giacenza="100"),
        ])
        db.session.commit()
        result = service.build_storico_lotti_search({"lotto": "LOT-1"})
        assert result["materiali"] == [
            {"codice": "ART-A", "descrizione": "Materiale A"},
            {"codice": "ART-B", "descrizione": "Materiale B archiviato"},
            {"codice": "ART-C", "descrizione": ""},
        ]
        assert result["rows"] == []
        assert result["quantity_summary"] is None
        assert service.build_storico_lotti_search({"lotto": "LOT"})["materiali"] == []


def test_same_lot_multiple_codes_keeps_history_and_quantities_separate(storico_app):
    with storico_app.app_context():
        for code, used, generated, available in [("A", "4", "10", "5"), ("B", "40", "100", "50")]:
            db.session.add_all([
                GiacenzaLotti(CodArt=code, RifLottoAlfa="20261030", CodMag="A", Giacenza=available),
                LottiUsatiLog(OperationGroupId=f"used-{code}", IdDocumento="DOC", IdRiga=code, CodArt=code, RifLottoAlfa="20261030", Quantita=used),
                LottiGeneratiLog(OperationGroupId=f"generated-{code}", IdDocumento="DOC", IdRiga=code, CodArt=code, RifLottoAlfa="20261030", Quantita=generated),
            ])
        db.session.commit()
        for code, quantities in [("A", ("10", "5", "4")), ("B", ("100", "50", "40"))]:
            result = service.build_storico_lotti_search({"lotto": "20261030", "articolo": code})
            assert result["articolo"] == code
            assert len(result["rows"]) == 2
            assert all(row["articolo"] == code and row["lotto"] == "20261030" for row in result["rows"])
            assert result["quantity_summary"] == dict(zip(
                ("original_quantity", "available_quantity", "used_quantity", "original_source"),
                (*quantities, "generated"),
            ))


def test_alphanumeric_lot_stock_only_is_selectable_and_summed_across_warehouses(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            GiacenzaLotti(CodArt="A", RifLottoAlfa="L_%", CodMag="A", Giacenza="2.5"),
            GiacenzaLotti(CodArt="A", RifLottoAlfa="L_%", CodMag="B", Giacenza="3"),
            GiacenzaLotti(CodArt="A", RifLottoAlfa="L_OTHER", CodMag="A", Giacenza="100"),
        ])
        db.session.commit()
        result = service.build_storico_lotti_search({"lotto": "L_%", "articolo": "A"})
        assert result["rows"] == []
        assert result["quantity_summary"] == {
            "original_quantity": "5.5", "available_quantity": "5.5",
            "used_quantity": "0", "original_source": "reconstructed",
        }


def test_lot_search_rejects_a_code_from_another_lot(storico_app):
    with storico_app.app_context():
        db.session.add(GiacenzaLotti(CodArt="A", RifLottoAlfa="LOT-A", CodMag="A", Giacenza="1"))
        db.session.commit()
        assert service.build_storico_lotti_search({"lotto": "LOT-A", "articolo": "B"})["ok"] is False
        assert service.build_storico_lotti_search({"lotto": ""})["ok"] is False
        assert service.build_storico_lotti_search({"lotto": "UNKNOWN"})["materiali"] == []


def test_material_choices_are_not_truncated_by_the_history_limit(storico_app, monkeypatch):
    monkeypatch.setattr(service, "LOT_SEARCH_LIMIT", 2)
    with storico_app.app_context():
        for n, code in enumerate(["B", "A", "A", "A"]):
            db.session.add(LottiUsatiLog(OperationGroupId=f"op-{n}", IdDocumento="DOC", IdRiga=str(n), CodArt=code, RifLottoAlfa="LOT", Quantita="1", ClosedAt=f"2026-10-30T0{n}:00:00"))
        db.session.commit()
        choices = service.build_storico_lotti_search({"lotto": "LOT"})["materiali"]
        assert [item["codice"] for item in choices] == ["A", "B"]
        result = service.build_storico_lotti_search({"lotto": "LOT", "articolo": "A"})
        assert result["limit_reached"] is True
        assert len(result["rows"]) == 2
        assert result["quantity_summary"]["used_quantity"] == "3"


def test_used_material_does_not_inherit_the_parent_order_description(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            LottiUsatiLog(OperationGroupId="op", IdDocumento="DOC", IdRiga="1", CodArt="COMPONENT", RifLottoAlfa="LOT", Quantita="1"),
            InputOdpLog(OperationGroupId="op", IdDocumento="DOC", IdRiga="1", CodArt="PARENT", DesArt="Macchina completa", ClosedAt="2026-10-30T09:00:00"),
        ])
        db.session.commit()
        assert service.build_storico_lotti_search({"lotto": "LOT"})["materiali"] == [{"codice": "COMPONENT", "descrizione": ""}]


def test_lot_descriptions_come_from_article_master_when_material_stock_table_is_empty(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            GiacenzaLotti(CodArt="A", RifLottoAlfa="LOT", CodMag="A", Giacenza="2"),
            GiacenzaLotti(CodArt="B", RifLottoAlfa="LOT", CodMag="A", Giacenza="3"),
            AcqArticoli(CodArt="A", DesArt="Descrizione articolo A"),
            AcqArticoliLookup(CodArt="B", VarianteArt="", IndiceModifica="", DesArt="Descrizione articolo B"),
        ])
        db.session.commit()
        assert GiacenzaMateriale.query.count() == 0
        assert service.build_storico_lotti_search({"lotto": "LOT"})["materiali"] == [
            {"codice": "A", "descrizione": "Descrizione articolo A"},
            {"codice": "B", "descrizione": "Descrizione articolo B"},
        ]
        selected = service.build_storico_lotti_search({"lotto": "LOT", "articolo": "B"})
        assert selected["descrizione"] == "Descrizione articolo B"


def test_article_master_description_takes_precedence_over_stock_and_old_snapshots(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            GiacenzaLotti(CodArt="A", RifLottoAlfa="LOT", CodMag="A", Giacenza="2"),
            AcqArticoli(CodArt="A", DesArt="Descrizione corrente"),
            AcqArticoliLookup(CodArt="A", VarianteArt="", IndiceModifica="", DesArt="Descrizione lookup"),
            GiacenzaMateriale(CodArt="A", CodMag="A", RifLottoAlfa="", Giacenza="2", DesArt="Descrizione giacenza"),
            InputOdpLog(OperationGroupId="old", IdDocumento="DOC", IdRiga="1", CodArt="A", DesArt="Descrizione storica", ClosedAt="2026-10-30T09:00:00"),
        ])
        db.session.commit()
        assert service.build_storico_lotti_search({"lotto": "LOT"})["materiali"] == [
            {"codice": "A", "descrizione": "Descrizione corrente"},
        ]


def test_empty_master_description_uses_latest_nonempty_lookup_description(storico_app):
    with storico_app.app_context():
        db.session.add_all([
            GiacenzaLotti(CodArt="A", RifLottoAlfa="LOT", CodMag="A", Giacenza="2"),
            AcqArticoli(CodArt="A", DesArt=" "),
            AcqArticoliLookup(CodArt="A", VarianteArt="", IndiceModifica="1", DesArt="Descrizione precedente", synced_at="2026-09-01"),
            AcqArticoliLookup(CodArt="A", VarianteArt="", IndiceModifica="2", DesArt="Descrizione aggiornata", synced_at="2026-10-01"),
            AcqArticoliLookup(CodArt="A", VarianteArt="", IndiceModifica="3", DesArt="", synced_at="2026-10-30"),
        ])
        db.session.commit()
        assert service.build_storico_lotti_search({"lotto": "LOT"})["materiali"][0]["descrizione"] == "Descrizione aggiornata"
