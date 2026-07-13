from types import SimpleNamespace

from app_odp.services import priorita_service
from app_odp.services.priorita_service import _make_ordine_fase_key


def test_make_ordine_fase_key_strips_values_and_defaults_blank_phase():
    assert _make_ordine_fase_key(" DOC1 ", " 2 ", " ") == ("DOC1", "2", "1")


def test_make_ordine_fase_key_keeps_explicit_phase():
    assert _make_ordine_fase_key("DOC1", "2", " 3 ") == ("DOC1", "2", "3")


def test_compact_priorita_operatore_renumbers_each_priority_band(monkeypatch):
    rows = [SimpleNamespace() for _ in range(5)]

    monkeypatch.setattr(
        priorita_service,
        "_priorita_rows_for_operatore",
        lambda operatore_id: rows if operatore_id == 42 else [],
    )
    monkeypatch.setattr(priorita_service, "_priorita_2_max", lambda: 2)
    monkeypatch.setattr(
        priorita_service,
        "_priority_now_iso",
        lambda: "2026-07-10T10:00:00+02:00",
    )

    priorita_service._compact_priorita_operatore(42)

    assert [
        (row.Priorita, row.Posizione)
        for row in rows
    ] == [(1, 1), (2, 1), (2, 2), (3, 1), (3, 2)]
    assert {row.updated_at for row in rows} == {"2026-07-10T10:00:00+02:00"}


def test_consume_priorita_ordine_removes_all_operators_and_compacts_each_once(
    monkeypatch,
):
    rows = [
        SimpleNamespace(operatore_id=7),
        SimpleNamespace(operatore_id=7),
        SimpleNamespace(operatore_id=8),
    ]
    filters = {}
    deleted = []
    flushes = []
    compacted = []

    def filter_by(**values):
        filters.update(values)
        return SimpleNamespace(all=lambda: rows)

    monkeypatch.setattr(
        priorita_service,
        "OdpPriorita",
        SimpleNamespace(query=SimpleNamespace(filter_by=filter_by)),
    )
    monkeypatch.setattr(
        priorita_service,
        "db",
        SimpleNamespace(
            session=SimpleNamespace(
                delete=deleted.append,
                flush=lambda: flushes.append(True),
            )
        ),
    )
    monkeypatch.setattr(
        priorita_service,
        "_compact_priorita_operatore",
        compacted.append,
    )

    priorita_service._consume_priorita_ordine("DOC1", "2", "3")

    assert filters == {
        "IdDocumento": "DOC1",
        "IdRiga": "2",
        "Fase": "3",
    }
    assert deleted == rows
    assert flushes == [True]
    assert sorted(compacted) == [7, 8]


def test_snapshot_priorita_in_runtime_records_taken_priority():
    stato = SimpleNamespace()
    priorita_row = SimpleNamespace(Priorita="2")
    when_iso = "2026-07-10T10:00:00+02:00"

    priorita_service._snapshot_priorita_in_runtime(
        stato=stato,
        priorita_row=priorita_row,
        operatore_id=7,
        when_iso=when_iso,
    )

    assert stato.PrioritaInCarico == 2
    assert stato.PrioritaOperatoreIdInCarico == 7
    assert stato.PrioritaPresaInCaricoAt == when_iso


def test_snapshot_priorita_in_runtime_clears_fields_without_priority():
    stato = SimpleNamespace(
        PrioritaInCarico=2,
        PrioritaOperatoreIdInCarico=7,
        PrioritaPresaInCaricoAt="old",
    )

    priorita_service._snapshot_priorita_in_runtime(
        stato=stato,
        priorita_row=None,
        operatore_id=7,
        when_iso="2026-07-10T10:00:00+02:00",
    )

    assert stato.PrioritaInCarico is None
    assert stato.PrioritaOperatoreIdInCarico is None
    assert stato.PrioritaPresaInCaricoAt is None


def test_restore_priorita_for_next_phase_keeps_priority_and_appends(monkeypatch):
    added = []

    def fake_priorita(**values):
        return SimpleNamespace(**values)

    fake_priorita.Posizione = object()
    fake_priorita.query = SimpleNamespace(
        filter_by=lambda **_: SimpleNamespace(first=lambda: None)
    )
    max_query = SimpleNamespace(
        filter_by=lambda **_: SimpleNamespace(scalar=lambda: 4)
    )
    session = SimpleNamespace(query=lambda _: max_query, add=added.append)

    monkeypatch.setattr(priorita_service, "OdpPriorita", fake_priorita)
    monkeypatch.setattr(
        priorita_service,
        "func",
        SimpleNamespace(max=lambda _: None),
    )
    monkeypatch.setattr(
        priorita_service,
        "db",
        SimpleNamespace(session=session),
    )
    monkeypatch.setattr(
        priorita_service,
        "_priority_now_iso",
        lambda: "2026-07-10T10:00:00+02:00",
    )
    monkeypatch.setattr(
        priorita_service,
        "_current_username",
        lambda fallback: fallback,
    )

    priorita_service._restore_priorita_for_next_phase_from_runtime(
        stato=SimpleNamespace(
            PrioritaInCarico=2,
            PrioritaOperatoreIdInCarico=7,
        ),
        ordine=SimpleNamespace(IdDocumento="DOC1", IdRiga="2"),
        next_phase="3",
    )

    assert len(added) == 1
    row = added[0]
    assert (
        row.operatore_id,
        row.IdDocumento,
        row.IdRiga,
        row.Fase,
        row.Priorita,
        row.Posizione,
    ) == (7, "DOC1", "2", "3", 2, 5)


def test_apply_priorita_to_ordini_sorts_by_priority_position_date_and_order(
    monkeypatch,
):
    def ordine(documento, data_fine, riferimento):
        return SimpleNamespace(
            IdDocumento=documento,
            IdRiga="1",
            FaseAttiva="1",
            DataFineSched=data_fine,
            RifRegistraz=riferimento,
        )

    ordini = [
        ordine("NONE", "2026-01-01", "A0"),
        ordine("P2-LATE", "2026-07-12", "B"),
        ordine("P3", "2026-07-01", "A"),
        ordine("P2-EARLY-Z", "2026-07-11", "Z"),
        ordine("P1", "2026-07-20", "Z"),
        ordine("P2-POS2", "2026-07-01", "A"),
        ordine("P2-EARLY-A", "2026-07-11", "A"),
    ]
    priorita_map = {
        ("P1", "1", "1"): SimpleNamespace(Priorita=1, Posizione=1),
        ("P2-LATE", "1", "1"): SimpleNamespace(Priorita=2, Posizione=1),
        ("P3", "1", "1"): SimpleNamespace(Priorita=3, Posizione=1),
        ("P2-EARLY-Z", "1", "1"): SimpleNamespace(Priorita=2, Posizione=1),
        ("P2-POS2", "1", "1"): SimpleNamespace(Priorita=2, Posizione=2),
        ("P2-EARLY-A", "1", "1"): SimpleNamespace(Priorita=2, Posizione=1),
    }

    monkeypatch.setattr(
        priorita_service,
        "_priorita_map_for_operatore",
        lambda operatore_id: priorita_map if operatore_id == 7 else {},
    )

    result = priorita_service._apply_priorita_to_ordini(ordini, operatore_id=7)

    assert [ordine.IdDocumento for ordine in result] == [
        "P1",
        "P2-EARLY-A",
        "P2-EARLY-Z",
        "P2-LATE",
        "P2-POS2",
        "P3",
        "NONE",
    ]

