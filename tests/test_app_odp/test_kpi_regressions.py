from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app_odp.services import dashboard_service as service
from app_odp.services import kpi_service as kpi
from app_odp.services import ordini_log_service as logs
from app_odp.tasks import generate_kpi_snapshot as task


def closure(group, when, *, phase="1", partial=False, good="3", rejected="0", **overrides):
    runtime = SimpleNamespace(
        OperationGroupId=group, IdDocumento="D", IdRiga="1", log_id=int(group),
        Azione="chiusura_parziale" if partial else "chiusura_finale", EventAt=when,
        DataInCaricoPre="2026-07-01T08:00:00+02:00" if phase == "1" else "2026-07-03T08:00:00+02:00",
        StatoOrdinePost="Pianificata" if partial else "Chiusa", UtenteOperazione="mario",
    )
    values = dict(
        ClosedAt=when, FaseConsuntivata=phase, FaseAttiva=phase,
        NumFase='["1", "2"]', CodReparto='["10", "70"]',
        CodRisorsaProd='["R1", "R2"]', CodLavorazione='["L1", "L2"]',
        TempoPrevistoLavoraz='["20", "20"]', TempoFunzionamentoFinale="1",
        QuantitaConforme=good, QuantitaNonConforme=rejected,
        ChiusuraParziale="si" if partial else "no", CodArt="A",
        StatoOrdinePost=runtime.StatoOrdinePost, DataFineSched='["2026-07-03"]',
    )
    values.update(overrides)
    return runtime, SimpleNamespace(**values)


def test_expected_time_uses_minutes_per_unit_and_rejects():
    _, log = closure("1", "2026-07-03T09:00:00+02:00", good="2", rejected="1")
    assert kpi._kpi_tempo_previsto_ore(log) == 1.0
    assert task._tempo_previsto_ore(log) == 1.0


@pytest.mark.parametrize("raw", ["0", 0, "0,00"])
def test_zero_runtime_does_not_restore_removed_downtime(raw):
    runtime = SimpleNamespace(TempoFunzionamentoPost="0", ElapsedSeconds="3600", TempoNonFunzionamentoSecondi="3600")
    log = SimpleNamespace(TempoFunzionamentoFinale=raw)
    assert kpi._kpi_tempo_reale_ore(runtime, log) == 0.0
    assert task._tempo_reale_ore(runtime, log) == 0.0


def test_missing_runtime_fallback_subtracts_downtime():
    assert kpi._kpi_tempo_reale_ore(SimpleNamespace(ElapsedSeconds="3600", TempoNonFunzionamentoSecondi="600"), None) == 5 / 6


def test_phase_lists_keep_empty_positions_and_normalize_float_phase_codes():
    for read in (kpi._kpi_active_value_from_list, task._active_value_from_list):
        assert read('["", "R2", "R3"]', '[1.0, 2.0, 3.0]', "2") == "R2"
        assert read('["A", "B"]', '[10.0, 20.0]', "20") == "B"
        assert read('["R1", ""]', '[1, 2]', "2") == ""


def test_closed_phase_ignores_next_phase_active_fields():
    _, log = closure("1", "2026-07-03T09:00:00+02:00", FaseAttiva="2", RisorsaAttiva="R2", LavorazioneAttiva="L2")
    assert kpi._kpi_risorsa_for_log(log) == "R1"
    assert kpi._kpi_lavorazione_for_log(log) == "L1"
    assert task._risorsa_for_log(log) == "R1"


def test_dropdown_filters_match_whole_codes():
    assert not service._kpi_matches_filters({"reparto": "170"}, {"reparto": "70"})
    assert not service._kpi_matches_filters({"operatore": "mario.rossi"}, {"operatore": "mario"})
    assert service._kpi_matches_filters({"operatore": "Mario"}, {"operatore": "mario"})


def test_history_collects_partials_and_earliest_phase_start_in_any_order():
    partial = closure("1", "2026-07-01T09:00:00+02:00", partial=True, good="1", rejected="1")
    final = closure("2", "2026-07-02T09:00:00+02:00", good="1")
    next_phase = closure("3", "2026-07-03T09:00:00+02:00", phase="2", good="2")
    starts, quantities = kpi._kpi_history([next_phase, final, partial])
    assert quantities[("2", "D", "1")] == 3
    assert quantities[("3", "D", "1")] == 2
    assert kpi._kpi_lead_time_ore(next_phase[0], datetime.fromisoformat(next_phase[1].ClosedAt), starts[("D", "1")]) == 49
    assert kpi._kpi_event_is_eligible(*partial)
    assert not kpi._kpi_is_order_completed(*partial)


def test_partial_machine_with_missing_components_does_not_duplicate_units():
    partial = closure("1", "2026-07-01T09:00:00+02:00", partial=True)
    partial[0].Azione = "chiusura_parziale_macchina"
    final = closure("2", "2026-07-02T09:00:00+02:00")
    _, quantities = kpi._kpi_history([partial, final])
    assert quantities[("2", "D", "1")] == 3
    assert kpi._kpi_quantita_consuntivata(partial[1], partial[0]) == (0.0, 0.0)


class Query:
    def __init__(self, rows):
        self.rows = rows

    def outerjoin(self, *args):
        return self

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def filter_by(self, **kwargs):
        return self

    def all(self):
        return self.rows


def prepare_dashboard(monkeypatch, rows, *, date_from="2026-07-01", date_to="2026-07-03"):
    monkeypatch.setattr(service, "db", SimpleNamespace(session=SimpleNamespace(query=lambda *args: Query(rows))))
    monkeypatch.setattr(service, "request", SimpleNamespace(args={"date_from": date_from, "date_to": date_to}))
    monkeypatch.setattr(service, "_dashboard_today", lambda: date(2026, 7, 3))
    monkeypatch.setattr(service, "_dashboard_filter_label_maps", lambda: {})
    monkeypatch.setattr(service, "_dashboard_seed_master_filter_options", lambda *args: None)
    monkeypatch.setattr(service, "_dashboard_collect_filter_options_from_row", lambda *args: None)
    monkeypatch.setattr(service, "_dashboard_reparti_label_map", lambda: {})


def test_dashboard_partial_quality_and_cumulative_times_are_counted_once(monkeypatch):
    rows = [closure("1", "2026-07-01T09:00:00+02:00", partial=True, good="1", rejected="1"),
            closure("2", "2026-07-03T09:00:00+02:00", good="1", TempoFunzionamentoFinale="1")]
    prepare_dashboard(monkeypatch, rows)
    payload = service._build_dashboard_kpi_payload(detail_limit=None)
    cards = payload["cards"]
    assert cards["quantita_consuntivata"] == 3
    assert cards["quantita_non_conforme"] == 1
    assert cards["tasso_non_conformita"] == 33.33
    assert cards["fasi_totali"] == 1
    assert cards["ordini_chiusi"] == 1
    assert cards["tempo_previsto_totale"] == 1
    assert cards["tempo_reale_totale"] == 1
    assert payload["aggregati"]["risorse"][0]["fasi_consuntivate"] == 1


def test_period_filters_do_not_cut_off_phase_quantities_or_order_start(monkeypatch):
    rows = [closure("1", "2026-07-01T09:00:00+02:00", partial=True, good="1", rejected="1"),
            closure("2", "2026-07-03T09:00:00+02:00", good="1")]
    prepare_dashboard(monkeypatch, rows, date_from="2026-07-03")
    cards = service._build_dashboard_kpi_payload()["cards"]
    assert cards["quantita_consuntivata"] == 1
    assert cards["quantita_non_conforme"] == 0
    assert cards["tempo_previsto_totale"] == 1
    assert cards["lead_time_medio_ore"] == 49


def test_export_keeps_all_aggregate_groups_and_ranks_before_chart_limit(monkeypatch):
    rows = [closure(str(index + 1), "2026-07-03T09:00:00+02:00", CodArt=f"A{index:02}",
                    RisorsaAttiva=f"R{index:02}", TempoFunzionamentoFinale="10" if index == 50 else "1") for index in range(51)]
    prepare_dashboard(monkeypatch, rows)
    payload = service._build_dashboard_kpi_payload(detail_limit=None)
    assert len(payload["details"]) == 51
    assert len(payload["aggregati"]["articoli"]) == 51
    assert payload["charts"]["top_risorse_scostamento"][0]["label"] == "R50"


def test_snapshot_matches_live_cumulative_phase_times(monkeypatch):
    rows = [closure("1", "2026-07-01T09:00:00+02:00", partial=True, good="1", rejected="1"),
            closure("2", "2026-07-03T09:00:00+02:00", good="1")]
    records = []
    monkeypatch.setattr(task, "db", SimpleNamespace(session=SimpleNamespace(query=lambda *args: Query(rows), commit=lambda: None)))
    monkeypatch.setattr(task, "ProductionKpiSnapshot", SimpleNamespace(query=Query([])))
    monkeypatch.setattr(task, "_upsert_snapshot", lambda record, **kwargs: records.append(record))
    result = task.generate_snapshot_for_month("2026-07")
    global_record = next(record for record in records if record["scope_type"] == "global")
    assert result["eligible_events"] == 1
    assert global_record["tempo_previsto_totale"] == 1
    assert global_record["tempo_reale_totale"] == 1
    assert global_record["ordini_chiusi"] == 1


def test_new_closure_logs_keep_the_closed_phase_fields(monkeypatch):
    rows = []
    order = SimpleNamespace(**{field: "" for field in (
        "IdDocumento", "IdRiga", "RifRegistraz", "CodArt", "DesArt", "Quantita", "NumFase",
        "CodLavorazione", "CodRisorsaProd", "DataInizioSched", "DataFineSched", "GestioneLotto",
        "GestioneMatricola", "DistintaMateriale", "CodMatricola", "StatoRiga", "CodFamiglia",
        "CodMacrofamiglia", "CodMagPrincipale", "CodReparto", "TempoPrevistoLavoraz",
        "CodClassifTecnica", "CodTipoDoc", "FaseAttiva", "QtyDaLavorare", "RisorsaAttiva",
        "LavorazioneAttiva", "AttrezzaggioAttivo", "Note",
    )})
    order.NumFase = "[1, 2]"
    order.FaseAttiva = "2"
    order.CodRisorsaProd = '["R1", "R2"]'
    order.CodLavorazione = '["L1", "L2"]'
    order.TempoAttrezzaggio = '["10", "20"]'
    order.RisorsaAttiva, order.LavorazioneAttiva = "R2", "L2"
    monkeypatch.setattr(logs, "db", SimpleNamespace(session=SimpleNamespace(add=rows.append)))
    logs._add_input_odp_closure_log(operation_group_id="test", ordine=order, fase_consuntivata="1",
        q_ok=Decimal(1), q_nok=Decimal(0), tempo_finale="1", minuti_non_funzionamento=0,
        secondi_non_funzionamento=0, chiusura_parziale=False, note_chiusura="",
        stato_ordine_pre="Attivo", stato_ordine_post="Pianificata", qty_pre="1", qty_post="1",
        closed_by="mario", closed_at="2026-07-03T09:00:00+02:00")
    assert rows[0].FaseAttiva == "1"
    assert rows[0].RisorsaAttiva == "R1"
    assert rows[0].LavorazioneAttiva == "L1"
    assert rows[0].AttrezzaggioAttivo == "10"


def test_serial_comes_from_snapshot_or_runtime_payload_and_keeps_leading_zeros():
    runtime = SimpleNamespace(Azione="chiusura_macchina", PayloadJson='{"matricola":" 000123 "}')
    assert kpi._kpi_matricola_for_log(runtime, None) == "000123"
    assert kpi._kpi_macchine_prodotte(runtime, None) == 1.0
    assert kpi._kpi_matricola_for_log(runtime, SimpleNamespace(CodMatricola=" M001 ")) == "M001"
    runtime.PayloadJson = "invalid"
    assert kpi._kpi_macchine_prodotte(runtime, None) == 0.0
    runtime.PayloadJson = "[]"
    assert kpi._kpi_matricola_for_log(runtime, None) == ""


def test_partial_and_deleted_machine_events_do_not_count():
    runtime, log = closure("1", "2026-07-03T09:00:00+02:00", partial=True, CodMatricola="M001")
    assert kpi._kpi_macchine_prodotte(runtime, log) == 0.0
    runtime.Azione, log.ChiusuraParziale = "chiusura_macchina", "no"
    runtime.Topic = "eliminato_gestionale"
    assert kpi._kpi_macchine_prodotte(runtime, log) == 0.0


def test_live_and_snapshot_count_distinct_serials_across_phases_and_orders(monkeypatch):
    rows = [
        closure("1", "2026-07-01T09:00:00+02:00", CodMatricola="M001", StatoOrdinePost="Pianificata"),
        closure("2", "2026-07-02T09:00:00+02:00", phase="2", CodMatricola="M001"),
        closure("3", "2026-07-03T09:00:00+02:00", CodMatricola="M002", GestioneMatricola="no", StatoOrdinePost="Pianificata"),
        closure("4", "2026-07-03T09:00:00+02:00", CodMatricola="", GestioneMatricola="si"),
    ]
    prepare_dashboard(monkeypatch, rows)
    live = service._build_dashboard_kpi_payload(detail_limit=None)
    assert live["cards"]["macchine_prodotte"] == 2
    assert sum(row["macchine_prodotte"] for row in live["details"]) == 2
    records = []
    monkeypatch.setattr(task, "db", SimpleNamespace(session=SimpleNamespace(query=lambda *args: Query(rows), commit=lambda: None)))
    monkeypatch.setattr(task, "ProductionKpiSnapshot", SimpleNamespace(query=Query([])))
    monkeypatch.setattr(task, "_upsert_snapshot", lambda record, **kwargs: records.append(record))
    task.generate_snapshot_for_month("2026-07")
    assert next(record for record in records if record["scope_type"] == "global")["macchine_prodotte"] == 2
    assert next(record for record in records if record["scope_type"] == "reparto" and record["scope_code"] == "10")["macchine_prodotte"] == 2
    assert next(record for record in records if record["scope_type"] == "reparto" and record["scope_code"] == "70")["macchine_prodotte"] == 1


def test_serial_deduplication_is_applied_after_date_and_department_filters(monkeypatch):
    rows = [closure("1", "2026-07-01T09:00:00+02:00", CodMatricola="M001"),
            closure("2", "2026-07-03T09:00:00+02:00", phase="2", CodMatricola="M001")]
    prepare_dashboard(monkeypatch, rows, date_from="2026-07-03")
    service.request.args["reparto"] = "70"
    payload = service._build_dashboard_kpi_payload()
    assert payload["cards"]["macchine_prodotte"] == 1
    assert len(payload["details"]) == 1


def test_machine_total_is_independent_of_final_orders_and_detail_limit(monkeypatch):
    rows = [closure(str(index + 1), "2026-07-03T09:00:00+02:00",
                    CodMatricola=f"M{index:04}", GestioneMatricola="si",
                    StatoOrdinePost="Chiusa" if index < 298 else "Pianificata")
            for index in range(583)]
    prepare_dashboard(monkeypatch, rows)
    payload = service._build_dashboard_kpi_payload()
    assert payload["cards"]["ordini_chiusi"] == 298
    assert payload["cards"]["macchine_prodotte"] == 583
    assert len(payload["details"]) == 500


def test_late_order_sample_reports_missing_due_dates_without_changing_formula(monkeypatch):
    rows = [closure("1", "2026-07-03T09:00:00+02:00", DataFineSched='["2026-07-02"]'),
            closure("2", "2026-07-03T09:00:00+02:00", DataFineSched=""),
            closure("3", "2026-07-03T09:00:00+02:00", partial=True)]
    prepare_dashboard(monkeypatch, rows)
    cards = service._build_dashboard_kpi_payload()["cards"]
    assert cards["ordini_chiusi"] == 2
    assert cards["ordini_con_scadenza"] == 1
    assert cards["percentuale_ritardo"] == 50


def test_excel_summary_explains_indicators_and_distinguishes_missing_data_from_zero(monkeypatch):
    class Sheet:
        def __init__(self):
            self.cells = {}

        def cell(self, row, column, value=None):
            cell = self.cells.setdefault((row, column), SimpleNamespace(value=None))
            if value is not None:
                cell.value = value
            return cell

    monkeypatch.setattr(service, "_autosize_worksheet", lambda sheet: None)
    sheet = Sheet()
    service._write_kpi_summary_sheet(sheet, {"cards": {"ordini_chiusi": 0, "quantita_consuntivata": 100, "tasso_non_conformita": 0}})
    assert sheet.cells[(1, 3)].value == "Come leggere il dato"
    rows = {cell.value: (sheet.cells[(row, 2)].value, sheet.cells[(row, 3)].value)
            for (row, col), cell in sheet.cells.items() if col == 1 and row > 1}
    assert rows["Quantità non conforme (%)"][0] == 0
    assert rows["Tempo medio di attraversamento (ore solari)"][0] == "Non disponibile"
    assert rows["Ordini conclusi in ritardo (%)"][0] == "Non disponibile"
    assert "attese e sospensioni" in rows["Tempo medio di attraversamento (ore solari)"][1]
