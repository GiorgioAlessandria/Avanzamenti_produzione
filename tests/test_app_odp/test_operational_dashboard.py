from datetime import date
from types import SimpleNamespace

from app_odp.services import dashboard_service as service


def open_order(key="1", **changes):
    values = dict(IdDocumento="D", IdRiga=key, RifRegistraz="2026.1.1", NumProgrRiga="1",
                  StatoOrdine="Pianificata", runtime_row=None, FaseAttiva="1", NumFase="[1]",
                  TempoPrevistoLavoraz='["20"]', Quantita="3", QtyDaLavorare="3",
                  CodReparto='["70"]', CodRisorsaProd='["R1"]', CodLavorazione='["L1"]',
                  RisorsaAttiva="R1", LavorazioneAttiva="L1", DataFineSched='["2026-07-03"]',
                  CodArt="A", DesArt="Articolo", DataFinePrevista="")
    values.update(changes)
    return SimpleNamespace(**values)


def prepare(monkeypatch, orders, users=()):
    monkeypatch.setattr(service, "_base_odp_query", lambda: None)
    monkeypatch.setattr(service, "_dashboard_today", lambda: date(2026, 7, 3))
    monkeypatch.setattr(service, "_dashboard_cruscotto_filters_from_request", lambda: {})
    monkeypatch.setattr(service, "_dashboard_filter_label_maps", lambda: {})
    monkeypatch.setattr(service, "_dashboard_seed_master_filter_options", lambda *args: None)
    monkeypatch.setattr(service, "_dashboard_collect_filter_options_from_row", lambda *args: None)
    monkeypatch.setattr(service, "_dashboard_reparti_label_map", lambda: {})
    monkeypatch.setattr(service, "_dashboard_model_label_map", lambda model: {})
    monkeypatch.setattr(service, "_dashboard_capacity_users", lambda *args: list(users))
    monkeypatch.setattr(service, "_capacity_calendar_payload", lambda: [])
    return SimpleNamespace(filter_input_odp=lambda query: SimpleNamespace(all=lambda: orders))


def test_operational_phase_lists_preserve_empty_cells_and_match_nonsequential_phases():
    order = open_order(NumFase="[10.0,20.0]", FaseAttiva="20", CodReparto='["", "70"]',
                       TempoPrevistoLavoraz='["", "20"]', DataFineSched='["", "2026-07-04"]')
    assert service._dashboard_reparto_attivo(order) == "70"
    assert service._dashboard_carico_ore(order) == 1
    assert service._dashboard_data_fine_prevista(order) == date(2026, 7, 4)
    order.FaseAttiva = "10"
    assert service._dashboard_reparto_attivo(order) == ""
    assert service._dashboard_data_fine_prevista(order) is None


def test_operational_dates_follow_runtime_phase():
    order = open_order(NumFase="[1,2]", DataFineSched='["2026-07-03","2026-07-04"]',
                       runtime_row=SimpleNamespace(FaseAttiva="2"))
    assert service._dashboard_data_fine_prevista(order) == date(2026, 7, 4)


def test_no_capacity_produces_no_percentage_instead_of_invented_120(monkeypatch):
    policy = prepare(monkeypatch, [open_order()])
    payload = service._dashboard_build_cruscotto_payload(policy)
    assert payload["cards"]["saturazione_totale"] is None
    assert payload["cards"]["risorse_senza_capacita"] == 1
    assert payload["charts"]["saturazione_risorse"][0]["saturazione"] is None
    assert payload["charts"]["colli_bottiglia_risorse"][0]["saturazione"] is None


def test_zero_load_is_not_a_bottleneck_when_capacity_is_zero(monkeypatch):
    order = open_order(TempoPrevistoLavoraz='["0"]')
    policy = prepare(monkeypatch, [order])
    assert service._dashboard_colli_bottiglia_risorse([order, order], policy) == []


def test_open_orders_returned_twice_do_not_double_totals(monkeypatch):
    order = open_order()
    policy = prepare(monkeypatch, [order, order])
    payload = service._dashboard_build_cruscotto_payload(policy)
    assert payload["cards"]["ordini_totali"] == 1
    assert payload["cards"]["ore_previste_produzione"] == 1
    assert payload["charts"]["colli_bottiglia_risorse"][0]["ore_totali"] == 1


def test_critical_order_counts_are_unique_and_not_limited_to_visible_alerts(monkeypatch):
    orders = [open_order(str(index), TempoPrevistoLavoraz='["0"]', DataFineSched='["2026-07-02"]') for index in range(120)]
    payload = service._dashboard_build_cruscotto_payload(prepare(monkeypatch, orders))
    assert payload["cards"]["ordini_critici"] == 120
    assert payload["cards"]["criticita_totali"] == 240
    assert len(payload["criticita"]) == 100


def test_accumulated_time_is_not_lost_after_reactivation(monkeypatch):
    order = open_order(StatoOrdine="Attivo", runtime_row=SimpleNamespace(Stato_odp="Attivo",
                       Tempo_funzionamento="2", data_ultima_attivazione=None))
    assert service._dashboard_tempo_registrato_ore(order) == 2
    payload = service._dashboard_build_cruscotto_payload(prepare(monkeypatch, [order]))
    assert any(row["tipo"] == "Tempo registrato oltre standard aperto" for row in payload["criticita"])


def test_capacity_filters_match_exact_codes_and_include_operation_filter():
    user = SimpleNamespace(username="mario.rossi", RepartoPrinc="170",
                           risorse=[SimpleNamespace(Codice="R10")], lavorazioni=[SimpleNamespace(Codice="L1")])
    assert not service._dashboard_user_matches_capacity_filters(user, {"operatore": "mario"})
    assert not service._dashboard_user_matches_capacity_filters(user, {"reparto": "70"})
    assert not service._dashboard_user_matches_capacity_filters(user, {"risorsa": "R1"})
    assert not service._dashboard_user_matches_capacity_filters(user, {"lavorazione": "L2"})
    assert service._dashboard_user_matches_capacity_filters(user, {"risorsa": "r10", "lavorazione": "l1"})


def test_operator_with_explicit_capacity_but_no_department_is_not_dropped(monkeypatch):
    user = SimpleNamespace(id=1, username="mario", RepartoPrinc="")
    policy = prepare(monkeypatch, [], [user])
    monkeypatch.setattr(service, "_dashboard_capacity_for_operator", lambda user: {4: 8})
    totals, count = service._dashboard_capacity_by_weekday_for_policy(policy)
    departments = service._dashboard_capacity_by_reparto_today_for_policy(policy)
    assert totals[4] == 8
    assert count == 1
    assert departments["-"]["capacita"] == 8


def test_capacity_filters_include_inherited_roles_but_keep_user_restrictions():
    role = SimpleNamespace(risorse=[SimpleNamespace(Codice="R1"), SimpleNamespace(Codice="R2")],
                           lavorazioni=[SimpleNamespace(Codice="L1")])
    user = SimpleNamespace(risorse=[], lavorazioni=[], _iter_roles=lambda: iter([role]))
    assert service._dashboard_user_matches_capacity_filters(user, {"risorsa": "R1", "lavorazione": "L1"})
    user.risorse = [SimpleNamespace(Codice="R2")]
    assert not service._dashboard_user_matches_capacity_filters(user, {"risorsa": "R1"})
    assert service._dashboard_user_matches_capacity_filters(user, {"risorsa": "R2"})


def test_empty_capacity_window_stays_empty():
    assert service._dashboard_capacity_hours_from_weekday({4: 8}, days=[]) == 0


def test_standard_load_is_not_presented_as_actual_remaining_time():
    order = open_order(runtime_row=SimpleNamespace(Tempo_funzionamento="0.5"))
    assert service._dashboard_carico_ore(order) == 1
