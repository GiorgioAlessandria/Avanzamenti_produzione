from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app_odp.routes_modules.ordini import _tempo_chiusura_mascherato_ore
from app_odp.services import ordini_gruppi_service as gruppi


@pytest.mark.parametrize("group_type, uid, note, roles, modes, closing", [
    ("MASCHERATO", "MASK-test", None, ["MAIN", "MASKED", "MASKED"], ["FULL", "ZERO", "ZERO"], ("1", "1")),
    ("MULTIPLO", "MIX-test", "MISTO", ["MEMBER", "MEMBER", "MASKED"], ["SPLIT", "SPLIT", "ZERO"], None),
])
def test_intervallo_esporta_solo_i_principali(group_type, uid, note, roles, modes, closing):
    start = "2026-11-17T09:00:00+01:00"
    end = datetime.fromisoformat("2026-11-17T10:00:00+01:00")
    members = [
        SimpleNamespace(
            id=i, IdDocumento=str(i), IdRiga="1", Fase="2", Status="Attivo",
            Role=roles[i - 1], TimeShareMode=modes[i - 1],
            RuntimeSecondsAssigned=0,
        )
        for i in range(1, 4)
    ]
    group = SimpleNamespace(
        GroupUid=uid, GroupType=group_type, Note=note, Status="Attivo",
        InitialMemberCount=3, LastActivationAt=start, TotalRuntimeSeconds=0, members=members,
    )
    orders = {
        i: SimpleNamespace(IdDocumento=str(i), IdRiga="1", RisorsaAttiva=resource, StatoOrdine="Attivo")
        for i, resource in enumerate(("FRESA", "BURATTO", "LAVAGGIO"), start=1)
    }
    runtimes = {
        i: SimpleNamespace(Tempo_funzionamento="2", data_ultima_attivazione=start, Stato_odp="Attivo")
        for i in orders
    }
    db = MagicMock()
    db.session.execute.return_value.rowcount = 1
    lookup = db.session.query.return_value.filter.return_value.all
    lookup.side_effect = [[], []]
    queue = MagicMock(side_effect=[SimpleNamespace(outbox_id=i) for i in (10, 11)])
    time_exports = []
    with patch.multiple(
        gruppi,
        db=db, update=MagicMock(),
        _order_for_member=lambda member: orders[member.id],
        _runtime_for_member=lambda member: runtimes[member.id],
        _append_group_runtime_log=MagicMock(),
        _fase_corrente_for_export=MagicMock(return_value="2"),
        _queue_suspension_export=queue,
        _current_username=lambda: "operatore",
        _now_iso=lambda: end.isoformat(),
    ):
        try:
            gruppi._consuntiva_intervallo_gruppo(group, end, time_exports=time_exports, close_all=True)
        except ValueError as exc:
            assert str(exc) == "Manca risorsa mascherata, segnalarlo all'ufficio tecnico"
        else:
            raise AssertionError("La risorsa MAS mancante deve bloccare l'operazione.")
        assert group.LastActivationAt == start
        assert [runtime.Tempo_funzionamento for runtime in runtimes.values()] == ["2"] * 3
        assert time_exports == []
        queue.assert_not_called()

        rates = gruppi._consuntiva_intervallo_gruppo(
            group, end, time_exports=time_exports, closing=closing,
            minuti_non_funzionamento=15,
        )
        if closing:
            assert rates == {closing: {"risorsa_export": "FRESA", "tempo_gruppo": "0.75"}}
            assert time_exports == []
            queue.assert_not_called()
            assert [member.RuntimeSecondsAssigned for member in members] == [2700, 0, 0]
        else:
            assert rates == {}
            assert time_exports == [10, 11]
            assert [call.args[2] for call in queue.call_args_list] == ["0.38", "0.38"]
            assert [call.kwargs["risorsa"] for call in queue.call_args_list] == ["FRESA", "BURATTO"]
            assert [member.RuntimeSecondsAssigned for member in members] == [1350, 1350, 0]
        assert [order.RisorsaAttiva for order in orders.values()] == ["FRESA", "BURATTO", "LAVAGGIO"]

        if closing:
            members[0].Status = "Chiuso"
            gruppi.finalize_group_after_single_member_closure(group, members[0])
            assert gruppi.is_pure_multiple(group)
            assert group.Status == "Attivo" and group.InitialMemberCount == 2
            assert [runtime.data_ultima_attivazione for runtime in tuple(runtimes.values())[1:]] == [
                end.isoformat(), end.isoformat(),
            ]


def test_tempo_ciclo_proposto_per_fase_e_calcolato_sui_pezzi_della_chiusura():
    ordine = SimpleNamespace(NumFase='["10", "20", "30"]', TempoPrevistoLavoraz='[10.5, null, 0]')
    assert gruppi._tempo_ciclo_teorico_minuti(ordine, "10") == 11
    assert gruppi._tempo_ciclo_teorico_minuti(ordine, "20") is None
    assert gruppi._tempo_ciclo_teorico_minuti(ordine, "30") is None
    assert _tempo_chiusura_mascherato_ore("10", Decimal(2) + Decimal(1)) == "0.5"
    assert _tempo_chiusura_mascherato_ore("11", Decimal(1)) == "0.1833"


@pytest.mark.parametrize("minuti", [None, "", "0", "-1", "1.5"])
def test_chiusura_mascherato_richiede_minuti_interi_positivi(minuti):
    with pytest.raises(ValueError, match="tempo ciclo"):
        _tempo_chiusura_mascherato_ore(minuti, Decimal(3))
