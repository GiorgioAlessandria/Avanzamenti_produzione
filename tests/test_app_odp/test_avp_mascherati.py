from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app_odp.services import ordini_gruppi_service as gruppi


def test_mascherati_usano_mas_e_il_principale_mantiene_la_risorsa_originale():
    start = "2026-11-17T09:00:00+01:00"
    end = datetime.fromisoformat("2026-11-17T10:00:00+01:00")
    members = [
        SimpleNamespace(
            id=i, IdDocumento=str(i), IdRiga="1", Fase="2", Status="Attivo",
            Role="MAIN" if i == 1 else "MASKED", TimeShareMode="FULL" if i == 1 else "ZERO",
            RuntimeSecondsAssigned=0,
        )
        for i in range(1, 4)
    ]
    group = SimpleNamespace(
        GroupUid="MASK-test", GroupType="MASCHERATO", Note=None, Status="Attivo",
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
    lookup.side_effect = [[("BURATTOMAS",)], [("BURATTOMAS",), ("LAVAGGIOMAS",)]]
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
            gruppi._consuntiva_intervallo_gruppo(group, end, time_exports=time_exports)
        except ValueError as exc:
            assert str(exc) == "Manca risorsa mascherata, segnalarlo all'ufficio tecnico"
        else:
            raise AssertionError("La risorsa MAS mancante deve bloccare l'operazione.")
        assert group.LastActivationAt == start
        assert [runtime.Tempo_funzionamento for runtime in runtimes.values()] == ["2"] * 3
        assert time_exports == []
        queue.assert_not_called()

        closing = gruppi._consuntiva_intervallo_gruppo(
            group, end, time_exports=time_exports, closing=("1", "1"),
            minuti_non_funzionamento=15,
        )
        assert closing == {("1", "1"): {"risorsa_export": "FRESA", "tempo_gruppo": "0.75"}}
        assert time_exports == [10, 11]
        assert [call.args[2] for call in queue.call_args_list] == ["0.75", "0.75"]
        assert [call.kwargs["risorsa"] for call in queue.call_args_list] == ["BURATTOMAS", "LAVAGGIOMAS"]
        assert [member.RuntimeSecondsAssigned for member in members] == [2700] * 3
        assert [order.RisorsaAttiva for order in orders.values()] == ["FRESA", "BURATTO", "LAVAGGIO"]

        members[0].Status = "Chiuso"
        gruppi.finalize_group_after_single_member_closure(group, members[0])
        assert gruppi.is_pure_multiple(group)
        assert group.Status == "Attivo" and group.InitialMemberCount == 2
        assert [runtime.data_ultima_attivazione for runtime in tuple(runtimes.values())[1:]] == [
            end.isoformat(), end.isoformat(),
        ]


def test_la_regola_mas_non_si_applica_ai_misti():
    for group_type, uid, note in (
        ("MULTIPLO", "MIX-test", "MISTO"),
        ("MASCHERATO", "MIX-test", "MISTO"),
        ("MASCHERATO", "MIX-test", None),
    ):
        group = SimpleNamespace(GroupType=group_type, GroupUid=uid, Note=note, members=[])
        assert not gruppi.uses_group_interval_export(group)
