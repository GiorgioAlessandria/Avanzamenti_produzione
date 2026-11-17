from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app_odp.services import ordini_gruppi_service as gruppi


def test_regola_multipla_esclude_misti_e_mascherati():
    split = SimpleNamespace(TimeShareMode="SPLIT")
    zero = SimpleNamespace(TimeShareMode="ZERO")
    group = SimpleNamespace(GroupType="MULTIPLO", Note=None, members=[split, split])
    assert gruppi.is_pure_multiple(group)
    group.members.append(zero)
    assert not gruppi.is_pure_multiple(group)
    group.members.pop()
    group.Note = "MISTO"
    assert not gruppi.is_pure_multiple(group)
    group.Note = None
    group.GroupType = "MASCHERATO"
    assert not gruppi.is_pure_multiple(group)


def test_chiusure_singole_consuntivano_al_numero_precedente_e_l_ultimo_resta_attivo():
    start = "2026-11-17T09:00:00+01:00"
    members = [
        SimpleNamespace(
            id=i, IdDocumento=str(i), IdRiga="1", Fase="2", Role="MEMBER",
            Status="Attivo", TimeShareMode="SPLIT", RuntimeSecondsAssigned=0,
        )
        for i in range(1, 4)
    ]
    group = SimpleNamespace(
        GroupUid="MULT-test", GroupType="MULTIPLO", Note=None, Status="Attivo",
        InitialMemberCount=3, LastActivationAt=start, TotalRuntimeSeconds=0, members=members,
    )
    orders = {
        member.id: SimpleNamespace(
            IdDocumento=member.IdDocumento, IdRiga=member.IdRiga, StatoOrdine="Attivo",
            RisorsaAttiva="BURATTO" if member.id != 2 else "ALTRA",
        )
        for member in members
    }
    runtimes = {
        member.id: SimpleNamespace(
            Tempo_funzionamento="7", data_ultima_attivazione=start, Stato_odp="Attivo"
        )
        for member in members
    }
    db = MagicMock()
    db.session.execute.return_value.rowcount = 1
    db.session.query.return_value.filter.return_value.all.side_effect = [
        [("BURATTOMUL3",)], [],
    ]
    time_docs = []

    def queue_time(ordine, fase, tempo, causale, now_dt, *, risorsa):
        time_docs.append((ordine.IdDocumento, risorsa, tempo))
        return SimpleNamespace(outbox_id=len(time_docs))

    ends = [
        datetime.fromisoformat("2026-11-17T10:00:00+01:00"),
        datetime.fromisoformat("2026-11-17T11:00:00+01:00"),
    ]
    time_exports = []
    with patch.multiple(
        gruppi,
        db=db,
        update=MagicMock(),
        _order_for_member=lambda member: orders[member.id],
        _runtime_for_member=lambda member: runtimes[member.id],
        _append_group_runtime_log=MagicMock(),
        _fase_corrente_for_export=MagicMock(return_value="2"),
        _queue_suspension_export=queue_time,
        _current_username=lambda: "operatore",
        _now_iso=MagicMock(side_effect=[dt.isoformat() for dt in ends]),
    ):
        first = gruppi._consuntiva_intervallo_gruppo(
            group, ends[0], time_exports=time_exports, closing=("1", "1"),
            minuti_non_funzionamento=15,
        )
        assert first == {("1", "1"): {"risorsa_export": "BURATTOMUL3", "tempo_gruppo": "0.75"}}
        assert time_docs == [("2", "ALTRA", "0.25"), ("3", "BURATTOMUL3", "0.75")]
        assert [m.RuntimeSecondsAssigned for m in members] == [2700, 900, 2700]
        members[0].Status = "Chiuso"
        gruppi.finalize_group_after_single_member_closure(group, members[0])
        assert group.InitialMemberCount == 2

        second = gruppi._consuntiva_intervallo_gruppo(
            group, ends[1], time_exports=time_exports, closing=("2", "1"),
        )
        assert second == {("2", "1"): {"risorsa_export": "ALTRA", "tempo_gruppo": "0.5"}}
        assert time_docs[-1] == ("3", "BURATTO", "0.5")
        assert time_exports == [1, 2, 3]
        members[1].Status = "Chiuso"
        gruppi.finalize_group_after_single_member_closure(group, members[1])

    assert group.Status == "Sciolto"
    assert runtimes[3].Stato_odp == orders[3].StatoOrdine == "Attivo"
    assert runtimes[3].data_ultima_attivazione == ends[1].isoformat()
    assert runtimes[3].Tempo_funzionamento == "0"
    assert orders[3].RisorsaAttiva == "BURATTO"


def test_scioglimento_invia_un_documento_tempo_per_membro_attivo():
    members = [SimpleNamespace(id=i, Status="Attivo") for i in (1, 2)]
    group = SimpleNamespace(
        GroupUid="MULT-test", GroupType="MULTIPLO", Status="Attivo", LastActivationAt=None, members=members,
    )
    orders = {
        member.id: SimpleNamespace(IdDocumento=str(member.id), IdRiga="1", RisorsaAttiva="R")
        for member in members
    }
    runtime = SimpleNamespace(Tempo_funzionamento="2", data_ultima_attivazione=None)
    db = MagicMock()
    db.session.execute.return_value.rowcount = 1
    db.session.query.return_value.filter.return_value.all.return_value = [("RMUL2",)]
    queue = MagicMock(side_effect=[SimpleNamespace(outbox_id=i) for i in (10, 11)])
    outbox_ids = []
    end = datetime.fromisoformat("2026-11-17T12:00:00+01:00")
    with patch.multiple(
        gruppi,
        db=db,
        update=MagicMock(),
        _order_for_member=lambda member: orders[member.id],
        _runtime_for_member=lambda _: runtime,
        assign_elapsed_group_runtime=MagicMock(return_value=1800),
        _fase_corrente_for_export=MagicMock(return_value="1"),
        _queue_suspension_export=queue,
    ):
        assert gruppi._consuntiva_intervallo_gruppo(group, end, time_exports=outbox_ids) == {}
        assert outbox_ids == [10, 11]
        assert [c.args[:3] for c in queue.call_args_list] == [
            (orders[1], "1", "0.5"), (orders[2], "1", "0.5"),
        ]
        assert all(c.kwargs == {"risorsa": "RMUL2"} for c in queue.call_args_list)

        group.Status = "In Sospeso"
        for member in members:
            member.Status = "In Sospeso"
        gruppi._consuntiva_intervallo_gruppo(group, end, time_exports=outbox_ids)
        assert outbox_ids == [10, 11]
