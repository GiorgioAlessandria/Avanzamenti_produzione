from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app_odp.services import ordini_gruppi_service as gruppi


def test_multiplo_di_tre_ordini_diventa_misto_senza_riesportare_l_intervallo():
    members = [
        SimpleNamespace(id=i, IdDocumento=str(i), IdRiga="1", Status="In Sospeso", TimeShareMode="SPLIT")
        for i in (1, 2, 3)
    ]
    multiplo = SimpleNamespace(
        GroupUid="MULT-1", GroupType="MULTIPLO", Note=None, Status="In Sospeso",
        OperatoreUsername="operatore", members=members,
    )
    orders = {
        str(i): SimpleNamespace(
            IdDocumento=str(i), IdRiga="1", RisorsaAttiva="R", CodRisorsaProd="R", NumFase="1",
            StatoOrdine="In Sospeso" if i < 4 else "Pianificata",
        )
        for i in range(1, 5)
    }
    activate = MagicMock()
    create_member = MagicMock()
    blocking = MagicMock(return_value=None)
    selection = [{"id_documento": "4", "id_riga": "1"}]
    time_exports = []
    with patch.multiple(
        gruppi,
        db=MagicMock(), OdpWorkGroup=SimpleNamespace,
        _get_group_or_error=lambda uid: multiplo,
        _get_visible_odp_by_key=lambda policy, doc, row: orders[doc],
        _current_username=lambda: "operatore", _current_user_id=lambda: 1,
        _lock_lct_order_runtime=MagicMock(), _ensure_operator_can_activate_group=MagicMock(),
        _ensure_order_can_enter_group=MagicMock(), _order_reparto=lambda order: "R1",
        _order_kind=lambda order: "SL", _fase_corrente_for_export=lambda order: "1",
        _get_blocking_outbox_for_phase=blocking, _resource_codes_available=lambda codes: {"RMAS"} & codes,
        _activate_order_for_group=activate, _create_member=create_member,
        _group_uid=lambda prefix, orders: "MIX-1",
    ):
        blocking.return_value = object()
        try:
            gruppi.create_misto_group([], selection, None, time_exports=time_exports, source_group_uid="MULT-1")
        except ValueError as exc:
            assert "export AVP in attesa" in str(exc)
        else:
            raise AssertionError("Attendere la pubblicazione degli AVP del multiplo.")
        assert multiplo.Status == "In Sospeso"
        activate.assert_not_called()

        blocking.return_value = None
        misto = gruppi.create_misto_group(
            [], selection, None, time_exports=time_exports, source_group_uid="MULT-1",
        )

    assert multiplo.Status == "Sciolto"
    assert [m.Status for m in members] == ["Sciolto"] * 3
    assert misto.GroupUid == "MIX-1" and misto.Status == "Attivo"
    assert misto.Note == "MISTO" and misto.InitialMemberCount == 3
    assert [call.kwargs["role"] for call in create_member.call_args_list] == ["MEMBER"] * 3 + ["MASKED"]
    assert [call.args[0].IdDocumento for call in activate.call_args_list] == ["1", "2", "3", "4"]
    assert time_exports == []


def test_multiplo_di_un_altro_operatore_compare_ma_non_si_puo_convertire():
    members = [
        SimpleNamespace(id=i, IdDocumento=str(i), IdRiga="1", Status="Attivo", TimeShareMode="SPLIT")
        for i in (1, 2)
    ]
    multiplo = SimpleNamespace(
        GroupUid="MIX-ricomposto", GroupType="MULTIPLO", Note="MISTO", Status="Attivo",
        OperatoreUsername="altro_operatore", members=members,
    )
    members.append(SimpleNamespace(id=3, Status="Chiuso", TimeShareMode="ZERO"))
    model = MagicMock()
    model.query.filter_by.return_value.filter.return_value.all.return_value = [multiplo]
    orders = {
        str(i): SimpleNamespace(IdDocumento=str(i), IdRiga="1", StatoOrdine="Attivo")
        for i in (1, 2)
    }
    user = MagicMock(return_value="admin")
    base_query = MagicMock()
    base_query.return_value.all.return_value = []
    with patch.multiple(
        gruppi,
        OdpWorkGroup=model, _base_odp_query=base_query,
        _current_username=user, _get_group_or_error=lambda _: multiplo,
        _get_visible_odp_by_key=lambda policy, doc, row: orders[doc],
        _lock_lct_order_runtime=MagicMock(), _order_label=lambda ordine: ordine.IdDocumento,
        _order_kind=lambda _: "SL", _order_reparto=lambda _: "R1",
    ):
        assert gruppi.available_orders_payload(None) == []
        [row] = gruppi.available_orders_payload(None, include_multipli=True)
        assert row["ordine"] == "Multiplo · 1 + 2"
        assert row["operatore"] == "altro_operatore"
        assert row["motivo_blocco"] == "Solo l'operatore assegnato può creare il misto da questo multiplo."
        assert "OperatoreUsername" not in model.query.filter_by.call_args.kwargs
        try:
            gruppi.prepare_misto_selection([], [], None, source_group_uid=multiplo.GroupUid)
        except ValueError as exc:
            assert str(exc) == row["motivo_blocco"]
        else:
            raise AssertionError("La conversione resta riservata all'operatore assegnato.")

        user.return_value = "altro_operatore"
        [row] = gruppi.available_orders_payload(None, include_multipli=True)
        assert row["motivo_blocco"] == ""

        members[-1].Status = "Attivo"
        assert gruppi.available_orders_payload(None, include_multipli=True) == []
