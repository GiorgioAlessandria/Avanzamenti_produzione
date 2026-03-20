import importlib

import pytest
from flask import Flask

MODULE_PATH = "app_odp.models"

# Indice test
# 1. test_user_preferences_returns_empty_for_blank_and_invalid_json:
#    verifica che preferences ritorni {} con preference assente o JSON non valido.
# 2. test_user_preferences_setter_get_pref_and_set_pref_roundtrip:
#    verifica setter preferences, get_pref e set_pref con serializzazione JSON.
# 3. test_user_has_role_checks_assigned_role_names:
#    verifica has_role sui ruoli assegnati all'utente.
# 4. test_roles_iter_self_and_included_and_effective_dimensions_deduplicate:
#    verifica inclusione ricorsiva dei ruoli e deduplica degli effective_*.
# 5. test_roles_add_permission_and_remove_permission_manage_duplicates:
#    verifica add_permission senza duplicati e remove_permission con rimozione idempotente.
# 6. test_user_iter_roles_respects_include_inherited_flag:
#    verifica _iter_roles con e senza ruoli ereditati.
# 7. test_user_has_permission_accepts_multiple_permission_formats:
#    verifica has_permission con codice, id stringa, oggetto Permission, None e valore assente.
# 8. test_user_public_id_is_generated_on_insert:
#    verifica la generazione automatica di public_id all'inserimento.
# 9. test_input_odp_text_normalizes_blank_values:
#    verifica il comportamento di _text con None, spazi e valori numerici.
# 10. test_input_odp_ensure_runtime_row_creates_and_links_row:
#     verifica la creazione e il collegamento di InputOdpRuntime.
# 11. test_input_odp_ensure_stato_row_creates_and_links_row_with_rif_registraz:
#     verifica la creazione e il collegamento di StatoOdp con RifRegistraz.
# 12. test_input_odp_stato_ordine_prefers_runtime_then_erp_then_default:
#     verifica la precedenza StatoOrdine: stato_row, ERP, fallback Pianificata.
# 13. test_input_odp_stato_ordine_setter_writes_stato_row:
#     verifica che il setter StatoOrdine scriva sulla riga stato runtime.
# 14. test_input_odp_fase_attiva_prefers_runtime_then_stato_then_default:
#     verifica la precedenza FaseAttiva: runtime, stato, fallback "1".
# 15. test_input_odp_fase_attiva_setter_updates_runtime_and_stato_rows:
#     verifica che il setter FaseAttiva sincronizzi runtime_row e stato_row.
# 16. test_input_odp_qty_da_lavorare_returns_runtime_or_quantita_and_setter:
#     verifica fallback a Quantita e setter QtyDaLavorare.
# 17. test_input_odp_runtime_text_fields_roundtrip:
#     verifica getter/setter di Note, RisorsaAttiva e LavorazioneAttiva.
# 18. test_erp_outbox_defaults_are_applied_on_log_bind:
#     verifica i default di ErpOutbox sul bind "log".
# Indice test aggiuntivi
# 1. test_input_odp_setters_create_runtime_row_only_once:
#    verifica che più setter runtime non creino righe duplicate in InputOdpRuntime.
# 2. test_input_odp_statoordine_setter_creates_stato_row_only_once:
#    verifica che più assegnazioni a StatoOrdine non creino righe duplicate in StatoOdp.
# 3. test_input_odp_qtydalavorare_persists_after_commit_and_reload:
#    verifica la persistenza reale di QtyDaLavorare dopo commit e reload ORM.
# 4. test_input_odp_note_risorsa_lavorazione_attiva_persist_after_reload:
#    verifica la persistenza reale di Note, RisorsaAttiva e LavorazioneAttiva.
# 5. test_input_odp_faseattiva_reads_from_stato_row_after_reload:
#    verifica il fallback di FaseAttiva dalla sola stato_row dopo reload.
# 6. test_role_included_by_backref_is_populated:
#    verifica che included_roles aggiorni il backref included_by.
# 7. test_user_roles_many_to_many_persists:
#    verifica la persistenza della many-to-many user_roles.
# 8. test_role_permissions_backref_roles_is_available:
#    verifica il backref roles dal lato Permissions.
# 9. test_user_username_is_unique_in_db:
#    verifica il vincolo unique=True su username.
# 10. test_effective_reparti_deduplicates_same_entity_from_multiple_roles:
#     verifica la deduplica per id negli effective_reparti.
# 11. test_erp_outbox_defaults_status_pending_and_attempts_zero_after_commit:
#     verifica i default reali di ErpOutbox dopo commit e reload.
# 12. test_lotti_usati_log_requires_core_fields:
#     verifica i vincoli nullable=False sui campi core di LottiUsatiLog.

# Indice test aggiuntivi
# 1. test_user_public_id_is_generated_as_uuid_like_string:
#    verifica che public_id venga generato automaticamente con formato UUID-like.
# 2. test_has_role_is_case_sensitive_current_behavior:
#    congela il comportamento attuale case-sensitive di has_role().
# 3. test_preferences_setter_with_none_resets_to_empty_dict:
#    verifica che preferences = None serializzi a {}.
# 4. test_preferences_invalid_json_reads_as_empty_dict:
#    verifica fallback a {} quando preference contiene JSON invalido.
# 5. test_set_pref_preserves_existing_keys:
#    verifica che set_pref mantenga le chiavi già presenti.
# 6. test_has_permission_returns_false_for_blank_string:
#    verifica che stringhe vuote o solo spazi non attivino permessi.
# 7. test_iter_self_and_included_handles_none_in_included_roles:
#    verifica robustezza di iter_self_and_included con inclusioni sporche contenenti None.
# 8. test_effective_magazzini_merges_direct_and_inherited:
#    verifica merge tra magazzini diretti ed ereditati.
# 9. test_add_permission_is_idempotent:
#    verifica che add_permission non duplichi il permesso.
# 10. test_remove_permission_is_idempotent_when_missing:
#     verifica che remove_permission non rompa se il permesso manca.
# 11. test_change_event_log_created_with_optional_extracted_keys_empty:
#     verifica che i campi opzionali di ChangeEventLog restino gestibili.
# 12. test_stato_odp_log_timestamp_default_is_populated:
#     verifica che logged_at venga valorizzato automaticamente su StatoOdpLog.
# 13. test_lotti_generati_log_parentlottijson_can_be_null:
#     verifica che ParentLottiJson possa restare nullo dopo persistenza.


# Indice test aggiuntivi
# 1. test_repr_user_contains_model_name:
#    verifica che __repr__ di User non esploda e contenga il nome modello.
# 2. test_repr_roles_contains_model_name:
#    verifica che __repr__ di Roles non esploda e contenga il nome modello.
# 3. test_repr_input_odp_is_not_crashing_with_empty_fields:
#    verifica che __repr__ di InputOdp sia robusto anche con campi quasi vuoti.


@pytest.fixture()
def mod():
    return importlib.import_module(MODULE_PATH)


@pytest.fixture()
def app(mod, tmp_path):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY="test-secret",
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{tmp_path / 'models_main.sqlite'}",
        SQLALCHEMY_BINDS={
            "log": f"sqlite:///{tmp_path / 'models_log.sqlite'}",
        },
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )

    mod.db.init_app(app)
    ctx = app.app_context()
    ctx.push()
    mod.db.create_all()
    mod.db.create_all(bind_key="log")

    try:
        yield app
    finally:
        mod.db.session.remove()
        mod.db.drop_all(bind_key="log")
        mod.db.drop_all()
        ctx.pop()


@pytest.fixture()
def session(mod, app):
    return mod.db.session


def _persist(session, *rows):
    session.add_all(rows)
    session.commit()
    return rows


def test_user_preferences_returns_empty_for_blank_and_invalid_json(mod):
    user_blank = mod.User(username="blank-user")
    user_invalid = mod.User(username="invalid-user", preference="{not-json")

    assert user_blank.preferences == {}
    assert user_invalid.preferences == {}


def test_user_preferences_setter_get_pref_and_set_pref_roundtrip(mod):
    user = mod.User(username="prefs-user")

    user.preferences = {"tema": "dark"}
    user.set_pref("rows", 50)

    assert user.preferences == {"tema": "dark", "rows": 50}
    assert user.preference == '{"tema": "dark", "rows": 50}'
    assert user.get_pref("tema") == "dark"
    assert user.get_pref("missing", "fallback") == "fallback"


def test_user_has_role_checks_assigned_role_names(mod, session):
    admin_role = mod.Roles(name="admin")
    operatore_role = mod.Roles(name="operatore")
    user = mod.User(username="gio", roles=[operatore_role])
    _persist(session, admin_role, operatore_role, user)

    assert user.has_role("operatore") is True
    assert user.has_role("admin") is False


def test_roles_iter_self_and_included_and_effective_dimensions_deduplicate(
    mod, session
):
    rep_a = mod.Reparti(Codice="R-A", Descrizione="Reparto A")
    rep_b = mod.Reparti(Codice="R-B", Descrizione="Reparto B")
    ris_a = mod.Risorse(Codice="RS-A", Descrizione="Risorsa A")
    ris_b = mod.Risorse(Codice="RS-B", Descrizione="Risorsa B")
    lav_a = mod.Lavorazioni(Codice="L-A", Descrizione="Lav A")
    lav_b = mod.Lavorazioni(Codice="L-B", Descrizione="Lav B")
    mag_a = mod.Magazzini(Codice="M-A", Descrizione="Mag A")
    mag_b = mod.Magazzini(Codice="M-B", Descrizione="Mag B")
    fam_a = mod.Famiglia(Codice="F-A", Descrizione="Fam A")
    fam_b = mod.Famiglia(Codice="F-B", Descrizione="Fam B")
    macro_a = mod.Macrofamiglia(Codice="MF-A", Descrizione="Macro A")
    macro_b = mod.Macrofamiglia(Codice="MF-B", Descrizione="Macro B")

    base = mod.Roles(name="base")
    child = mod.Roles(name="child")
    leaf = mod.Roles(name="leaf")

    base.reparti.extend([rep_a])
    base.risorse.extend([ris_a])
    base.lavorazioni.extend([lav_a])
    base.magazzini.extend([mag_a])
    base.famiglia.extend([fam_a])
    base.macrofamiglia.extend([macro_a])

    child.reparti.extend([rep_a, rep_b])
    child.risorse.extend([ris_a, ris_b])
    child.lavorazioni.extend([lav_a, lav_b])
    child.magazzini.extend([mag_a, mag_b])
    child.famiglia.extend([fam_a, fam_b])
    child.macrofamiglia.extend([macro_a, macro_b])

    base.included_roles.append(child)
    child.included_roles.append(leaf)
    leaf.included_roles.append(base)

    _persist(
        session,
        rep_a,
        rep_b,
        ris_a,
        ris_b,
        lav_a,
        lav_b,
        mag_a,
        mag_b,
        fam_a,
        fam_b,
        macro_a,
        macro_b,
        base,
        child,
        leaf,
    )

    iterated_ids = {role.id for role in base.iter_self_and_included()}

    assert iterated_ids == {base.id, child.id, leaf.id}
    assert {x.id for x in base.effective_reparti} == {rep_a.id, rep_b.id}
    assert {x.id for x in base.effective_risorse} == {ris_a.id, ris_b.id}
    assert {x.id for x in base.effective_lavorazioni} == {lav_a.id, lav_b.id}
    assert {x.id for x in base.effective_magazzini} == {mag_a.id, mag_b.id}
    assert {x.id for x in base.effective_famiglia} == {fam_a.id, fam_b.id}
    assert {x.id for x in base.effective_macrofamiglia} == {macro_a.id, macro_b.id}


def test_roles_add_permission_and_remove_permission_manage_duplicates(mod, session):
    role = mod.Roles(name="quality")
    perm = mod.Permissions(Codice="chiudi", Descrizione="Chiudi ordine")
    _persist(session, role, perm)

    role.add_permission(perm)
    session.commit()
    role.add_permission(perm)
    session.commit()

    assert role.permissions.count() == 1
    assert role.permissions.first().Codice == "chiudi"

    role.remove_permission(perm)
    session.commit()
    role.remove_permission(perm)
    session.commit()

    assert role.permissions.count() == 0


def test_user_iter_roles_respects_include_inherited_flag(mod, session):
    parent = mod.Roles(name="parent")
    child = mod.Roles(name="child")
    parent.included_roles.append(child)
    user = mod.User(username="role-user", roles=[parent])
    _persist(session, parent, child, user)

    direct_ids = {role.id for role in user._iter_roles(include_inherited=False)}
    inherited_ids = {role.id for role in user._iter_roles(include_inherited=True)}

    assert direct_ids == {parent.id}
    assert inherited_ids == {parent.id, child.id}


def test_user_has_permission_accepts_multiple_permission_formats(mod, session):
    perm = mod.Permissions(Codice="EXPORT_AVP", Descrizione="Export AVP")
    child = mod.Roles(name="child")
    parent = mod.Roles(name="parent")
    child.permissions.append(perm)
    parent.included_roles.append(child)
    user = mod.User(username="perm-user", roles=[parent])
    _persist(session, perm, child, parent, user)

    assert user.has_permission("EXPORT_AVP") is True
    assert user.has_permission(str(perm.id)) is True
    assert user.has_permission(perm) is True
    assert user.has_permission(None) is False
    assert user.has_permission("MISSING") is False


def test_user_public_id_is_generated_on_insert(mod, session):
    user = mod.User(username="public-id-user")
    _persist(session, user)

    assert isinstance(user.public_id, str)
    assert user.public_id


def test_input_odp_text_normalizes_blank_values(mod):
    assert mod.InputOdp._text(None) == ""
    assert mod.InputOdp._text("  abc  ") == "abc"
    assert mod.InputOdp._text(15) == "15"


def test_input_odp_ensure_runtime_row_creates_and_links_row(mod, session):
    ordine = mod.InputOdp(IdDocumento="DOC1", IdRiga="R1", RifRegistraz="RR1")
    _persist(session, ordine)

    row = ordine._ensure_runtime_row()
    session.commit()

    assert row.IdDocumento == "DOC1"
    assert row.IdRiga == "R1"
    assert ordine.runtime_row is row
    saved = mod.InputOdpRuntime.query.get(("DOC1", "R1"))
    assert saved is not None


def test_input_odp_ensure_stato_row_creates_and_links_row_with_rif_registraz(
    mod, session
):
    ordine = mod.InputOdp(IdDocumento="DOC2", IdRiga="R2", RifRegistraz="RR2")
    _persist(session, ordine)

    row = ordine._ensure_stato_row()
    session.commit()

    assert row.IdDocumento == "DOC2"
    assert row.IdRiga == "R2"
    assert row.RifRegistraz == "RR2"
    assert ordine.stato_row is row
    saved = mod.StatoOdp.query.get(("DOC2", "R2"))
    assert saved is not None


def test_input_odp_stato_ordine_prefers_runtime_then_erp_then_default(mod, session):
    with_runtime = mod.InputOdp(
        IdDocumento="DOC3",
        IdRiga="R3",
        RifRegistraz="RR3",
        StatoOrdineErp="ERP",
    )
    with_runtime.stato_row = mod.StatoOdp(
        IdDocumento="DOC3",
        IdRiga="R3",
        RifRegistraz="RR3",
        Stato_odp="Sospesa",
    )

    with_erp = mod.InputOdp(
        IdDocumento="DOC4",
        IdRiga="R4",
        RifRegistraz="RR4",
        StatoOrdineErp="Attiva",
    )
    fallback = mod.InputOdp(IdDocumento="DOC5", IdRiga="R5", RifRegistraz="RR5")
    _persist(session, with_runtime, with_erp, fallback)

    assert with_runtime.StatoOrdine == "Sospesa"
    assert with_erp.StatoOrdine == "Attiva"
    assert fallback.StatoOrdine == "Pianificata"


def test_input_odp_stato_ordine_setter_writes_stato_row(mod, session):
    ordine = mod.InputOdp(IdDocumento="DOC6", IdRiga="R6", RifRegistraz="RR6")
    _persist(session, ordine)

    ordine.StatoOrdine = "In corso"
    session.commit()

    assert ordine.stato_row is not None
    assert ordine.stato_row.Stato_odp == "In corso"
    assert ordine.StatoOrdine == "In corso"


def test_input_odp_fase_attiva_prefers_runtime_then_stato_then_default(mod, session):
    with_runtime = mod.InputOdp(IdDocumento="DOC7", IdRiga="R7", RifRegistraz="RR7")
    with_runtime.runtime_row = mod.InputOdpRuntime(
        IdDocumento="DOC7", IdRiga="R7", FaseAttiva="8"
    )
    with_runtime.stato_row = mod.StatoOdp(
        IdDocumento="DOC7",
        IdRiga="R7",
        RifRegistraz="RR7",
        Fase="3",
    )

    with_stato = mod.InputOdp(IdDocumento="DOC8", IdRiga="R8", RifRegistraz="RR8")
    with_stato.stato_row = mod.StatoOdp(
        IdDocumento="DOC8",
        IdRiga="R8",
        RifRegistraz="RR8",
        Fase="5",
    )

    fallback = mod.InputOdp(IdDocumento="DOC9", IdRiga="R9", RifRegistraz="RR9")
    _persist(session, with_runtime, with_stato, fallback)

    assert with_runtime.FaseAttiva == "8"
    assert with_stato.FaseAttiva == "5"
    assert fallback.FaseAttiva == "1"


def test_input_odp_fase_attiva_setter_updates_runtime_and_stato_rows(mod, session):
    ordine = mod.InputOdp(IdDocumento="DOC10", IdRiga="R10", RifRegistraz="RR10")
    _persist(session, ordine)

    ordine.FaseAttiva = " 12 "
    session.commit()

    assert ordine.runtime_row is not None
    assert ordine.stato_row is not None
    assert ordine.runtime_row.FaseAttiva == "12"
    assert ordine.stato_row.Fase == "12"
    assert ordine.FaseAttiva == "12"


def test_input_odp_qty_da_lavorare_returns_runtime_or_quantita_and_setter(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC11",
        IdRiga="R11",
        RifRegistraz="RR11",
        Quantita="25",
    )
    _persist(session, ordine)

    assert ordine.QtyDaLavorare == "25"

    ordine.QtyDaLavorare = " 10 "
    session.commit()

    assert ordine.runtime_row is not None
    assert ordine.runtime_row.QtyDaLavorare == "10"
    assert ordine.QtyDaLavorare == "10"


def test_input_odp_runtime_text_fields_roundtrip(mod, session):
    ordine = mod.InputOdp(IdDocumento="DOC12", IdRiga="R12", RifRegistraz="RR12")
    _persist(session, ordine)

    ordine.Note = "  nota prova  "
    ordine.RisorsaAttiva = " RS-01 "
    ordine.LavorazioneAttiva = " LAV-01 "
    session.commit()

    assert ordine.Note == "nota prova"
    assert ordine.RisorsaAttiva == "RS-01"
    assert ordine.LavorazioneAttiva == "LAV-01"


def test_erp_outbox_defaults_are_applied_on_log_bind(mod, session):
    row = mod.ErpOutbox(
        kind="consuntivo_fase",
        IdDocumento="DOC13",
        IdRiga="R13",
        Fase="1",
        payload_json='{"ok": true}',
    )
    _persist(session, row)

    saved = mod.ErpOutbox.query.get(row.outbox_id)

    assert saved is not None
    assert saved.status == "pending"
    assert saved.attempts == 0
    assert isinstance(saved.created_at, str)
    assert saved.created_at


from sqlalchemy.exc import IntegrityError


def test_input_odp_setters_create_runtime_row_only_once(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC_RT_1",
        IdRiga="R_RT_1",
        RifRegistraz="RR_RT_1",
    )
    _persist(session, ordine)

    ordine.Note = "nota uno"
    first_row = ordine.runtime_row
    ordine.QtyDaLavorare = "12"
    ordine.RisorsaAttiva = "RS-01"
    ordine.LavorazioneAttiva = "LAV-01"
    session.commit()
    session.expire_all()

    saved = mod.InputOdp.query.get(("DOC_RT_1", "R_RT_1"))
    rows = mod.InputOdpRuntime.query.filter_by(
        IdDocumento="DOC_RT_1",
        IdRiga="R_RT_1",
    ).all()

    assert first_row is not None
    assert saved.runtime_row is not None
    assert len(rows) == 1
    assert saved.runtime_row.Note == "nota uno"
    assert saved.runtime_row.QtyDaLavorare == "12"
    assert saved.runtime_row.RisorsaAttiva == "RS-01"
    assert saved.runtime_row.LavorazioneAttiva == "LAV-01"


def test_input_odp_statoordine_setter_creates_stato_row_only_once(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC_ST_1",
        IdRiga="R_ST_1",
        RifRegistraz="RR_ST_1",
    )
    _persist(session, ordine)

    ordine.StatoOrdine = "Presa in carico"
    first_row = ordine.stato_row
    ordine.StatoOrdine = "Sospesa"
    session.commit()
    session.expire_all()

    saved = mod.InputOdp.query.get(("DOC_ST_1", "R_ST_1"))
    rows = mod.StatoOdp.query.filter_by(
        IdDocumento="DOC_ST_1",
        IdRiga="R_ST_1",
    ).all()

    assert first_row is not None
    assert saved.stato_row is not None
    assert len(rows) == 1
    assert saved.stato_row.Stato_odp == "Sospesa"
    assert saved.StatoOrdine == "Sospesa"


def test_input_odp_qtydalavorare_persists_after_commit_and_reload(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC_QTY_1",
        IdRiga="R_QTY_1",
        RifRegistraz="RR_QTY_1",
        Quantita="50",
    )
    _persist(session, ordine)

    ordine.QtyDaLavorare = "17"
    session.commit()
    session.expire_all()

    saved = mod.InputOdp.query.get(("DOC_QTY_1", "R_QTY_1"))

    assert saved is not None
    assert saved.runtime_row is not None
    assert saved.runtime_row.QtyDaLavorare == "17"
    assert saved.QtyDaLavorare == "17"


def test_input_odp_note_risorsa_lavorazione_attiva_persist_after_reload(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC_TXT_1",
        IdRiga="R_TXT_1",
        RifRegistraz="RR_TXT_1",
    )
    _persist(session, ordine)

    ordine.Note = " nota persistita "
    ordine.RisorsaAttiva = " RS-77 "
    ordine.LavorazioneAttiva = " LAV-88 "
    session.commit()
    session.expire_all()

    saved = mod.InputOdp.query.get(("DOC_TXT_1", "R_TXT_1"))

    assert saved is not None
    assert saved.Note == "nota persistita"
    assert saved.RisorsaAttiva == "RS-77"
    assert saved.LavorazioneAttiva == "LAV-88"


def test_input_odp_faseattiva_reads_from_stato_row_after_reload(mod, session):
    ordine = mod.InputOdp(
        IdDocumento="DOC_FA_1",
        IdRiga="R_FA_1",
        RifRegistraz="RR_FA_1",
    )
    stato = mod.StatoOdp(
        IdDocumento="DOC_FA_1",
        IdRiga="R_FA_1",
        RifRegistraz="RR_FA_1",
        Fase="9",
    )
    _persist(session, ordine, stato)
    session.expire_all()

    saved = mod.InputOdp.query.get(("DOC_FA_1", "R_FA_1"))

    assert saved is not None
    assert saved.runtime_row is None
    assert saved.stato_row is not None
    assert saved.stato_row.Fase == "9"
    assert saved.FaseAttiva == "9"


def test_role_included_by_backref_is_populated(mod, session):
    parent = mod.Roles(name="parent-role")
    child = mod.Roles(name="child-role")
    parent.included_roles.append(child)
    _persist(session, parent, child)
    session.expire_all()

    saved_parent = mod.Roles.query.filter_by(name="parent-role").first()
    saved_child = mod.Roles.query.filter_by(name="child-role").first()

    assert saved_parent is not None
    assert saved_child is not None
    assert any(role.id == saved_child.id for role in saved_parent.included_roles)
    assert any(role.id == saved_parent.id for role in saved_child.included_by)


def test_user_roles_many_to_many_persists(mod, session):
    role = mod.Roles(name="operatore-linea")
    user = mod.User(username="user-role-m2m", roles=[role])
    _persist(session, role, user)
    session.expire_all()

    saved_user = mod.User.query.filter_by(username="user-role-m2m").first()
    saved_role = mod.Roles.query.filter_by(name="operatore-linea").first()

    assert saved_user is not None
    assert saved_role is not None
    assert [r.name for r in saved_user.roles] == ["operatore-linea"]
    assert saved_role.users.filter_by(id=saved_user.id).first() is not None


def test_role_permissions_backref_roles_is_available(mod, session):
    role = mod.Roles(name="responsabile-export")
    perm = mod.Permissions(Codice="EXPORT_TXT", Descrizione="Export txt")
    role.permissions.append(perm)
    _persist(session, role, perm)
    session.expire_all()

    saved_role = mod.Roles.query.filter_by(name="responsabile-export").first()
    saved_perm = mod.Permissions.query.filter_by(Codice="EXPORT_TXT").first()

    assert saved_role is not None
    assert saved_perm is not None
    assert saved_role.permissions.filter_by(id=saved_perm.id).first() is not None
    assert saved_perm.roles.filter_by(id=saved_role.id).first() is not None


def test_user_username_is_unique_in_db(mod, session):
    first = mod.User(username="unique-user")
    _persist(session, first)

    session.add(mod.User(username="unique-user"))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_effective_reparti_deduplicates_same_entity_from_multiple_roles(mod, session):
    reparto = mod.Reparti(Codice="REP-1", Descrizione="Reparto unico")
    base = mod.Roles(name="base-dedupe")
    child = mod.Roles(name="child-dedupe")

    base.reparti.append(reparto)
    child.reparti.append(reparto)
    base.included_roles.append(child)

    _persist(session, reparto, base, child)
    session.expire_all()

    saved_base = mod.Roles.query.filter_by(name="base-dedupe").first()

    assert saved_base is not None
    assert len(saved_base.effective_reparti) == 1
    assert saved_base.effective_reparti[0].Codice == "REP-1"


def test_erp_outbox_defaults_status_pending_and_attempts_zero_after_commit(
    mod, session
):
    row = mod.ErpOutbox(
        kind="consuntivo_fase",
        IdDocumento="DOC_OB_1",
        IdRiga="R_OB_1",
        RifRegistraz="RR_OB_1",
        Fase="2",
        payload_json='{"ok": true}',
    )
    _persist(session, row)
    session.expire_all()

    saved = mod.ErpOutbox.query.get(row.outbox_id)

    assert saved is not None
    assert saved.status == "pending"
    assert saved.attempts == 0
    assert isinstance(saved.created_at, str)
    assert saved.created_at


def test_lotti_usati_log_requires_core_fields(mod, session):
    required_fields = [
        "IdDocumento",
        "IdRiga",
        "CodArt",
        "RifLottoAlfa",
        "Quantita",
    ]

    base_payload = {
        "IdDocumento": "DOC_LU_1",
        "IdRiga": "R_LU_1",
        "RifRegistraz": "RR_LU_1",
        "CodArt": "ART-01",
        "RifLottoAlfa": "LOTTO-01",
        "Quantita": "5",
        "Esito": "ok",
    }

    for field in required_fields:
        payload = dict(base_payload)
        payload[field] = None

        session.add(mod.LottiUsatiLog(**payload))
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()


def test_user_public_id_is_generated_as_uuid_like_string(mod, session):
    user = mod.User(username="uuid-like-user")
    _persist(session, user)

    assert isinstance(user.public_id, str)
    assert len(user.public_id) == 36
    assert user.public_id[8] == "-"
    assert user.public_id[13] == "-"
    assert user.public_id[18] == "-"
    assert user.public_id[23] == "-"


def test_has_role_is_case_sensitive_current_behavior(mod, session):
    role = mod.Roles(name="Operatore")
    user = mod.User(username="case-role-user", roles=[role])
    _persist(session, role, user)

    assert user.has_role("Operatore") is True
    assert user.has_role("operatore") is False
    assert user.has_role("OPERATORE") is False


def test_preferences_setter_with_none_resets_to_empty_dict(mod):
    user = mod.User(username="prefs-none-user")
    user.preferences = {"tema": "dark", "rows": 25}

    user.preferences = None

    assert user.preference == "{}"
    assert user.preferences == {}


def test_preferences_invalid_json_reads_as_empty_dict(mod):
    user = mod.User(username="prefs-invalid-user", preference='{"tema": invalid}')

    assert user.preferences == {}
    assert user.get_pref("tema", "fallback") == "fallback"


def test_set_pref_preserves_existing_keys(mod):
    user = mod.User(username="prefs-merge-user")
    user.preferences = {"tema": "dark", "rows": 25}

    user.set_pref("lang", "it")

    assert user.preferences == {"tema": "dark", "rows": 25, "lang": "it"}
    assert user.get_pref("tema") == "dark"
    assert user.get_pref("rows") == 25
    assert user.get_pref("lang") == "it"


def test_has_permission_returns_false_for_blank_string(mod, session):
    perm = mod.Permissions(Codice="EXPORT_AVP", Descrizione="Export AVP")
    role = mod.Roles(name="perm-blank-role")
    role.permissions.append(perm)
    user = mod.User(username="perm-blank-user", roles=[role])
    _persist(session, perm, role, user)

    assert user.has_permission("") is False
    assert user.has_permission("   ") is False


def test_iter_self_and_included_handles_none_in_included_roles(mod, session):
    root = mod.Roles(name="root-none-included")
    child = mod.Roles(name="child-none-included")
    _persist(session, root, child)

    # Iniezione "sporca" per congelare la robustezza del metodo senza passare
    # dall'instrumentation ORM, che normalmente non accetta None nella relazione.
    root.__dict__["included_roles"] = [child, None]
    child.__dict__["included_roles"] = []

    got_ids = [role.id for role in root.iter_self_and_included()]

    assert got_ids == [root.id, child.id]


def test_effective_magazzini_merges_direct_and_inherited(mod, session):
    mag_direct = mod.Magazzini(Codice="MAG-DIR", Descrizione="Magazzino diretto")
    mag_child = mod.Magazzini(Codice="MAG-CHD", Descrizione="Magazzino ereditato")
    base = mod.Roles(name="base-mag-role")
    child = mod.Roles(name="child-mag-role")

    base.magazzini.append(mag_direct)
    child.magazzini.append(mag_child)
    base.included_roles.append(child)

    _persist(session, mag_direct, mag_child, base, child)
    session.expire_all()

    saved = mod.Roles.query.filter_by(name="base-mag-role").first()

    assert saved is not None
    assert {x.Codice for x in saved.effective_magazzini} == {"MAG-DIR", "MAG-CHD"}


def test_add_permission_is_idempotent(mod, session):
    role = mod.Roles(name="idempotent-add-role")
    perm = mod.Permissions(Codice="CHIUDI_ODP", Descrizione="Chiusura ODP")
    _persist(session, role, perm)

    role.add_permission(perm)
    session.commit()
    role.add_permission(perm)
    session.commit()

    assert role.permissions.count() == 1
    assert role.permissions.first().Codice == "CHIUDI_ODP"


def test_remove_permission_is_idempotent_when_missing(mod, session):
    role = mod.Roles(name="idempotent-remove-role")
    perm = mod.Permissions(Codice="EXPORT_TXT", Descrizione="Export TXT")
    _persist(session, role, perm)

    role.remove_permission(perm)
    session.commit()

    assert role.permissions.count() == 0

    role.add_permission(perm)
    session.commit()
    assert role.permissions.count() == 1

    role.remove_permission(perm)
    session.commit()
    role.remove_permission(perm)
    session.commit()

    assert role.permissions.count() == 0


def test_change_event_log_created_with_optional_extracted_keys_empty(mod, session):
    row = mod.ChangeEventLog(
        topic="odp.changed",
    )
    _persist(session, row)
    session.expire_all()

    saved = mod.ChangeEventLog.query.get(row.log_id)

    assert saved is not None
    assert saved.topic == "odp.changed"
    assert saved.scope is None
    assert saved.payload_json is None
    assert saved.created_at is None
    assert saved.IdDocumento is None
    assert saved.IdRiga is None
    assert isinstance(saved.logged_at, str)
    assert saved.logged_at


def test_stato_odp_log_timestamp_default_is_populated(mod, session):
    row = mod.StatoOdpLog(
        IdDocumento="DOC_STLOG_1",
        IdRiga="R_STLOG_1",
        RifRegistraz="RR_STLOG_1",
        Stato_odp="Presa in carico",
    )
    _persist(session, row)
    session.expire_all()

    saved = mod.StatoOdpLog.query.get(row.log_id)

    assert saved is not None
    assert isinstance(saved.logged_at, str)
    assert saved.logged_at
    assert saved.IdDocumento == "DOC_STLOG_1"
    assert saved.IdRiga == "R_STLOG_1"


def test_lotti_generati_log_parentlottijson_can_be_null(mod, session):
    row = mod.LottiGeneratiLog(
        IdDocumento="DOC_LG_1",
        IdRiga="R_LG_1",
        RifRegistraz="RR_LG_1",
        CodArt="ART-LG-1",
        RifLottoAlfa="LOTTO-LG-1",
        Quantita="8",
        Fase="2",
    )
    _persist(session, row)
    session.expire_all()

    saved = mod.LottiGeneratiLog.query.get(row.log_id)

    assert saved is not None
    assert saved.ParentLottiJson is None
    assert saved.CodArt == "ART-LG-1"
    assert saved.RifLottoAlfa == "LOTTO-LG-1"
    assert saved.Quantita == "8"


def test_repr_user_contains_model_name(mod):
    user = mod.User(username="repr-user")

    got = repr(user)

    assert isinstance(got, str)
    assert got.startswith("<Users ")
    assert "repr-user" in got


def test_repr_roles_contains_model_name(mod):
    role = mod.Roles(name="repr-role")

    got = repr(role)

    assert isinstance(got, str)
    assert got.startswith("<Roles ")
    assert "repr-role" in got


def test_repr_input_odp_is_not_crashing_with_empty_fields(mod):
    ordine = mod.InputOdp(
        IdDocumento="DOC_REPR_1",
        IdRiga="R_REPR_1",
    )

    got = repr(ordine)

    assert isinstance(got, str)
    assert got.startswith("<")
    assert got.endswith(">")
    assert "DOC_REPR_1" in got
    assert "R_REPR_1" in got
