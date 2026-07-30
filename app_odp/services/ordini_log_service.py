from decimal import Decimal
import json

from app_odp.models import (
    db,
    InputOdpLog,
    LottiGeneratiLog,
    LottiUsatiLog,
    OdpRuntimeLog,
)
from app_odp.services.erp_export_service import _build_operation_group_id
from app_odp.services.order_helpers import (
    _bool_text,
    _extract_codes_from_cell,
    _first_not_blank,
    _norm_text,
)


def _add_input_odp_closure_log(
    *,
    operation_group_id: str,
    ordine,
    fase_consuntivata: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    minuti_non_funzionamento: int,
    secondi_non_funzionamento: int,
    chiusura_parziale: bool,
    note_chiusura: str,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    closed_by: str,
    closed_at: str,
):
    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            Note=ordine.Note,
            FaseConsuntivata=_norm_text(fase_consuntivata),
            QuantitaConforme=str(q_ok),
            QuantitaNonConforme=str(q_nok),
            TempoFunzionamentoFinale=_norm_text(tempo_finale),
            TempoNonFunzionamentoMinuti=_norm_text(minuti_non_funzionamento),
            TempoNonFunzionamentoSecondi=_norm_text(secondi_non_funzionamento),
            ChiusuraParziale=_bool_text(chiusura_parziale),
            NoteChiusura=_norm_text(note_chiusura),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(closed_by),
            ClosedAt=_norm_text(closed_at),
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        )
    )


def _add_input_odp_takeover_log(
    *,
    operation_group_id: str,
    ordine,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    taken_by: str,
    taken_at: str,
    note_evento: str = "Presa in carico ordine",
):
    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            Note=ordine.Note,
            FaseConsuntivata=None,
            QuantitaConforme=None,
            QuantitaNonConforme=None,
            TempoFunzionamentoFinale=None,
            TempoNonFunzionamentoMinuti=None,
            TempoNonFunzionamentoSecondi=None,
            ChiusuraParziale=None,
            NoteChiusura=_norm_text(note_evento),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(taken_by),
            ClosedAt=_norm_text(taken_at),
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        )
    )


def _add_input_odp_suspend_log(
    *,
    operation_group_id: str,
    ordine,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    suspended_by: str,
    suspended_at: str,
    causale: str = "",
    minuti_non_funzionamento: int | str | None = None,
    secondi_non_funzionamento: int | str | None = None,
    note_evento: str = "Sospensione ordine",
):
    note_parts = [note_evento]
    if causale:
        note_parts.append(f"Causale: {causale}")
    if minuti_non_funzionamento not in (None, ""):
        note_parts.append(
            f"Tempo non funzionamento minuti: {_norm_text(minuti_non_funzionamento)}"
        )

    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
            Note=ordine.Note,
            FaseConsuntivata=None,
            QuantitaConforme=None,
            QuantitaNonConforme=None,
            TempoFunzionamentoFinale=None,
            TempoNonFunzionamentoMinuti=_norm_text(minuti_non_funzionamento),
            TempoNonFunzionamentoSecondi=_norm_text(secondi_non_funzionamento),
            ChiusuraParziale=None,
            NoteChiusura=" | ".join(note_parts),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(suspended_by),
            ClosedAt=_norm_text(suspended_at),
        )
    )


def _add_lotti_usati_logs(
    *,
    operation_group_id: str,
    ordine,
    lotti_input: list[dict],
    fase: str,
    closed_by: str,
    closed_at: str,
):
    for lotto_row in lotti_input or []:
        db.session.add(
            LottiUsatiLog(
                OperationGroupId=operation_group_id,
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                CodArt=_norm_text(lotto_row.get("CodArt")),
                RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
                Quantita=str(lotto_row.get("Quantita", 0)),
                Esito=_norm_text(lotto_row.get("Esito", "ok")),
                ClosedBy=_norm_text(closed_by),
                ClosedAt=_norm_text(closed_at),
                Fase=_norm_text(fase),
            )
        )


def _add_lotto_generato_log(
    *,
    operation_group_id: str,
    ordine,
    lotto_prodotto: dict | None,
    closed_by: str,
    closed_at: str,
    label_filename: str = "",
):
    if lotto_prodotto is None:
        return
    db.session.add(
        LottiGeneratiLog(
            OperationGroupId=operation_group_id,
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=lotto_prodotto["CodArt"],
            RifLottoAlfa=lotto_prodotto["RifLottoAlfa"],
            Quantita=lotto_prodotto["Quantita"],
            Fase=lotto_prodotto["Fase"],
            ClosedBy=_norm_text(closed_by),
            ClosedAt=_norm_text(closed_at),
            LabelFilename=_norm_text(label_filename),
        ),
    )


def _append_operazione_log(
    *,
    topic: str,
    ordine,
    action: str,
    event_at: str,
    username: str,
    runtime_pre: dict | None,
    runtime_post: dict | None,
    stato_ordine_pre: str = "",
    stato_ordine_post: str = "",
    qty_pre: str = "",
    qty_post: str = "",
    q_ok: str = "",
    q_nok: str = "",
    elapsed_seconds: int | str | None = None,
    tempo_non_funzionamento_minuti: int | str | None = None,
    tempo_non_funzionamento_secondi: int | str | None = None,
    causale: str = "",
    note: str = "",
    motivo: str = "",
    fase: str = "",
    extra_payload: dict | None = None,
):
    runtime_pre = runtime_pre or {}
    runtime_post = runtime_post or {}

    reparto_codes = _extract_codes_from_cell(ordine.CodReparto)
    scope = reparto_codes[0] if reparto_codes else _norm_text(ordine.CodReparto)

    payload = {
        "azione": action,
        "utente": username,
        "fase": _first_not_blank(
            fase,
            _norm_text(runtime_post.get("fase")),
            _norm_text(runtime_pre.get("fase")),
            default="",
        ),
        "tempo_funzionamento": _norm_text(runtime_post.get("tempo_funzionamento")),
    }
    if q_ok not in (None, ""):
        payload["quantita_conforme"] = _norm_text(q_ok)
    if q_nok not in (None, ""):
        payload["quantita_non_conforme"] = _norm_text(q_nok)
    if elapsed_seconds not in (None, ""):
        payload["elapsed_seconds"] = elapsed_seconds
    if tempo_non_funzionamento_minuti not in (None, ""):
        payload["tempo_non_funzionamento_minuti"] = tempo_non_funzionamento_minuti
    if tempo_non_funzionamento_secondi not in (None, ""):
        payload["tempo_non_funzionamento_secondi"] = tempo_non_funzionamento_secondi
    if causale:
        payload["causale"] = causale
    if note:
        payload["note"] = note
    if extra_payload:
        payload.update(extra_payload)

    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action=action,
        when_iso=event_at,
    )

    row = OdpRuntimeLog(
        OperationGroupId=operation_group_id,
        EventSequence=1,
        Topic=topic,
        Scope=scope,
        CodArt=_norm_text(ordine.CodArt),
        CodReparto=_norm_text(ordine.CodReparto),
        PayloadJson=json.dumps(payload, ensure_ascii=False),
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Azione=action,
        Motivo=_norm_text(motivo),
        UtenteOperazione=username,
        EventAt=event_at,
        StatoOdpPre=_norm_text(runtime_pre.get("stato_odp")),
        StatoOdpPost=_norm_text(runtime_post.get("stato_odp")),
        StatoOrdinePre=_norm_text(stato_ordine_pre),
        StatoOrdinePost=_norm_text(stato_ordine_post),
        FasePre=_norm_text(runtime_pre.get("fase")),
        FasePost=_norm_text(runtime_post.get("fase")),
        DataInCaricoPre=_norm_text(runtime_pre.get("data_in_carico")),
        DataInCaricoPost=_norm_text(runtime_post.get("data_in_carico")),
        DataUltimaAttivazionePre=_norm_text(runtime_pre.get("data_ultima_attivazione")),
        DataUltimaAttivazionePost=_norm_text(
            runtime_post.get("data_ultima_attivazione")
        ),
        VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        TempoFunzionamentoPre=_norm_text(runtime_pre.get("tempo_funzionamento")),
        TempoFunzionamentoPost=_norm_text(runtime_post.get("tempo_funzionamento")),
        ElapsedSeconds=_norm_text(elapsed_seconds),
        TempoNonFunzionamentoMinuti=_norm_text(tempo_non_funzionamento_minuti),
        TempoNonFunzionamentoSecondi=_norm_text(tempo_non_funzionamento_secondi),
        QtyDaLavorarePre=_norm_text(qty_pre),
        QtyDaLavorarePost=_norm_text(qty_post),
        QuantitaConforme=_norm_text(q_ok),
        QuantitaNonConforme=_norm_text(q_nok),
        Causale=_norm_text(causale),
        Note=_norm_text(note),
        RifOrdinePrinc=_first_not_blank(
            runtime_post.get("rif_ordine_princ"),
            runtime_pre.get("rif_ordine_princ"),
            default="",
        ),
    )
    db.session.add(row)
    db.session.flush()
    return row
