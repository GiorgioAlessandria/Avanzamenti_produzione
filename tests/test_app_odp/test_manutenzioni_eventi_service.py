from datetime import date, datetime
from types import SimpleNamespace

from flask import Flask

from app_odp.manutenzioni_models import (
    EventoManutenzione,
    Macchinario,
    MacchinarioOperatore,
    ManutenzioneRicorrente,
    ManutenzioneStraordinaria,
)
from app_odp.models import db
from app_odp.services import manutenzioni_eventi_service as service


def test_ricorrenza_giornaliera_mantiene_le_date_teoriche():
    assert service._calculate_daily_dates(
        date(2026, 8, 14),
        1,
        date(2026, 8, 14),
        date(2026, 8, 17),
    ) == [
        date(2026, 8, 14),
        date(2026, 8, 15),
        date(2026, 8, 16),
        date(2026, 8, 17),
    ]


def test_sync_rigenera_solo_eventi_futuri_programmati(monkeypatch):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"manutenzioni": "sqlite://"},
    )
    db.init_app(app)

    oggi = date(2026, 7, 13)
    data_desiderata = date(2026, 7, 20)
    data_passata_teorica = date(2026, 7, 14)

    with app.app_context():
        db.create_all(bind_key="manutenzioni")

        macchinario = Macchinario(
            codice="M1",
            descrizione="Macchina 1",
            reparto_codice="R1",
        )
        piano = ManutenzioneRicorrente(
            macchinario=macchinario,
            titolo="Controllo",
            frequenza_unita="giorni",
            frequenza_intervallo=1,
            data_inizio=oggi,
        )
        db.session.add_all([macchinario, piano])
        db.session.flush()

        def evento(data_teorica, data_programmata, stato="PROGRAMMATA"):
            row = EventoManutenzione(
                manutenzione_ricorrente=piano,
                data_teorica=data_teorica,
                data_programmata=data_programmata,
                stato=stato,
                titolo_snapshot="Controllo",
                data_esecuzione=(
                    datetime(2026, 7, 10, 8, 0)
                    if stato == "COMPLETATA"
                    else None
                ),
            )
            db.session.add(row)
            return row

        desiderato = evento(data_desiderata, data_desiderata)
        obsoleto = evento(date(2026, 7, 21), date(2026, 7, 21))
        passato = evento(data_passata_teorica, date(2026, 7, 12))
        chiuso = evento(
            date(2026, 7, 22),
            date(2026, 7, 22),
            "COMPLETATA",
        )
        db.session.commit()

        ids_da_conservare = {desiderato.id, passato.id, chiuso.id}
        ids_storico = {passato.id, chiuso.id}
        id_desiderato = desiderato.id
        id_obsoleto = obsoleto.id

        monkeypatch.setattr(
            service,
            "calculate_piano_dates",
            lambda *args, **kwargs: [data_passata_teorica, data_desiderata],
        )
        monkeypatch.setattr(
            service,
            "normalizza_data_manutenzione",
            lambda giorno, **kwargs: (giorno, None),
        )

        result = service.sync_eventi_piano(
            piano,
            data_dal=oggi,
            data_fino=date(2026, 8, 1),
        )

        ids_rimasti = {row.id for row in EventoManutenzione.query.all()}
        assert result["deleted"] == 1
        assert id_obsoleto not in ids_rimasti
        assert ids_da_conservare <= ids_rimasti
        assert passato.data_programmata == date(2026, 7, 12)

        piano.attiva = False
        result = service.sync_eventi_piano(piano, data_dal=oggi)
        ids_rimasti = {row.id for row in EventoManutenzione.query.all()}
        assert result["deleted"] == 1
        assert id_desiderato not in ids_rimasti
        assert ids_storico <= ids_rimasti


def test_intervento_richiesto_diventa_completato_dopo_lo_straordinario():
    evento = EventoManutenzione(
        data_teorica=date(2026, 7, 14),
        data_programmata=date(2026, 7, 14),
        stato="COMPLETATA",
        esito="INTERVENTO_RICHIESTO",
        titolo_snapshot="Controllo",
    )
    straordinaria = ManutenzioneStraordinaria(
        evento_manutenzione=evento,
        data_intervento=datetime(2026, 7, 14, 10, 0),
        titolo="Intervento richiesto - Controllo",
        descrizione_problema="Guasto",
        intervento_eseguito="DA COMPLETARE",
        esito="DA_VERIFICARE",
    )

    assert service.get_stato_visuale_evento(evento) == "INTERVENTO_RICHIESTO"

    straordinaria.intervento_eseguito = "Sostituito il componente"

    assert service.get_stato_visuale_evento(evento) == "COMPLETATA"


def test_elimina_evento_programmato_solo_con_permesso_admin(monkeypatch):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"manutenzioni": "sqlite://"},
    )
    db.init_app(app)

    class User:
        def __init__(self, admin):
            self.admin = admin

        def has_role(self, role_name):
            return self.admin and role_name == "admin"

    class Policy:
        def __init__(self, admin):
            self.user = User(admin)

        def can(self, permission):
            return False

    with app.app_context():
        db.create_all(bind_key="manutenzioni")
        macchinario = Macchinario(
            codice="M1",
            descrizione="Macchina 1",
            reparto_codice="R1",
        )
        piano = ManutenzioneRicorrente(
            macchinario=macchinario,
            titolo="Controllo",
            frequenza_unita="mesi",
            frequenza_intervallo=1,
            data_inizio=date(2026, 6, 16),
        )
        evento = EventoManutenzione(
            manutenzione_ricorrente=piano,
            data_teorica=date(2026, 6, 16),
            data_programmata=date(2026, 6, 16),
            stato="PROGRAMMATA",
            titolo_snapshot="Controllo",
        )
        db.session.add_all([macchinario, piano, evento])
        db.session.commit()
        evento_id = evento.id

        monkeypatch.setattr(
            service,
            "get_evento_manutenzione",
            lambda *args, **kwargs: evento,
        )

        try:
            service.delete_evento_manutenzione(evento_id, Policy(False))
            assert False, "Un non amministratore non può eliminare eventi"
        except service.PermessoManutenzioniError:
            pass

        assert service.delete_evento_manutenzione(
            evento_id,
            Policy(True),
        ) == evento_id
        assert db.session.get(EventoManutenzione, evento_id) is None


def test_calendario_operatore_mostra_solo_macchinari_assegnati():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"manutenzioni": "sqlite://"},
    )
    db.init_app(app)

    with app.app_context():
        db.create_all(bind_key="manutenzioni")
        assegnato = Macchinario(
            codice="M1",
            descrizione="Assegnato",
            reparto_codice="R1",
        )
        non_assegnato = Macchinario(
            codice="M2",
            descrizione="Non assegnato",
            reparto_codice="R1",
        )
        assegnato_ad_altri = Macchinario(
            codice="M3",
            descrizione="Assegnato ad altri",
            reparto_codice="R1",
        )
        assegnato.operatori_assegnati.append(
            MacchinarioOperatore(
                operatore_public_id="op-1",
                operatore_username="operatore",
            )
        )
        assegnato_ad_altri.operatori_assegnati.append(
            MacchinarioOperatore(
                operatore_public_id="op-2",
                operatore_username="altro",
            )
        )
        db.session.add_all([assegnato, non_assegnato, assegnato_ad_altri])
        db.session.commit()

        rows = [
            {"macchinario_id": macchina.id}
            for macchina in (assegnato, non_assegnato, assegnato_ad_altri)
        ]
        assert service.filter_eventi_per_operatore(
            rows,
            SimpleNamespace(public_id="op-1"),
            include_unassigned=False,
        ) == [{"macchinario_id": assegnato.id}]
