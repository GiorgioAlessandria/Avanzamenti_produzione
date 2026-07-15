from datetime import date, datetime

from flask import Flask

from app_odp.manutenzioni_models import (
    EventoManutenzione,
    Macchinario,
    ManutenzioneRicorrente,
)
from app_odp.models import db
from app_odp.services import manutenzioni_piani_service as service


def test_elimina_programmati_e_conserva_lo_storico(monkeypatch):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"manutenzioni": "sqlite://"},
    )
    db.init_app(app)

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
            data_inizio=date(2026, 1, 1),
        )
        programmato = EventoManutenzione(
            manutenzione_ricorrente=piano,
            data_teorica=date(2026, 8, 1),
            data_programmata=date(2026, 8, 1),
            stato="PROGRAMMATA",
            titolo_snapshot="Controllo",
        )
        completato = EventoManutenzione(
            manutenzione_ricorrente=piano,
            data_teorica=date(2026, 7, 1),
            data_programmata=date(2026, 7, 1),
            stato="COMPLETATA",
            data_esecuzione=datetime(2026, 7, 1, 8, 0),
            titolo_snapshot="Controllo",
        )
        db.session.add_all([macchinario, piano, programmato, completato])
        db.session.commit()
        id_programmato = programmato.id
        id_completato = completato.id

        monkeypatch.setattr(
            service,
            "get_piano_manutenzione",
            lambda *args, **kwargs: piano,
        )

        result = service.delete_piano_manutenzione(
            piano.id,
            object(),
        )

        assert result == {
            "eventi_eliminati": 1,
            "serie_archiviata": True,
        }
        assert db.session.get(EventoManutenzione, id_programmato) is None
        assert db.session.get(EventoManutenzione, id_completato) is not None
        assert db.session.get(ManutenzioneRicorrente, piano.id).archiviata is True
