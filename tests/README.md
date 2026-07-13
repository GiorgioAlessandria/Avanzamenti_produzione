# Struttura test

Questa cartella contiene la suite `pytest` del progetto.

## Comandi principali

Da `V:\Avanzamenti_produzione`:

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m pytest tests\test_app_odp
.\.venv\Scripts\python.exe -m pytest tests\test_sync
.\.venv\Scripts\python.exe -m pytest tests\test_app_odp\test_routes.py
```

Per una raccolta rapida senza eseguire i test:

```powershell
.\.venv\Scripts\python.exe -m pytest --collect-only -q -p no:cacheprovider
```

## Aree coperte

| Area | Percorso test | Cosa valida |
| --- | --- | --- |
| App Flask | `tests/test_app_odp/test_app.py` | factory, configurazione, logging, context processor |
| Autenticazione | `tests/test_app_odp/test_auth.py` | login, logout, loader utente |
| Modelli e DB | `tests/test_app_odp/test_models.py` | SQLAlchemy, vincoli, relazioni, helper dei modelli |
| Policy/RBAC | `tests/test_app_odp/test_policy.py` | permessi, filtri query, decorator |
| Filtri Jinja | `tests/test_app_odp/test_filters.py` | parsing JSON, liste, date |
| Route e API ODP | `tests/test_app_odp/test_routes.py` | flussi ordini, lotti, export AVP, home bridge |
| Etichette | `tests/test_app_odp/test_etichette.py` | layout, QR, font, wrapping testo |
| Output ODP | `tests/test_app_odp/test_odp_output.py` | generazione righe TXT ODP |
| Acquisti | `tests/test_app_odp/test_acquisti_service.py` | controlli giacenze e workbook Excel |
| Dashboard | `tests/test_app_odp/test_dashboard_*.py` | helper KPI, date e normalizzazioni |
| Report settimanale | `tests/test_app_odp/test_report_settimanale_*.py` | percentuali, fasi e helper scalari |
| Priorita operatori | `tests/test_app_odp/test_priorita_service.py` | chiave ordine/fase; regole utente da confermare |
| Sync input | `tests/test_sync/test_sync_input.py` | import ERP, trasformazioni pandas, runtime, schedulazione |
| Sync acquisti e inventario | `tests/test_sync/test_sync_acq*.py`, `tests/test_sync/test_estrazione_inventario.py` | trasformazioni e aggregazioni pure |

## Copertura da estendere

Priorita consigliata:

1. `app_odp/services/ordini_gruppi_service.py`: creazione, sospensione, riattivazione e chiusura gruppi.
2. `app_odp/services/priorita_service.py`: compattazione code, visibilita operatori, ripristino priorita.
3. `app_odp/services/dashboard_service.py`: payload, filtri e comportamento UI.
4. `app_odp/services/report_settimanale_service.py`: ore lavorate e fallback runtime/log.
5. `app_odp/routes_modules/*.py`: smoke test per blueprint e contratti JSON principali.
6. Funzioni sync che sostituiscono tabelle: test con DB temporaneo dopo conferma del comportamento.

## Regole pratiche

- Un file test per modulo o area funzionale, con nome `test_<modulo>.py`.
- Preferire unit test piccoli sui service; usare Flask client solo per route e contratti HTTP.
- Usare SQLite temporaneo e `tmp_path` per i DB; non scrivere nei database in `instance`.
- Ogni bug corretto deve lasciare almeno un test che fallirebbe senza la correzione.
- Se un file serve come script manuale, metterlo in `tools/` e non chiamarlo `test_*.py`.

