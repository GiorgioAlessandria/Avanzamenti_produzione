# Avanzamenti Produzione (ODP)

> ⚠️ **Progetto in sviluppo attivo** — Alcune funzionalità potrebbero essere incomplete o soggette a modifiche.

Sistema interno per la gestione degli **Ordini Di Produzione (ODP)** e degli avanzamenti di fabbricazione, sviluppato per ottimizzare il flusso di lavoro tra i reparti produttivi.

---

## Descrizione

`Avanzamenti_produzione` è un'applicazione web sviluppata con **Flask** che permette ai reparti di produzione di gestire in tempo reale lo stato degli ordini, tracciare le fasi attive, registrare i lotti e le matricole utilizzate e monitorare l'avanzamento complessivo della produzione.

Il sistema supporta diversi tipi di ordine — **semilavorati (SL)** e **macchine** — con flussi di avanzamento dedicati per ciascun reparto.

---

## Funzionalità principali

### Gestione ODP
- Visualizzazione degli ordini attivi per reparto
- Ciclo di vita completo dell'ordine: presa in carico → sospensione → riattivazione → chiusura
- Supporto a ordini di tipo **semilavorato** e **macchina**
- Tracciamento della **fase attiva** (`FaseAttiva` / `NumFase`) per ogni ordine

### Reparti supportati
| Reparto | Descrizione |
|---|---|
| Carpenteria | Lavorazioni strutturali |
| Officina | Lavorazioni meccaniche |
| Montaggio | Assemblaggio componenti |
| Collaudo | Verifica e test finali |

### Gestione materiali e lotti
- **Distinta Base (BOM)** associata a ogni ordine, gestita in formato JSON
- Registrazione dei **lotti componenti** utilizzati (`GestioneLotto`) con tracciamento OK/KO
- Supporto alle **matricole** (`GestioneMatricola`) per la tracciabilità dei prodotti finiti
- Log storico dei lotti usati (`LottiUsatiLog`)

### Interfaccia utente
- UI responsive basata su **Bootstrap 5**
- Notifiche in tempo reale tramite **toast** dedicati per ogni azione e reparto
- Aggiornamenti parziali della pagina tramite `applyHomeFragments` (polling su `change_event`)
- Tabelle dinamiche per la gestione dei lotti con stati OK/KO

---

## Stack tecnologico

| Layer | Tecnologia |
|---|---|
| Backend | Python · Flask |
| Database | SQLite |
| Frontend | Jinja2 · Bootstrap 5 · JavaScript |
| Deploy | Uso interno (LAN aziendale) |

---

## Struttura del progetto

```
Avanzamenti_produzione/
│
├── app_odp/
│   ├── __init__.py              # Factory dell'app Flask
│   ├── models.py                # Modelli SQLAlchemy (ODP, StatoOdp, Lotti, Matricole…)
│   ├── routes/
│   │   ├── carpenteria.py       # Blueprint reparto Carpenteria
│   │   ├── officina.py          # Blueprint reparto Officina
│   │   ├── montaggio.py         # Blueprint reparto Montaggio
│   │   └── collaudo.py          # Blueprint reparto Collaudo
│   ├── api/
│   │   └── api_lotti.py         # Endpoint REST per la gestione lotti componenti
│   ├── templates/
│   │   ├── base.html
│   │   ├── home.html
│   │   └── reparti/             # Template Jinja2 per ogni reparto
│   └── static/
│       ├── css/
│       └── js/
│
├── instance/
│   └── odp.db                   # Database SQLite (non versionato)
│
├── config.py                    # Configurazione dell'applicazione
├── run.py                       # Entry point
└── README.md
```

---

## Modelli principali

| Modello | Descrizione |
|---|---|
| `ODP` | Ordine di produzione con tipo, stato e fasi |
| `StatoOdp` | Storico degli stati di un ordine |
| `FaseAttiva` | Fase corrente in lavorazione per reparto |
| `DistintaMateriale` | BOM dell'ordine in formato JSON |
| `GestioneLotto` | Lotti componenti associati a un ODP |
| `LottiUsatiLog` | Log storico dei lotti utilizzati |
| `GestioneMatricola` | Matricole dei prodotti finiti |

---

## Stato sviluppo

| Reparto | Presa in carico | Sospensione | Chiusura con lotti |
|---|:---:|:---:|:---:|
| Montaggio | ✅ | ✅ | ✅ |
| Carpenteria | ✅ | ✅ | 🔄 In corso |
| Officina | ✅ | ✅ | 🔄 In corso |
| Collaudo | ✅ | ✅ | ⏳ Pianificato |

---

## Uso interno

Questo progetto è sviluppato per uso **esclusivamente interno**. Non è prevista distribuzione pubblica né un sistema di autenticazione multi-utente nella versione attuale.

Per eseguire l'applicazione in ambiente locale:

```bash
pip install -r requirements.txt
python run.py
```

L'applicazione sarà disponibile su `http://localhost:5000`.

---

*Sviluppato e mantenuto internamente — tutti i diritti riservati.*
