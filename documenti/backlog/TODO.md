# TODO delle task da integrare a progetto

## 01 - Chiusura degli ordini di produzione

    - Stato: in corso
    - Descrizione: Implementare le funzionalità per chiudere gli ordini di produzione
    - Priorità: alta
    - Area: app_odp e sync_odp

### Obiettivo

Procedura per la chiusura degli ordini di produzione nella totalità dei casi previsti a gestionale e interni

#### Fase 1: Procedura normale - Integrata

1. L'operatore selezione la linea da chiudere in un table in corso. Si apre un toast html e inserisce la quantità di
   componenti prodotti e la quantità di componenti scartati.
    1. se il componente in input_odp ha la voce GestioneLotto a "si" allora bisogna verificare nella distinta base quali
       componenti hanno GestioneLotto a "si" e poi acquisire dalla tabella RBAC\giacenza_lotti l'elenco dei Lotti
       corrispondenti ai lotti del componente in distinta. Assegnazione dei lotti all'ordine (possono essere anche più
       lotti a ordine), sono da inserire con il numero di lotto e la quantità utilizzata
2. il programma crea l'evento di chiusura della riga con la quantità Ok/Ko, eventuali lotti in change_event
3. Il programma inserisce in un db_log tutte le voci riguardanti l'ordine dentro le tabelle
   change_event, input_odp, odp_in_carico per una futura analisi dei dati
4. Il programma cancella da change_event,
   input_odp, odp_in_carico tutte le voci riguardanti l'ordine selezionato

#### Fase 2: Eccezione 1 - Ordini parziali - Da integrare

Ordini chiusi in modo parziale (da attivare con una spunta), che mi permettono di scalare un parziale e poi sospendere
automaticamente l'ordine. In questo caso si esegue il punto 1, 1.1 e 2 ma cambiando tipologia di evento (chiusura
parziale) e inserendo le quantità prodotte, cambiando anche quelle a table html (da prevedere una nuova colonna per gli
ordini in carico in modo da tracciare le quantità parziali ancora da produrre). Quando non attivo la spunta allora
chiudo in modo totale l'ordine con il numero di componenti rimanenti eseguendo la procedura completa 1, 1.1, 2, 3 e 4

#### Fase 3: Eccezione 2 -Componenti multifase - Da integrare

Se il componente è multifase allora il sistema deve cambiare FaseAttiva al componente incrementandola di 1
e inserire lo StatoOrdine come Pianificata. In questo caso si esegue il punto 1, 1.1, 2 con un evento differente (
chiusura fase X). Quando la fase attuale corrisponde all'ultima di NumFase allora si esegue la procedura completa 1,
1.1, 2, 3 e 4

## 02 - Cambio logica gestione dati input

    - Stato: da iniziare
    - Descrizione: cambio logica dati input
    - Priorità: bassa
    - Area: RABC e sync_odp

### Obiettivo

Cambio logica per la gestione delle table del database migliorandone integrità, sicurezza.
La nuova logica prevede l'inserimento dei dati con più tabelle in modo da poter mantenere aggiornate le distinte base
e i cambi di gestione lotto senza dover modificare gli indicatori come lo stato o la fase attiva

#### Fase 1: Integrazione della logica

Fare riferimento al documento "documenti/input_odp_safe_update_todolist.pdf" per la descrizione dettagliata della nuova
logica da integrare. Il file è già pensato per AI

## 03 - Responsabile di qualità

    - Stato: da iniziare
    - Descrizione: implementazione pagina collaudo
    - Priorità: medio-alta
    - Area: app_odp/policy e templates

### Obiettivo

Integrare la logica nella policy per il responsabile di qualità

#### Fase 1: Debug e test

1. Verificare che la policy per il responsabile di qualità sia correttamente implementata e funzioni come previsto
2. Eseguire test approfonditi per identificare e risolvere eventuali bug o problemi di funzionalità
3. Assicurarsi che la pagina collaudo sia accessibile solo agli utenti con il ruolo di responsabile di qualità

## 04 - Implementazione pagine di supporto

    - Stato: da iniziare
    - Descrizione: Implementazione pagine di supporto utente
    - Priorità: media
    - Area: app_odp e templates

### Obiettivo

Inserire le pagine di supporto utente per migliorare l'esperienza d'uso dell'applicazione e fornire assistenza agli
utenti

#### Fase 1: Impostazioni utente

Impostazioni a utente. Cambio dimensioni font e tema.

#### Fase 2: Dasboard reparto

Dashboard reparto con statistiche di produzione, ordini in corso, ordini chiusi, performance del reparto, ecc.

#### Fase 3: Dashboard responsabile produzione

Dashboard responsabile produzione con statistiche di produzione, ordini in corso, ordini chiusi, performance del
reparto,

#### Fase 4: Pagina priorità

Pagina che permette di impostare le priorita di produzione per gli ordini da apririre