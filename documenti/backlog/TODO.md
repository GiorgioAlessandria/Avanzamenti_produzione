# TODO delle task da integrare a progetto

## 01 - Chiusura degli ordini di produzione __INTEGRATO__

    - Stato: Integrato
    - Descrizione: Implementare le funzionalità per chiudere gli ordini di produzione
    - Priorità: alta
    - Area: app_odp e sync_odp

### Obiettivo

Procedura per la chiusura degli ordini di produzione nella totalità dei casi previsti a gestionale e interni

#### Fase 1: Procedura normale - OK

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

#### Fase 2: Eccezione 1 - Ordini parziali - OK

1. Creare una colonna nella tabella ordini che prevede una colonna per il materiale parziale e cambiare la
   colonna dove
   punta la table html (QtyDaLavorare)
2. Attivare la spunta per la chiusura parziale in modo da scalare un parziale e poi sospendere automaticamente l'ordine.
3. L'evento diventa "chiusura parziale" con le quantità prodotte.
4. L'ordine di produzione viene sospeso e viene modificata la tabella con la quantità rimanente

#### Fase 3: Eccezione 2 - Componenti multifase - OK

1. l'operatore apre e chiude l'ordine con fase 1
2. il sistema genera la chiusura della fase 1 dell'ordine con gli eventuali lotti (deve essere anche pensato per gli
   ordini parziali)
3. all'ordine in input_odp viene cambiata fase attiva e torna nella table da eseguire con stato Pianificata.
4. Il procedimento si ripete fino a quando si raggiunge l'ultima fase che invece deve prevedere una chiusura normale

#### Fase 3.1 Ordini parziali - lotti - OK

Quando un ordine viene chiuso in maniera parziale il sistema deve fare riferimento alla quantità parziale per scalare i
lotti.

#### Fase 3.2 Logica ok/ko macchine - OK

Rimuovere la logica di ok/ko a livello di macchina. Il sistema dovrà prevedere di inserire
automaticamente ok la logica m

#### Fase 4 Blocco chiusura dell'ordine in sospeso - Rifiutato

Se l'ordine è in sospeso l'operatore non può chiudere l'ordine, ma deve riattivarlo e chiuderlo di conseguenza

## 02 - Responsabile di qualità

    - Stato: In corso
    - Descrizione: implementazione pagina collaudo
    - Priorità: medio-alta
    - Area: app_odp/policy e templates

### Obiettivo

Integrare la logica nella policy per il responsabile di qualità

#### Fase 1: Debug e test

1. Verificare che la policy per il responsabile di qualità sia correttamente implementata e funzioni come previsto
2. Eseguire test approfonditi per identificare e risolvere eventuali bug o problemi di funzionalità
3. Assicurarsi che la pagina collaudo sia accessibile solo agli utenti con il ruolo di responsabile di qualità
4. Inserimento home collaudo con le varie impostazioni

## 03 - Impostazioni utente

    - Stato: da iniziare
    - Descrizione: implementazione impostazioni utente e reparto
    - Priorità: medio-alta
    - Area: app_odp/policy e templates

### Obiettivo

Integrare le impostazioni utente e per reparto

#### Fase 1: Debug e test

## 03 - Implementazione pagine di supporto

    - Stato: da iniziare
    - Descrizione: Implementazione pagine di supporto utente
    - Priorità: media
    - Area: app_odp e templates

### Obiettivo

Inserire le pagine di supporto utente per migliorare l'esperienza d'uso dell'applicazione e fornire assistenza agli
utenti

#### Fase 1: Impostazioni utente

Impostazioni a utente. Cambio dimensioni font e tema.

#### Fase 2: Dashboard reparto

Dashboard reparto con statistiche di produzione, ordini in corso, ordini chiusi, performance del reparto, ecc.

#### Fase 3: Dashboard responsabile produzione

Dashboard responsabile produzione con statistiche di produzione, ordini in corso, ordini chiusi, performance del
reparto,

#### Fase 4: Pagina priorità

Pagina che permette di impostare le priorità di produzione per gli ordini da aprire

## 04 - Test e debug generale

    - Stato: da iniziare
    - Descrizione: Generazione di test e debug generale per l'applicazione
    - Priorità: media
    - Area: Avanzamenti_produzione

### Obiettivo

Creare test e debug generale per l'applicazione al fine di garantire la stabilità, la sicurezza e le prestazioni
ottimali

#### Fase 1: Aggiornamento test sync_odp

Aggiornare i test esistenti per la sincronizzazione degli ordini di produzione (sync_odp)

#### Fase 2: Creazione test per app_odp

Creare test per l'applicazione di gestione degli ordini di produzione (app_odp)

## 05 - Cambio logica gestione dati input

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

## 06 - Gestione acquisti

    - Stato: da iniziare
    - Descrizione: pagina per il responsabile acquisti
    - Priorità: bassa
    - Area: app_odp

### Obiettivo

Creare una o più pagine dedicate al responsabile acquisti per l'andamento della produzione

#### Fase 1: creazione pagina di riepilogo acquisti

Pagina con gli odp in esecuzione e il materiale impiegato vs il materiale in magazzino

## 07 - Gestione vendite

    - Stato: da iniziare
    - Descrizione: pagina per i responsabili vendite
    - Priorità: bassa
    - Area: app_odp

### Obiettivo

Creare una o più pagine dedicate alle responsabili vendite per l'andamento della produzione

#### Fase 1: Pagina macchine finite

Creazione di una pagina che mostra le macchine finite con le matricole e gli eventuali clienti assegnati