# Roadmap test

Obiettivo: coprire cio che viene eseguito con modifiche piccole, leggibili e
verificabili. Ogni passo deve aggiungere pochi test su una funzione alla volta.

## Regola di lavoro

1. Un file o una funzione alla volta.
2. Prima i test su funzioni pure, poi funzioni che leggono DB, infine funzioni che scrivono DB o generano eventi.
3. Ogni test deve avere input minimo, output atteso esplicito e nessun dato reale.
4. Se il test tocca log eventi, outbox ERP, cancellazioni, priorita utente o comportamento UI visibile, chiedere conferma prima.
5. Non aggiungere fixture generiche finche due test non usano davvero lo stesso setup.

## Stato di avanzamento

Copertura aggiunta per i punti senza dubbi funzionali o gia confermati:

- Fase 1 completa.
- Fase 2 completa.
- Fase 3 completa.
- Fase 4 completa.
- Fase 6: punto 1.
- Fase 7 completa.

Prossimo passo: Fase 5, punto 1 (`_order_kind`), dopo conferma dei codici ordine.

## Fase 1 - helper puri, rischio basso

Questi test non devono aprire Flask, non devono scrivere DB e non devono usare file reali.

| Ordine | Modifica piccola | Funzione | File test |
| --- | --- | --- | --- |
| 1 | Aggiungere casi base formato riferimento ordine | `format_erp_decimal_ref_part` | `tests/test_app_odp/test_ordine_ref.py` |
| 2 | Aggiungere caso display con valori mancanti | `format_ordine_ref_display` | `tests/test_app_odp/test_ordine_ref.py` |
| 3 | Aggiungere caso export con numero riga decimale | `format_ordine_ref_export` | `tests/test_app_odp/test_ordine_ref.py` |
| 4 | Coprire parsing JSON distinto da lista Python | `_load_distinta_base` | `tests/test_app_odp/test_odp_output.py` |
| 5 | Coprire suffisso fase vuoto, numerico, testo | `_normal_phase_suffix` | `tests/test_app_odp/test_odp_output.py` |
| 6 | Coprire riferimento fase multipla | `_phase_ref_for_export` | `tests/test_app_odp/test_odp_output.py` |
| 7 | Coprire parsing minuti non funzionamento | `_parse_minuti_non_funzionamento` | `tests/test_app_odp/test_order_helpers.py` |
| 8 | Coprire data registrazione da input utente | `_parse_registration_date_input` | `tests/test_app_odp/test_order_helpers.py` |
| 9 | Coprire estrazione codici da JSON/stringa | `_extract_codes_from_cell` | `tests/test_app_odp/test_order_helpers.py` |
| 10 | Coprire normalizzazione testo quantita | `_decimal_input_text` | `tests/test_app_odp/test_order_helpers.py` |

Done quando: i test girano con `pytest tests/test_app_odp/test_ordine_ref.py tests/test_app_odp/test_order_helpers.py tests/test_app_odp/test_odp_output.py`.

## Fase 2 - service read-only e payload

Qui si possono usare oggetti finti semplici o `SimpleNamespace`. DB solo se la funzione fa query internamente.

| Ordine | Modifica piccola | Funzione | Chiedere prima? |
| --- | --- | --- | --- |
| 1 | Aggiungere conversione ore/capacita | `_dashboard_tempo_previsto_ore` | No |
| 2 | Aggiungere stato dashboard normalizzato | `_dashboard_stato_norm` | No |
| 3 | Aggiungere data fine prevista valida/non valida | `_dashboard_data_fine_prevista` | No |
| 4 | Aggiungere payload ordine dashboard minimo | `_dashboard_order_payload` | Si, testo/etichette UI |
| 5 | Aggiungere filtro dashboard su reparto/stato | `_dashboard_row_matches_filters` | Si, comportamento utente |
| 6 | Aggiungere percentuale report con zero previsto | `_percent` | No |
| 7 | Aggiungere parsing fase da log report | `_phase_number_from_value` | No |
| 8 | Aggiungere ore runtime con fallback | `_worked_hours_with_fallback` | Si, regola di calcolo |
| 9 | Aggiungere formato storico evento | `_event_description` | Si, testo UI |
| 10 | Aggiungere filtro storico lato Python | `_row_matches_python_filters` | Si, comportamento utente |

File consigliati:

- `tests/test_app_odp/test_dashboard_service.py`
- `tests/test_app_odp/test_report_settimanale_service.py`
- `tests/test_app_odp/test_storico_ordini_service.py`

Done quando: ogni file contiene pochi test indipendenti e non serve ancora una fixture DB condivisa.

## Fase 3 - acquisti e documenti

Partire dalle funzioni che validano input o trasformano payload.

| Ordine | Modifica piccola | Funzione | Chiedere prima? |
| --- | --- | --- | --- |
| 1 | Coprire QR code valido/non valido | `_parse_scorta_qrcode` | Si, formato QR ufficiale |
| 2 | Coprire payload operatore scorta | `_scorta_operator_payload` | Si, default utente/reparto |
| 3 | Coprire row scorta serializzata | `_scorta_to_row` | Si, testo UI |
| 4 | Coprire filtri scorte aperte/chiuse | `_filter_acquisti_scorte_rows` | Si, comportamento utente |
| 5 | Coprire lookup articolo non trovato | `_find_scorta_lookup` | Si, cosa mostrare all'utente |
| 6 | Coprire ricerca articolo documenti | `api_ricerca_articolo` | Si, contratto JSON |

Saltare per ora test browser/end-to-end: costano molto e duplicano i test route finche i contratti JSON non sono stabili.

## Fase 4 - priorita operatori

Questa fase tocca DB e regole utente. Prima di ogni test confermare la regola di business.

| Ordine | Modifica piccola | Funzione | Domanda da fare |
| --- | --- | --- | --- |
| 1 | Test chiave ordine/fase | `_make_ordine_fase_key` | Nessuna |
| 2 | Test compattazione posizioni | `_compact_priorita_operatore` | Le priorita devono restare 1, 2, 3 con posizioni continue? |
| 3 | Test consumo priorita ordine | `_consume_priorita_ordine` | Va rimossa per tutti gli operatori o solo per uno? |
| 4 | Test snapshot priorita in runtime | `_snapshot_priorita_in_runtime` | Quali campi runtime sono contratto stabile? |
| 5 | Test ripristino fase successiva | `_restore_priorita_for_next_phase_from_runtime` | Quando una fase avanza, la priorita deve seguire la fase nuova? |
| 6 | Test ordinamento payload | `_apply_priorita_to_ordini` | L'ordinamento visibile deve essere per priorita, posizione, ordine? |

File consigliato: `tests/test_app_odp/test_priorita_service.py`.

## Fase 5 - gruppi ordini

Questa e la parte piu delicata: attiva ordini, scrive runtime, logga eventi e cambia stati. Fare un solo scenario alla volta.

| Ordine | Modifica piccola | Funzione | Domanda da fare |
| --- | --- | --- | --- |
| 1 | Test classificazione ordine | `_order_kind` | Quali codici distinguono standard/macchina/semilavorato? |
| 2 | Test ordine puo entrare in gruppo | `_order_can_enter_group` | Solo `Pianificata` o anche altri stati? |
| 3 | Test membro creato da ordine | `_create_member` | Campi obbligatori visibili nel gruppo? |
| 4 | Test tempo assegnato membro | `_assigned_seconds_for_member` | SPLIT/FULL/ZERO sono definitivi? |
| 5 | Test creazione gruppo multiplo con DB temporaneo | `create_multiplo_group` | Deve attivare subito tutti gli ordini? |
| 6 | Test sospensione gruppo | `suspend_group` | Quale log deve essere scritto? |
| 7 | Test riattivazione gruppo | `reactivate_group` | Deve bloccare altri ordini attivi dello stesso operatore? |
| 8 | Test chiusura membro singolo | `finalize_group_after_single_member_closure` | Il gruppo resta attivo o si scioglie? |
| 9 | Test chiusura gruppo completo | `finalize_group_after_member_closures` | Quando lo stato gruppo diventa `Chiuso`? |
| 10 | Test serializzazione gruppo | `group_to_dict` | Quali campi sono contratto API/UI? |

File consigliato: `tests/test_app_odp/test_ordini_gruppi_service.py`.

## Fase 6 - runtime, log, ERP outbox

Qui ogni test deve usare DB temporaneo e deve verificare una sola scrittura.

| Ordine | Modifica piccola | Funzione | Domanda da fare |
| --- | --- | --- | --- |
| 1 | Test snapshot runtime | `_runtime_snapshot` | Nessuna |
| 2 | Test accumulo tempo attivo | `_accumulate_runtime_until` | Arrotondamento secondi atteso? |
| 3 | Test applicazione fermo macchina | `_apply_stop_minutes_to_runtime` | Il fermo scala sempre dal tempo funzionamento? |
| 4 | Test stato operativo per chiusura | `_stato_operativo_chiusura` | Quali stati sono chiudibili? |
| 5 | Test log presa in carico | `_add_input_odp_takeover_log` | Campi log obbligatori per storico/ERP? |
| 6 | Test log sospensione | `_add_input_odp_suspend_log` | Causale obbligatoria o opzionale? |
| 7 | Test log chiusura | `_add_input_odp_closure_log` | Differenza chiusura parziale/finale? |
| 8 | Test log operazione runtime | `_append_operazione_log` | Topic/scope sono contratto stabile? |
| 9 | Test payload outbox fase | `_build_phase_payload` | Campi richiesti dal gestionale? |
| 10 | Test coda export fase | `_queue_phase_export` | Quando bloccare doppio export? |

File consigliati:

- `tests/test_app_odp/test_ordini_runtime_service.py`
- `tests/test_app_odp/test_ordini_log_service.py`
- `tests/test_app_odp/test_erp_export_service.py`

## Fase 7 - sync mancanti

Replicare lo stile gia usato in `tests/test_sync/test_sync_input.py`, ma senza copiare fixture grandi se non servono.

| Ordine | Modifica piccola | Funzione | File test |
| --- | --- | --- | --- |
| 1 | Test giorni lavorativi | `add_workdays` | `tests/test_sync/test_sync_acq.py` |
| 2 | Test articoli acquisti | `build_acq_articoli` | `tests/test_sync/test_sync_acq.py` |
| 3 | Test giacenze acquisti | `build_acq_giacenze` | `tests/test_sync/test_sync_acq.py` |
| 4 | Test fabbisogno da distinta | `build_acq_fabbisogno_odp` | `tests/test_sync/test_sync_acq.py` |
| 5 | Test riepilogo materiali | `build_acq_riepilogo_materiali` | `tests/test_sync/test_sync_acq.py` |
| 6 | Test collapse inventario | `_collapse_by_keys` | `tests/test_sync/test_estrazione_inventario.py` |
| 7 | Test espansione lotti inventario | `expand_giacenza_with_lotti` | `tests/test_sync/test_estrazione_inventario.py` |
| 8 | Test inserimento famiglia giacenze | `inserimento_descrizione_famiglia` | `tests/test_sync/test_sync_giacenze.py` |

Chiedere prima di testare funzioni che cancellano o sostituiscono tabelle intere: `_replace_table`, `elaborazione_dati_acq`, `elaborazione_dati`, `read_cycle`.

## Fase 8 - route modules

Le route vanno testate dopo i service. Ogni route deve avere solo contratti HTTP minimi.

| Ordine | Modifica piccola | Route/funzione | Domanda da fare |
| --- | --- | --- | --- |
| 1 | Smoke GET pagine principali con permessi finti | `dashboard_produzione`, `home_acquisti`, `dash_reparto` | Testo UI non serve, basta status? |
| 2 | JSON bridge dashboard | `api_dashboard_produzione_cruscotto` | Campi minimi del JSON? |
| 3 | JSON KPI dashboard | `api_dashboard_produzione_kpi` | Campi minimi del JSON? |
| 4 | Export Excel dashboard | `api_dashboard_produzione_kpi_export` | Basta header workbook? |
| 5 | Export acquisti Excel | `api_export_acquisti_excel` | Sezione non valida: 404 o 400? |
| 6 | Scorte segnala | `api_scorte_segnala` | Effetto DB e messaggio utente attesi? |
| 7 | Documenti PDF/materiale foto | `api_metodo_pdf`, `api_materiale_foto` | File mancanti: 404 o risposta vuota? |

## Sequenza consigliata delle prime 10 modifiche

1. `test_ordine_ref.py`: `format_erp_decimal_ref_part`.
2. `test_ordine_ref.py`: `format_ordine_ref_display`.
3. `test_ordine_ref.py`: `format_ordine_ref_export`.
4. `test_order_helpers.py`: `_parse_registration_date_input`.
5. `test_order_helpers.py`: `_parse_minuti_non_funzionamento`.
6. `test_odp_output.py`: `_load_distinta_base`.
7. `test_dashboard_service.py`: `_dashboard_tempo_previsto_ore`.
8. `test_dashboard_service.py`: `_dashboard_stato_norm`.
9. `test_report_settimanale_service.py`: `_percent`.
10. `test_sync_acq.py`: `add_workdays`.

Queste dieci modifiche sono intenzionalmente piccole: nessuna dovrebbe richiedere DB reale, login reale o chiamate al gestionale.

## Criterio di stop

Fermarsi e chiedere quando:

- il risultato atteso dipende da come l'utente vede una pagina o un messaggio;
- la funzione scrive su `OdpRuntimeLog`, `InputOdpLog`, `ErpOutbox`, `OdpPriorita`, `OdpWorkGroup`;
- la funzione cancella, sostituisce o sincronizza tabelle;
- il test deve decidere tra comportamento attuale e comportamento desiderato;
- serve usare un dato reale del gestionale per capire la regola.

