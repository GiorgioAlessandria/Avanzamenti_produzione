sequenceDiagram
participant
A
as
Client
A(utente
1
)
participant
F
as
Flask
participant
DB
as
SQLite
participant
B
as
Client
B(utente
2
)

A->
>
F: POST / api / odp / claim
{
    id_documento, id_riga
}
F->
>
DB: BEGIN(transaction)
F->
>
DB: INSERT
odp_in_carico(...)
F->
>
DB: UPDATE
input_odp
SET
StatoOrdine =
...
F->
>
DB: INSERT
change_event(topic, payload_json, created_at)
F->
>
DB: COMMIT
F-- >> A
:
200
{
    status:"claimed"
}

loop
polling
ogni
N
secondi
B->
>
F: GET / api / home / <tab>/bridge?after=lastChangeEventId
                           F->>DB: SELECT max(change_event.id)
                           alt changed
                           F-->>B: {changed:true,last_event_id,fragments}
                           B-->>B: applyChangeEvent(): sostituisce tbody + UI refresh
                           else unchanged
                           F-->>B: {changed:false,last_event_id}
                           end
                           end