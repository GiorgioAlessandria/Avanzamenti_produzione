from app_odp.models import Famiglia, InputOdp, db
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniConflictError,
    VenditeAssegnazioniError,
)
from app_odp.vendite_models import VenditeRaggruppamento


def build_machine_grouping():
    groups = [
        {"id": row.id, "name": row.nome, "family_codes": row.famiglie,
         "version": row.versione}
        for row in VenditeRaggruppamento.query.order_by(
            VenditeRaggruppamento.nome_chiave, VenditeRaggruppamento.id
        ).all()
    ]
    families = {
        row.Codice.strip(): (row.Descrizione or "").strip()
        for row in Famiglia.query.order_by(Famiglia.id).all()
        if row.Codice and row.Codice.strip()
    }
    # Comprende anche codici presenti sugli ODP ma non ancora nell'anagrafica.
    for (code,) in db.session.query(InputOdp.CodFamiglia).distinct():
        if code and code.strip():
            families.setdefault(code.strip(), "")
    # Conserva le famiglie configurate anche quando non hanno più ODP presenti.
    for group in groups:
        for code in group["family_codes"]:
            families.setdefault(code, "")
    return {
        "groups": groups,
        "families": [{"code": code, "description": families[code]}
                     for code in sorted(families, key=str.casefold)],
    }


def _group_for_update(group_id, payload):
    if type(group_id) is not int or group_id <= 0:
        raise VenditeAssegnazioniError("Raggruppamento non valido.")
    row = db.session.get(VenditeRaggruppamento, group_id)
    if row is None:
        raise VenditeAssegnazioniConflictError("Il raggruppamento non esiste più. Aggiornare la pagina.")
    if (not isinstance(payload, dict) or type(payload.get("version")) is not int
            or payload["version"] != row.versione):
        raise VenditeAssegnazioniConflictError("Il raggruppamento è stato modificato. Aggiornare la pagina.")
    return row


def save_machine_group(payload):
    if not isinstance(payload, dict):
        raise VenditeAssegnazioniError("Dati del raggruppamento non validi.")
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 80:
        raise VenditeAssegnazioniError("Inserire un nome da 1 a 80 caratteri.")
    codes = payload.get("family_codes")
    if (not isinstance(codes, list) or not codes or len(codes) > 500
            or any(not isinstance(code, str) or not code.strip() for code in codes)):
        raise VenditeAssegnazioniError("Selezionare da 1 a 500 famiglie.")
    codes = sorted({code.strip() for code in codes}, key=str.casefold)
    known = {family["code"] for family in build_machine_grouping()["families"]}
    if set(codes) - known:
        raise VenditeAssegnazioniError("Una delle famiglie selezionate non è più disponibile.")
    row = _group_for_update(payload["id"], payload) if payload.get("id") is not None else None
    name = name.strip()
    key = name.casefold()
    duplicate = VenditeRaggruppamento.query.filter_by(nome_chiave=key).first()
    if duplicate is not None and (row is None or duplicate.id != row.id):
        raise VenditeAssegnazioniConflictError("Esiste già un raggruppamento con questo nome.")
    if row is None:
        row = VenditeRaggruppamento()
        db.session.add(row)
    row.nome, row.nome_chiave, row.famiglie = name, key, codes
    db.session.flush()
    return row


def delete_machine_group(group_id, payload):
    db.session.delete(_group_for_update(group_id, payload))
    db.session.flush()
