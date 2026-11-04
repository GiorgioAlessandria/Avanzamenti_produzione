"""Adattamento della home LCT: nessuna modifica a runtime, gruppi o export."""
from app_odp.services.order_helpers import _fase_to_int, _norm_text


def uses_lct_home(policy, config):
    return (
        config is not None
        and config.renderer == "standard"
        and any(_norm_text(role.name).lower() == "operatore_officina_lct"
                for role in getattr(policy, "direct_assigned_roles", ()))
        and policy.can("home_lct_multifase")
    )


def lct_home_rows(rows):
    """Espande la rappresentazione dei gruppi, mantenendo i comandi di gruppo."""
    result = []
    for row in rows:
        if not getattr(row, "IsWorkGroup", False):
            row.LctGroup = None
            row.LctMode = "Principale" if _fase_to_int(row.FaseAttiva) == 1 else "Singolo"
            result.append(row)
            continue
        members = {(m.IdDocumento, m.IdRiga): m for m in row.WorkGroupMembers}
        for order in row.WorkGroupOrdini:
            member = members[(order.IdDocumento, order.IdRiga)]
            order.LctGroup = row
            order.LctMode = {"FULL": "Principale", "ZERO": "Mascherato", "SPLIT": "Multiplo"}.get(member.TimeShareMode, "")
            result.append(order)
    return result


def lct_sections(orders):
    sections = {"fresa_da_eseguire": [], "fresa_in_corso": [],
                "buratto_da_eseguire": [], "buratto_in_corso": []}
    for order in orders:
        phase = _fase_to_int(order.FaseAttiva)
        if phase not in (1, 2) or order.StatoOrdine == "Chiusa":
            continue
        kind = "fresa" if phase == 1 else "buratto"
        state = "da_eseguire" if order.StatoOrdine == "Pianificata" else "in_corso"
        sections[f"{kind}_{state}"].append(order)
    return sections


def lct_fresa_busy(orders, username):
    return any(_fase_to_int(o.FaseAttiva) == 1 and o.StatoOrdine == "Attivo"
               and o.runtime_row and o.runtime_row.Utente_operazione == username for o in orders)
