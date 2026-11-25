import re


PURCHASE_MAIL_FIELDS = {
    "numero_ordine": "Numero ordine",
    "fornitore": "Ragione sociale del fornitore",
    "codice_articolo": "Codice articolo",
    "descrizione_articolo": "Descrizione articolo",
    "data_consegna": "Data di consegna prevista",
    "quantita_residua": "Quantità ancora da consegnare",
    "unita_misura": "Unità di misura",
    "numero_solleciti": "Numero di solleciti registrati",
}
DEFAULT_PURCHASE_MAIL = {
    "subject": "Sollecito ordine {{numero_ordine}}",
    "body": (
        "Buongiorno,\n\n"
        "chiediamo un aggiornamento sulla consegna dell'articolo {{codice_articolo}} - "
        "{{descrizione_articolo}}, relativo al nostro ordine {{numero_ordine}}, "
        "con consegna prevista il {{data_consegna}} e quantità residua "
        "{{quantita_residua}} {{unita_misura}}.\n"
        "Potete confermarci la data di consegna aggiornata?\n\n"
        "Grazie, cordiali saluti."
    ),
}


def purchase_mail_template(user):
    preferences = user.preferences
    stored = preferences.get("purchase_reminder_mail", {}) if isinstance(preferences, dict) else {}
    if not isinstance(stored, dict):
        stored = {}
    return {
        key: stored[key] if isinstance(stored.get(key), str) and stored[key].strip() else default
        for key, default in DEFAULT_PURCHASE_MAIL.items()
    }


def validate_purchase_mail_template(subject, body):
    if not subject or len(subject) > 200 or "\n" in subject or "\r" in subject:
        raise ValueError("L'oggetto è obbligatorio e deve avere al massimo 200 caratteri su una sola riga.")
    if not body or len(body) > 2000:
        raise ValueError("Il testo è obbligatorio e deve avere al massimo 2000 caratteri.")
    for value in (subject, body):
        for field in re.findall(r"\{\{([^{}]*)\}\}", value):
            if field.strip() not in PURCHASE_MAIL_FIELDS:
                raise ValueError("Campo non riconosciuto: {{" + field.strip() + "}}.")
        remaining = re.sub(r"\{\{[^{}]*\}\}", "", value)
        if "{{" in remaining or "}}" in remaining:
            raise ValueError("Controllare le parentesi dei campi: utilizzare {{nome_campo}}.")
    return {"subject": subject, "body": body}
