"""Nomi personali delle fasi: solo presentazione, mai codici di produzione."""
import re


PREFERENCE_KEY = "phase_labels"
MAX_LABELS = 100
MAX_LABEL_LENGTH = 80
_PHASE_CODE = re.compile(r"(?:fase\s+)?([0-9]{1,6})(?:\.0+)?", re.IGNORECASE)


def phase_code(value):
    text = "" if value is None else str(value).strip()
    match = _PHASE_CODE.fullmatch(text)
    return str(int(match[1])) if match else None


def get_phase_labels(user):
    if not getattr(user, "is_authenticated", False):
        return {}
    preferences = user.preferences
    value = preferences.get(PREFERENCE_KEY, {}) if isinstance(preferences, dict) else {}
    if not isinstance(value, dict):
        return {}
    return {
        code: label.strip()
        for key, label in value.items()
        if (code := phase_code(key)) is not None
        and isinstance(label, str) and label.strip()
    }


def phase_label(value, labels):
    text = "" if value is None else str(value)
    # I gruppi mostrano "1 + 2", le regole di visibilità "1,2".
    parts = re.split(r"(\s*[+,]\s*)", text)
    if len(parts) > 1 and all(phase_code(part) is not None for part in parts[::2]):
        return "".join(phase_label(part, labels) if i % 2 == 0 else part
                       for i, part in enumerate(parts))
    code = phase_code(value)
    if code is None:
        return text
    return labels.get(code) or code


def validate_phase_labels(codes, names):
    if len(codes) != len(names) or len(codes) > MAX_LABELS:
        raise ValueError(f"Puoi configurare al massimo {MAX_LABELS} fasi.")
    labels, seen = {}, set()
    for raw_code, raw_name in zip(codes, names):
        raw_code, name = raw_code.strip(), raw_name.strip()
        if not raw_code and not name:
            continue
        if not re.fullmatch(r"[0-9]{1,6}", raw_code):
            raise ValueError("Indica un numero di fase intero tra 0 e 999999.")
        code = str(int(raw_code))
        if code in seen:
            raise ValueError(f"La fase {code} è presente più volte.")
        seen.add(code)
        if len(name) > MAX_LABEL_LENGTH or any(ord(char) < 32 for char in name):
            raise ValueError(f"Ogni nome deve avere al massimo {MAX_LABEL_LENGTH} caratteri, su una sola riga.")
        if name:
            labels[code] = name
    return labels
