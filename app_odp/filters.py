# filters.py (o dentro create_app)
import json


def register_filters(app):
    @app.template_filter("db_json")
    def db_json(value):
        """
        Accetta:
          - stringa JSON (es. '["10","20"]', '[{"key1":"10"}]')
          - lista/dict già python
        Ritorna:
          - oggetto python (list/dict) oppure None se non parseabile
        """
        if value is None:
            return None
        if isinstance(value, (list, dict)):
            return value
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                return None
        return None

    @app.template_filter("db_list_display")
    def db_list_display(value):
        """
        Casi supportati:
          - ["10"]        -> "10"
          - ["10","20"]   -> "10, 20"
        Se non è lista di scalari, ritorna stringa vuota.
        """
        obj = db_json(value)
        if isinstance(obj, list) and all(isinstance(x, (str, int, float)) for x in obj):
            return ", ".join(str(x) for x in obj)
        return ""
