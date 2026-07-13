"""Configurazione pytest condivisa per tutta la suite."""

# Script manuale di iniezione dati UI: non contiene test e importa modelli legacy.
collect_ignore = ["test_app_odp/test_inject_tool.py"]
