from pathlib import Path


TEMPLATE = (
    Path(__file__).parents[2]
    / "app_odp"
    / "templates"
    / "vendite_assegnazioni.j2"
)


def test_evasion_actions_are_only_on_customer_orders():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert "Evadi riga" in source
    assert "Evadi ordine" in source
    assert "data-confirm-shipment" in source
    assert "data-confirm-order-shipment" in source
    assert "data-ship-stock" not in source
    assert "data-stock-shipment-url" not in source
    assert ">Spedisci<" not in source
