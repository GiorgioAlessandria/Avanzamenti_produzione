from pathlib import Path


TEMPLATE = (
    Path(__file__).parents[2]
    / "app_odp"
    / "templates"
    / "vendite_assegnazioni.j2"
)


def test_customer_order_evasion_controls_are_removed():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert "Evadi riga" not in source
    assert "Evadi ordine" not in source
    assert "data-confirm-shipment" not in source
    assert "conferma-spedizione" not in source
    assert "data-confirm-order-shipment" not in source
    assert "confirmOrderShipment" not in source
    assert "data-order-shipment-url-template" not in source
    assert "shipment_ready" not in source
    assert "data-ship-stock" not in source
    assert "data-stock-shipment-url" not in source
    assert ">Spedisci<" not in source
    assert "const orderClass = order.packaged" in source
    assert "const rowClass = row.packaged" in source
