from app_odp.services.ordini_runtime_service import _runtime_order_key


def test_runtime_order_key_strips_document_and_row_ids():
    assert _runtime_order_key(" DOC ", " 10 ") == ("DOC", "10")
