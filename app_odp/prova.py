from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation


def _text(value) -> str:
    return str(value or "").strip()


def _to_decimal(value) -> Decimal:
    raw = _text(value).replace(",", ".")
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return Decimal("0")


q_lavorata = Decimal("192")

ore_per_pezzo = Decimal("0")
tempo_funzionamento = _to_decimal(9.5)
if q_lavorata > 0:
    ore_per_pezzo = (tempo_funzionamento / q_lavorata).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
print(tempo_funzionamento, q_lavorata, ore_per_pezzo)
