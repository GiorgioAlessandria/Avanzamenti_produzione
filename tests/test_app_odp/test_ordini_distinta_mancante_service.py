import json
from types import SimpleNamespace

import pytest

from app_odp.services.ordini_distinta_mancante_service import (
    distinta_pendente_per_ordine,
    filter_export_distinta,
    partition_distinta_step,
)


def _component(codice, *, lotto="no"):
    return {
        "CodArt": codice,
        "VarianteArt": "",
        "DesArt": f"Componente {codice}",
        "Quantita": 1,
        "GestioneLotto": lotto,
    }


def test_partial_steps_export_only_mounted_components_until_completion():
    pending = [
        _component("A", lotto="si"),
        _component("B"),
        _component("C", lotto="si"),
    ]

    mounted, missing = partition_distinta_step(
        pending,
        [{"CodArt": "C", "VarianteArt": ""}],
    )

    assert [row["CodArt"] for row in mounted] == ["A", "B"]
    assert [row["CodArt"] for row in missing] == ["C"]
    exported = json.loads(filter_export_distinta(json.dumps(pending), mounted))
    assert [row["CodArt"] for row in exported] == ["A", "B"]

    mounted_final, missing_final = partition_distinta_step(missing, [])
    assert [row["CodArt"] for row in mounted_final] == ["C"]
    assert missing_final == []


def test_partial_step_rejects_unknown_or_all_missing_components():
    pending = [_component("A"), _component("B")]

    with pytest.raises(ValueError, match="non è nella distinta residua"):
        partition_distinta_step(pending, [{"CodArt": "X"}])

    with pytest.raises(ValueError, match="almeno un componente montato"):
        partition_distinta_step(
            pending,
            [{"CodArt": "A"}, {"CodArt": "B"}],
        )


def test_phase_without_erp_components_has_no_pending_bom():
    ordine = SimpleNamespace(
        IdDocumento="100",
        IdRiga="1",
        DistintaMateriale=json.dumps(
            [
                {"CodArt": "A", "VarianteArt": "", "NumFase": "1"},
                {"CodArt": "B", "VarianteArt": "", "NumFase": "1"},
            ]
        ),
    )

    # La fase 2 è solo di controllo: non deve recuperare residui di altre fasi.
    assert distinta_pendente_per_ordine(ordine, "2") == []
