from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent
TESTS_ROOT = PROJECT_ROOT / "tests"

TEST_GROUPS = {
    "all": [
        TESTS_ROOT / "test_app_odp",
        TESTS_ROOT / "test_sync",
    ],
    "app": [
        TESTS_ROOT / "test_app_odp",
    ],
    "sync": [
        TESTS_ROOT / "test_sync",
    ],
}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Esegue i test del progetto Avanzamenti_produzione."
    )

    parser.add_argument(
        "group",
        nargs="?",
        choices=TEST_GROUPS,
        default="all",
        help="Gruppo di test da eseguire: 'all', 'app' oppure 'sync'. Default: all.",
    )

    parser.add_argument(
        "--failed",
        action="store_true",
        help="Riesegue prima i test falliti nell'ultima esecuzione.",
    )

    parser.add_argument(
        "--stop",
        action="store_true",
        help="Interrompe l'esecuzione al primo errore.",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Riduce le informazioni mostrate a terminale.",
    )

    return parser.parse_args()


def validate_test_directories(paths: list[Path]) -> None:
    missing_paths = [path for path in paths if not path.exists()]

    if missing_paths:
        missing = "\n".join(f"- {path}" for path in missing_paths)

        raise FileNotFoundError(
            f"Le seguenti cartelle di test non esistono:\n{missing}"
        )


def main() -> int:
    args = parse_arguments()
    selected_paths = TEST_GROUPS[args.group]

    try:
        validate_test_directories(selected_paths)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        return 2

    pytest_arguments = [
        *(str(path) for path in selected_paths),
        "--tb=short",
        "--strict-markers",
    ]

    if args.quiet:
        pytest_arguments.append("-q")
    else:
        pytest_arguments.append("-v")

    if args.failed:
        pytest_arguments.append("--failed-first")

    if args.stop:
        pytest_arguments.append("-x")

    print(f"Progetto: {PROJECT_ROOT}")
    print(f"Gruppo test: {args.group}")
    print("-" * 70)

    return pytest.main(pytest_arguments)


if __name__ == "__main__":
    sys.exit(main())
