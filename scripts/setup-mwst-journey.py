"""Prepare only the MWST demo's sources and separate completed example."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bdw"))
from bit_data_workbench.backend.mwst_setup import setup_journey


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daaif-url", default="http://127.0.0.1:8011")
    parser.add_argument("--daca-url", default="http://127.0.0.1:8080")
    arguments = parser.parse_args()
    setup_journey(arguments.daaif_url, arguments.daca_url)
