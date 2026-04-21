# Data Products Backend Guide

## Responsibility
- JSON-backed registry and live publication logic for managed data products.
- Source normalization, slug lifecycle, and public read adapters.

## Working Rules
- Keep persistence in `registry.py` and runtime behavior in `manager.py`.
- Reuse existing backend query and S3 helpers instead of duplicating connection logic.
- Keep `service.py` as a thin facade only.
