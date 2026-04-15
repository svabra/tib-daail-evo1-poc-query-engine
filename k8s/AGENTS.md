# Kubernetes Folder Guide

## Responsibility
- OpenShift/Kubernetes manifests for the current single-container PoC deployment.

## Working Rules
- Keep env var names, config/secret wiring, and trust-store mount assumptions aligned with `Settings.from_env()` and startup code.
- Avoid drift between BDW and DuckDB manifests when ports, names, or mounts change.