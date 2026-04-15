# Tests Folder Guide

## Responsibility
- Automated regression suite for backend and launcher behavior.

## Working Rules
- `pytest` is the primary runner; it collects the existing `unittest`-style modules and emits coverage automatically.
- Prefer focused tests for public behavior over implementation-coupled assertions.
- Mirror the code split: add neighboring test modules or extend the closest existing file.
- Keep the `bdw` path bootstrap pattern unless packaging changes repo-wide.