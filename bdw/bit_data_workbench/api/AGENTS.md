# API Folder Guide

## Responsibility
- Owns JSON, file-download, and SSE route handlers.
- Maps HTTP inputs onto backend service calls and returns payloads.

## Working Rules
- Keep routes thin; prefer calling `WorkbenchService` or dedicated backend helpers.
- Preserve request contracts based on `Query`, `Header`, and `Form` parameters.
- Keep response payload keys stable unless the matching frontend/tests are updated too.

## Validation
- Confirm `bit_data_workbench.main` still imports.
- Run focused tests when changing route behavior.