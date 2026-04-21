# API Folder Guide

## Responsibility
- Owns JSON, file-download, and SSE route handlers.
- Maps HTTP inputs onto backend service calls and returns payloads.

## Working Rules
- Keep routes thin; prefer calling `WorkbenchService` or dedicated backend helpers.
- As the surface grows, split feature routers out of `router.py` instead of leaving all endpoints in one file.
- Preserve request contracts based on `Query`, `Header`, and `Form` parameters.
- Keep response payload keys stable unless the matching frontend/tests are updated too.
- For public read-only APIs such as Data Products, keep management endpoints and consumer endpoints clearly separated.
- Derive deployment-specific absolute URLs from the request context instead of persisting them in backend state.
- If an endpoint streams raw files or objects, keep the JSON management surface separate from the streaming response.

## Validation
- Confirm `bit_data_workbench.main` still imports.
- Run focused tests when changing route behavior.
