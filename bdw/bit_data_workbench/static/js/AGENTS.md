# Frontend JS Folder Guide

## Responsibility
- Browser runtime and UI orchestration.
- `dialogs.js` owns modal creation.
- `local-workspace-picker.js` owns Local Workspace picker rendering.
- `app.js` is still the main controller and still contains the largest remaining render clusters.

## Working Rules
- Preferred extraction order: pure render/markup modules first, controller/network flows second.
- Preserve `data-*` hooks shared with templates, sidebar refreshes, and Playwright scripts.
- Local Workspace data is browser-local IndexedDB; Shared Workspace flows go through backend/S3 APIs.

## Validation
- Load the app locally after changes.
- Use the Playwright smoke scripts in `scripts/` for high-risk UI flows.