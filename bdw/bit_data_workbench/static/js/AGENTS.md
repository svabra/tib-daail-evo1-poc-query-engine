# Frontend JS Folder Guide

## Responsibility
- Browser runtime and UI orchestration.
- `dialogs.js` owns modal creation.
- `local-workspace-picker.js` owns Local Workspace picker rendering.
- `app.js` is still the main controller and still contains the largest remaining render clusters.
- `workbench-navigation-controller.js` owns cross-workbench entry/navigation actions.
- `source-sidebar-click-controller.js` owns sidebar action-menu behavior.

## Working Rules
- Preferred extraction order: pure render/markup modules first, controller/network flows second.
- For new workbenches, add page-specific modules and keep `app.js` to bootstrap and wiring.
- Keep sidebar action labels and behavior aligned between `templates/partials/sidebar.html` and `local-workspace-sidebar.js`.
- Put guided-flow dialog DOM in `dialogs.js` only if the interaction truly belongs to a reusable modal pattern; otherwise keep page markup server-rendered.
- Preserve `data-*` hooks shared with templates, sidebar refreshes, and Playwright scripts.
- Local Workspace data is browser-local IndexedDB; Shared Workspace flows go through backend/S3 APIs.

## Validation
- Load the app locally after changes.
- Use the Playwright smoke scripts in `scripts/` for high-risk UI flows.
