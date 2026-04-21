# Template Partials Guide

## Responsibility
- HTML fragments refreshed independently for sidebar, workbench panes, home, and tree nodes.
- Owns most first-paint workbench markup, card grids, and source-menu action labels.

## Working Rules
- Fragment contracts are brittle; preserve root structure and `data-*` hooks unless callers change too.
- If changing a partial, cross-check matching selectors in `static/js/app.js` and extracted JS modules.
- Keep sidebar source actions mirrored with the Local Workspace renderer in `static/js/local-workspace-sidebar.js` when labels or menu order change.
- Give each workbench page a stable root `data-*` hook so route activation and smoke tests stay simple.
- Keep server-rendered copy concise and leave interaction behavior to JS.
