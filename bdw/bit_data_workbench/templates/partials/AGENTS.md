# Template Partials Guide

## Responsibility
- HTML fragments refreshed independently for sidebar, workbench panes, home, and tree nodes.

## Working Rules
- Fragment contracts are brittle; preserve root structure and `data-*` hooks unless callers change too.
- If changing a partial, cross-check matching selectors in `static/js/app.js` and extracted JS modules.
- Keep server-rendered copy concise and leave interaction behavior to JS.