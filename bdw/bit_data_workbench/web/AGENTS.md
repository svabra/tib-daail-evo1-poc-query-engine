# Web Folder Guide

## Responsibility
- Owns HTML page routes and Jinja template composition for the browser UI.

## Working Rules
- Keep this layer focused on page assembly.
- Do not duplicate backend state logic or API payload shaping here.
- Preserve route names and fragment boundaries used by sidebar/workbench refreshes.