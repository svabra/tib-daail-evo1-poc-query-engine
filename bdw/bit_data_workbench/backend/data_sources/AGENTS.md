# Data Sources Folder Guide

## Responsibility
- Shared abstraction boundary for provider-specific source plugins and explorer helpers.

## Working Rules
- Put shared contracts and common types here.
- Keep source-specific logic in `postgres/` or `s3/`, not in `service.py`.
- Preserve stable `source_id` values because the frontend and notebooks depend on them.