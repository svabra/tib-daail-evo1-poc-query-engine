# S3 Data Source Guide

## Responsibility
- S3 provider implementations for plugin and explorer behavior.

## Working Rules
- Preserve bucket/prefix semantics used by the Shared Workspace explorer.
- Local MinIO behavior is sensitive to endpoint style and loopback resolution.
- Be careful with recursive delete and version-aware delete behavior; regressions here are user-visible quickly.