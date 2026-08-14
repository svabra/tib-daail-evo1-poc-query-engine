from __future__ import annotations

from fastapi.staticfiles import StaticFiles


class VersionedStaticFiles(StaticFiles):
    """Serve build-versioned assets immutably and other assets for one hour."""

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if b"v=" in scope.get("query_string", b""):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        else:
            response.headers.setdefault("Cache-Control", "public, max-age=3600")
        return response
