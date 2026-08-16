from __future__ import annotations

from fastapi.staticfiles import StaticFiles


class VersionedStaticFiles(StaticFiles):
    """Serve build-versioned assets immutably and other assets for one hour."""

    def __init__(self, *args, cache_control_override: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_control_override = str(cache_control_override or "").strip() or None

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if self._cache_control_override is not None:
            response.headers["Cache-Control"] = self._cache_control_override
        elif b"v=" in scope.get("query_string", b""):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        else:
            response.headers.setdefault("Cache-Control", "public, max-age=3600")
        return response
