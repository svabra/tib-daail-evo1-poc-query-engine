"""HTTP representations of one tabular product, sharing its existing DaCa policy."""

from __future__ import annotations

import re
import uuid
from urllib.parse import urlsplit

import httpx

from ..data_exporters import FORMAT_CONTENT_TYPES
from .daca_client import DacaPublicationError, response_error_detail


DOWNLOAD_FORMATS = {"csv": "CSV", "xlsx": "Excel (.xlsx)", "parquet": "Parquet"}


def supports_tabular_downloads(product) -> bool:
    source = product.source
    return source.source_kind == "relation" and bool(
        source.relation or (source.bucket and source.key.lower().endswith(".parquet"))
    )


def ensure_catalog_http_formats(
    client: httpx.Client, *, base_url: str, product_id: str, owner_user_id: str, public_path: str
) -> None:
    """Add missing descriptors through the owner API, with revision checks and audit.

    Existing descriptors, metadata, publication identities and policies are retained.
    Re-read after a conflict so concurrent reconciliation cannot add duplicates.
    """
    uuid.UUID(product_id)
    if not owner_user_id or not re.fullmatch(r"/api/public/data-products/[a-z0-9-]+", public_path):
        raise ValueError("HTTP formats require a product owner and a canonical public product path.")
    headers = {"Accept": "application/json", "X-DaCa-User": owner_user_id}
    product_url = f"{base_url}/api/v1/data-products/{product_id}"

    def read(url):
        response = client.get(url, headers=headers)
        if response.status_code != 200:
            raise DacaPublicationError(
                f"DaCa HTTP format reconciliation failed: {response_error_detail(response)}",
                kind="unavailable", upstream_status=response.status_code,
            )
        return response.json()

    for _ in range(8):
        product = read(product_url)
        if not isinstance(product, dict) or product.get("id") != product_id or product.get("ownerUserId") != owner_user_id:
            raise ValueError("DaCa returned a different product or owner during format reconciliation.")
        revision = product.get("revision")
        if type(revision) is not int or revision < 1:
            raise ValueError("DaCa returned no valid product revision.")
        endpoints = read(product_url + "/endpoints")
        if not isinstance(endpoints, list) or not all(isinstance(item, dict) for item in endpoints):
            raise ValueError("DaCa returned invalid endpoint metadata.")
        connections = [item.get("connection", {}) for item in endpoints if item.get("protocol") == "http-rest"]
        primary = next((item for item in connections if isinstance(item, dict)
                        and item.get("path") == public_path and item.get("method", "GET") == "GET"
                        and item.get("mediaType", "application/json") == "application/json"), None)
        if primary is None:
            raise ValueError("The existing product REST endpoint is required before adding formats.")
        origin = str(primary.get("baseUrl", "")).rstrip("/")
        parsed = urlsplit(origin)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("The product HTTP origin is invalid.")
        missing = []
        for export_format, label in DOWNLOAD_FORMATS.items():
            connection = {"baseUrl": origin, "path": public_path + "/download/" + export_format,
                          "method": "GET", "mediaType": FORMAT_CONTENT_TYPES[export_format]}
            if not any(all(existing.get(key) == value for key, value in connection.items())
                       for existing in connections if isinstance(existing, dict)):
                missing.append((label, connection))
        if not missing:
            return
        label, connection = missing[0]
        response = client.post(product_url + "/endpoints", headers={**headers, "If-Match": f'"{revision}"'}, json={
            "name": f"{public_path.rsplit('/', 1)[-1]} · HTTP / {label}"[:255],
            "description": "Vollständiger Datenstand desselben Produkts. Gemeinsame HTTP-Produktfreigabe für REST, CSV, Excel und Parquet.",
            "protocol": "http-rest", "connection": connection,
        })
        if response.status_code == 412:
            continue
        if response.status_code != 201:
            raise DacaPublicationError(
                f"DaCa HTTP format registration failed: {response_error_detail(response)}",
                kind="unavailable", upstream_status=response.status_code,
            )
    raise DacaPublicationError("DaCa product changed repeatedly during HTTP format registration.", kind="conflict")
