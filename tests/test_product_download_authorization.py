from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bdw"))
from bit_data_workbench.api.data_products import router
from bit_data_workbench.backend.data_products.authorization import (
    DacaPolicyDenied,
    DacaPolicyUnavailable,
)
from bit_data_workbench.dependencies import get_workbench_service


@pytest.mark.parametrize(
    "error, expected", [(None, 401), (DacaPolicyDenied(), 403), (DacaPolicyUnavailable(), 503)]
)
def test_downloads_fail_closed_before_any_data_is_read(error, expected):
    product = SimpleNamespace(
        daca_publication=SimpleNamespace(product_id="11111111-1111-4111-8111-111111111111")
    )
    service = SimpleNamespace(
        settings=SimpleNamespace(daca_http_timeout_seconds=1),
        data_product_by_slug=lambda slug: product,
        download_data_product=Mock(),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_workbench_service] = lambda: service
    headers = {"X-DaCa-User": "jan.waser"} if error else {}
    with patch("bit_data_workbench.api.data_products.DacaPolicyEnforcer") as enforcer:
        if error:
            enforcer.return_value.authorize.side_effect = error
        with TestClient(app) as client:
            for export_format in ("rest", "xlsx", "parquet", "csv"):
                suffix = "" if export_format == "rest" else f"/download/{export_format}"
                response = client.get(
                    f"/api/public/data-products/mwst-sg{suffix}", headers=headers
                )
                assert response.status_code == expected
    service.download_data_product.assert_not_called()


def test_one_product_grant_authorizes_every_http_representation(tmp_path):
    from bit_data_workbench.backend.data_products.downloads import ProductDownload
    from bit_data_workbench.backend.data_exporters import FORMAT_CONTENT_TYPES

    catalog_id = "11111111-1111-4111-8111-111111111111"
    product = SimpleNamespace(daca_publication=SimpleNamespace(product_id=catalog_id), source=SimpleNamespace(source_kind="relation"))
    def artifact(*, slug, export_format):
        directory = tmp_path / export_format
        directory.mkdir()
        path = directory / f"{slug}.{export_format}"
        path.write_bytes(b"authorized product")
        return ProductDownload(path, directory, path.name, FORMAT_CONTENT_TYPES[export_format])

    service = SimpleNamespace(settings=SimpleNamespace(daca_http_timeout_seconds=1), data_product_by_slug=lambda slug: product,
                              public_data_product_relation=Mock(return_value={"items":[{"canton_code":"SG"}]}), download_data_product=Mock(side_effect=artifact))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_workbench_service] = lambda: service
    with patch("bit_data_workbench.api.data_products.DacaPolicyEnforcer") as enforcer, TestClient(app) as client:
        for suffix in ("", "/download/csv", "/download/xlsx", "/download/parquet"):
            response = client.get("/api/public/data-products/mwst-sg" + suffix, headers={"X-DaCa-User":"jan.waser"})
            assert response.status_code == 200
        decisions = enforcer.return_value.authorize.call_args_list
        assert len(decisions) == 4
        assert {call.kwargs["product_id"] for call in decisions} == {catalog_id}
        assert {call.kwargs["subject_id"] for call in decisions} == {"jan.waser"}
