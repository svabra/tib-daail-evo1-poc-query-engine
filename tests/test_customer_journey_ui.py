from __future__ import annotations

from json import loads
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
STATIC_ROOT = BDW_ROOT / "bit_data_workbench" / "static"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.api.workbench_metadata import notebook_search_index
from bit_data_workbench.backend.notebook_search import notebook_search_items
from bit_data_workbench.backend.runbooks import build_runbook_tree
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry
from bit_data_workbench.backend.static_assets import VersionedStaticFiles
from bit_data_workbench.models import NotebookDefinition
from bit_data_workbench.web.router import ingestion_workbench_partial, sidebar_partial


def request(path: str, *, partial: bool = False) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"hx-request", b"true")] if partial else [],
        }
    )


class UiService:
    settings = SimpleNamespace()

    def runtime_info(self):
        return {
            "image_version": "test-build",
            "service": "test",
            "hostname": "test",
        }

    def catalogs(self):
        return []

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def source_options(self):
        return [{"id": "heavy-source", "payload": "x" * 4_000_000}]

    def completion_schema(self):
        return {"heavy": "x" * 3_000_000}

    def data_generators(self):
        return DataGeneratorRegistry().definitions()

    def runbook_tree(self):
        return build_runbook_tree(self.data_generators())


class CustomerJourneyUiTests(unittest.TestCase):
    def test_federal_header_identity_and_asset_contract(self) -> None:
        header = (BDW_ROOT / "bit_data_workbench/templates/partials/federal_header.html").read_text(
            encoding="utf-8"
        )
        identity = (STATIC_ROOT / "js/demo-identity.js").read_text(encoding="utf-8")
        self.assertIn("federal-authority-strip", header)
        self.assertIn("data-demo-user-select", header)
        for name in (
            "Kassandra Valdata",
            "Noémie Rochat",
            "Beat Stalder",
            "Joel Ruod",
            "Thomas Kriegli",
        ):
            self.assertIn(name, header)
        self.assertIn('DAAIF_DEMO_USER_STORAGE_KEY = "daaif-demo-user"', identity)
        self.assertIn('DAAIF_DEMO_USER_CHANGE_EVENT = "daaif-demo-user-change"', identity)
        self.assertIn('DEFAULT_USER_ID = "joel.ruod"', identity)
        self.assertIn("document.documentElement.dataset.daaifDemoUser", identity)
        hero_files = list((STATIC_ROOT / "img/daca").glob("swiss-*.*"))
        self.assertEqual(len(hero_files), 40)
        self.assertTrue((STATIC_ROOT / "img/daca/kassandra-valdata.webp").is_file())
        self.assertTrue((STATIC_ROOT / "img/daca/noemie-rochat.webp").is_file())

    def test_notebook_search_document_is_lightweight_and_etagged(self) -> None:
        notebook = NotebookDefinition(
            notebook_id="journey-notebook",
            title="Kantonale Gewerbesteuer",
            summary="Soll Ist und Hochrechnung",
            cells=[],
            tags=["Steuern", "Journey"],
            tree_path=("Customer Journeys",),
        )
        items = notebook_search_items([notebook])
        self.assertEqual(
            set(items[0]),
            {"id", "title", "summary", "tags", "path", "type", "targetUrl"},
        )
        self.assertNotIn("cells", items[0])
        service = SimpleNamespace(notebooks=lambda: [notebook])
        response = notebook_search_index(service=service, if_none_match=None)
        payload = loads(response.body)
        self.assertEqual(payload["items"], items)
        replay = notebook_search_index(
            service=service, if_none_match=response.headers["etag"]
        )
        self.assertEqual(replay.status_code, 304)

    def test_browser_search_is_diacritic_insensitive_and_and_tokenized(self) -> None:
        module_uri = (STATIC_ROOT / "js/home-notebook-search.js").resolve().as_uri()
        script = f"""
          import {{
            WORKBENCH_LIVE_RESULT_LIMIT,
            normalizedDataProduct,
            normalizedDataSource,
            searchNotebookIndex,
            searchWorkbenchIndex,
            searchWorkbenchPreview,
            workbenchSearchIsReady,
          }} from {module_uri!r};
          const items = [
            {{id:'a', title:'Kantonale Gewerbesteuer', summary:'Hochrechnung Zürich', tags:['Journey'], path:'Customer'}},
            {{id:'b', title:'Journey', summary:'Unrelated', tags:[], path:'Other'}}
          ];
          const results = searchNotebookIndex(items, 'gewerbe zurich', 8);
          if (results.length !== 1 || results[0].id !== 'a') process.exit(2);
          if (searchNotebookIndex([...items, ...items, ...items, ...items, ...items], 'journey', 8).length > 8) process.exit(3);
          if (WORKBENCH_LIVE_RESULT_LIMIT !== 3 || workbenchSearchIsReady('a') || !workbenchSearchIsReady('ab')) process.exit(4);
          const source = normalizedDataSource({{source_id:'pg_olap', label:'PostgreSQL OLAP', classification:'Internal'}});
          const product = normalizedDataProduct({{productId:'p1', slug:'steuerzahlen', title:'Steuerzahlen', description:'DAAIF product'}});
          const mixed = searchWorkbenchIndex([source, product], 'st', 3);
          if (mixed.length !== 2 || mixed.some((item) => !['source', 'product'].includes(item.kind))) process.exit(5);
          const preview = searchWorkbenchPreview([...items, ...items, ...items], 'journey', 3);
          if (preview.totalCount !== 6 || preview.items.length !== 3) process.exit(6);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

    def test_home_search_expands_on_focus_without_result_layout_shift(self) -> None:
        template = (
            BDW_ROOT / "bit_data_workbench/templates/partials/home.html"
        ).read_text(encoding="utf-8")
        search = (STATIC_ROOT / "js/home-notebook-search.js").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")

        self.assertIn('class="home-journey-hero"', template)
        self.assertIn('id="home-journey-title"', template)
        self.assertIn("data-home-hero-picture", template)
        self.assertIn('type="image/avif"', template)
        self.assertIn('type="image/webp"', template)
        self.assertIn("data-home-notebook-search-form", template)
        self.assertIn('href="/query-workbench"', template)
        self.assertIn('href="/ingestion-workbench"', template)
        self.assertIn('aria-expanded="false"', template)
        self.assertIn('role="combobox"', template)
        self.assertIn('form.addEventListener("focusin"', search)
        self.assertIn('form.addEventListener("focusout"', search)
        self.assertIn('form.classList.toggle("is-expanded", expanded)', search)
        self.assertIn(".home-notebook-search.is-expanded", styles)
        self.assertIn("height: 540px", styles)
        self.assertIn("height: 218px", styles)
        self.assertIn("WORKBENCH_LIVE_RESULT_LIMIT = 3", search)
        self.assertIn('"swiss-aarau-old-town-summer"', search)
        self.assertIn('"swiss-neuchatel-castle-lake"', search)
        self.assertIn("/search?q=", search)
        self.assertIn("/api/workbench/source-options", search)
        self.assertIn("/api/data-products", search)

    def test_expert_search_covers_notebooks_sources_and_daaif_products(self) -> None:
        router = (BDW_ROOT / "bit_data_workbench/web/router.py").read_text(encoding="utf-8")
        template = (
            BDW_ROOT / "bit_data_workbench/templates/partials/expert_search.html"
        ).read_text(encoding="utf-8")
        search = (STATIC_ROOT / "js/expert-search.js").read_text(encoding="utf-8")

        self.assertIn('@router.get("/search"', router)
        self.assertIn("Notebooks, Datenquellen und in DAAIF erstellte Datenprodukte", template)
        self.assertIn('value="notebook"', template)
        self.assertIn('value="source"', template)
        self.assertIn('value="product"', template)
        self.assertIn("loadWorkbenchSearchIndex", search)
        self.assertIn("searchWorkbenchIndex", search)

    def test_navigation_epoch_aborts_stale_request_and_clocks_are_one_hz(self) -> None:
        epoch = (STATIC_ROOT / "js/workspace-navigation-epoch.js").read_text(encoding="utf-8")
        app = (STATIC_ROOT / "js/app.js").read_text(encoding="utf-8")
        clock = (STATIC_ROOT / "js/visibility-clock.js").read_text(encoding="utf-8")
        realtime = (STATIC_ROOT / "js/realtime-controller.js").read_text(encoding="utf-8")
        self.assertIn("controller?.abort()", epoch)
        self.assertIn("workspaceNavigationIsCurrent(token)", app)
        self.assertIn("startupNavigationEpoch", app)
        self.assertNotIn("initialWorkspaceMode", app)
        self.assertIn("UI_CLOCK_INTERVAL_MS = 1000", clock)
        self.assertIn('document.visibilityState === "hidden"', clock)
        self.assertNotIn("setInterval(refreshLiveQueryClock, 100)", realtime)
        self.assertNotIn("setInterval(refreshLivePythonClock, 100)", app)
        module_uri = (STATIC_ROOT / "js/workspace-navigation-epoch.js").resolve().as_uri()
        script = f"""
          globalThis.window = {{ location: {{ pathname: '/ingestion-workbench' }} }};
          const {{ createWorkspaceNavigationEpoch }} = await import({module_uri!r});
          const nav = createWorkspaceNavigationEpoch();
          const ingestion = nav.begin({{ path: '/ingestion-workbench' }});
          const notebook = nav.begin({{ path: '/notebooks/journey', notebookId: 'journey' }});
          if (!ingestion.signal.aborted || nav.isCurrent(ingestion) || !nav.isCurrent(notebook)) process.exit(4);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

    def test_ingestion_popstate_restores_transient_form_state(self) -> None:
        app = (STATIC_ROOT / "js/app.js").read_text(encoding="utf-8")
        navigation_state = (
            STATIC_ROOT / "js/ingestion-workbench-navigation-state.js"
        ).read_text(encoding="utf-8")
        self.assertIn("captureIngestionWorkbenchNavigationState", app)
        self.assertIn("restoreIngestionWorkbenchNavigationState", app)
        self.assertIn("filesByEntry", navigation_state)
        self.assertIn('input.dispatchEvent(new Event("change", { bubbles: true }))', navigation_state)
        self.assertIn("focus({ preventScroll: true })", navigation_state)
        self.assertIn("window.scrollTo", navigation_state)

    def test_direct_ingestion_and_deferred_loader_sidebar_payloads(self) -> None:
        service = UiService()
        ingestion = ingestion_workbench_partial(
            request=request("/ingestion-workbench"), service=service
        ).body
        loader_sidebar = sidebar_partial(
            request=request("/sidebar"),
            active_notebook_id=None,
            mode="loader",
            source_tree="deferred",
            notebook_tree="deferred",
            runbook_tree="deferred",
            service=service,
        ).body
        self.assertLess(len(ingestion), 1_544_000)  # at least 80% below 7.72 MB
        self.assertLessEqual(len(loader_sidebar), 25_000)
        self.assertNotIn(b"heavy-source", ingestion)
        self.assertNotIn(b'"heavy"', ingestion)
        self.assertIn(b"data-deferred-source-tree", loader_sidebar)

    def test_loader_manual_csv_instruction_contract(self) -> None:
        app = (STATIC_ROOT / "js/app.js").read_text(encoding="utf-8")
        controller = (STATIC_ROOT / "js/ingestion-controller.js").read_text(
            encoding="utf-8"
        )
        loader_ui = (STATIC_ROOT / "js/ingestion-ui.js").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")
        self.assertIn("downloadableFiles", app)
        self.assertIn("data-loader-downloadable-file", loader_ui)
        self.assertIn("CSV manuell einlesen", loader_ui)
        self.assertIn("data-open-ingestion-workbench", loader_ui)
        self.assertIn("Ein erneuter Upload ersetzt nur diese manuelle Datei.", loader_ui)
        self.assertIn("data-copy-loader-target-path", loader_ui)
        self.assertIn("Copied to Clipbard", loader_ui)
        self.assertIn('role="status"', loader_ui)
        self.assertIn('aria-live="polite"', loader_ui)
        self.assertIn("writeTextToClipboard", controller)
        self.assertIn("handleIngestionClick", app)
        self.assertIn(".ingestion-manual-copy-feedback", styles)
        self.assertIn("bottom: calc(100% + 0.45rem)", styles)

    def test_csv_s3_review_previews_the_complete_uri(self) -> None:
        controller = (
            STATIC_ROOT / "js/ingestion-types/csv/controller.js"
        ).read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")

        self.assertIn("Full S3 URI", controller)
        self.assertIn("data-csv-s3-summary-uri", controller)
        self.assertIn("reviewS3Location", controller)
        self.assertIn("Multiple extracted objects; exact S3 URIs are shown after import.", controller)
        self.assertIn('objectKey: `${objectKey}/**/*.parquet`', controller)
        self.assertIn("uri: item.path", controller)
        self.assertIn("S3 dataset URI", controller)
        self.assertIn(".ingestion-csv-s3-summary-row.is-uri-preview", styles)
        self.assertIn("user-select: all", styles)

    def test_csv_s3_uri_paste_splits_the_complete_location(self) -> None:
        controller = (
            STATIC_ROOT / "js/ingestion-types/csv/controller.js"
        ).read_text(encoding="utf-8")
        template = (
            BDW_ROOT / "bit_data_workbench/templates/partials/ingestion_workbench.html"
        ).read_text(encoding="utf-8")
        app = (STATIC_ROOT / "js/app.js").read_text(encoding="utf-8")
        navigation_state = (
            STATIC_ROOT / "js/ingestion-workbench-navigation-state.js"
        ).read_text(encoding="utf-8")
        source_inspector = (
            STATIC_ROOT / "js/source-inspector-controller.js"
        ).read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")
        module_uri = (
            STATIC_ROOT / "js/ingestion-types/csv/s3-location.js"
        ).resolve().as_uri()
        file_names_uri = (
            STATIC_ROOT / "js/ingestion-types/csv/file-names.js"
        ).resolve().as_uri()
        script = f"""
          import assert from 'node:assert/strict';
          const {{ parseCsvS3LocationInput }} = await import({module_uri!r});
          const {{ normalizeCsvImportBaseName, resolveCsvDestinationFileName }} = await import({file_names_uri!r});
          assert.deepEqual(
            parseCsvS3LocationInput('s3://data-analysts-journey/manual/aargau/gewerbesteuer_aargau_2022_2026.csv'),
            {{
              bucket: 'data-analysts-journey',
              keyPrefix: 'manual/aargau',
              objectName: 'gewerbesteuer_aargau_2022_2026.csv',
              objectKey: 'manual/aargau/gewerbesteuer_aargau_2022_2026.csv',
              storageFormat: 'csv',
            }}
          );
          assert.equal(parseCsvS3LocationInput('s3://demo-bucket/file.parquet').keyPrefix, '');
          assert.equal(parseCsvS3LocationInput('demo-bucket/manual/aargau').objectName, '');
          for (const invalid of ['demo-bucket', 's3://demo-bucket/', 's3:///file.csv', 'https://demo-bucket/file.csv', 's3://demo-bucket/file.csv?x=1', 's3://demo-bucket/file%2Fname.csv']) {{
            assert.equal(parseCsvS3LocationInput(invalid), null);
          }}
          assert.equal(normalizeCsvImportBaseName('tax.v2.csv'), 'tax.v2');
          assert.equal(normalizeCsvImportBaseName(normalizeCsvImportBaseName('tax.v2.csv')), 'tax.v2');
          assert.equal(
            resolveCsvDestinationFileName('tax.v2', {{ targetId: 's3', storageFormat: 'parquet' }}),
            'tax.v2.parquet'
          );
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

        self.assertIn("handleCsvIngestionPaste", controller)
        self.assertIn("applyPastedS3Location", controller)
        self.assertIn("S3 URI split into Bucket, Object key prefix, and Object name.", controller)
        self.assertIn("CSV import partially completed", controller)
        self.assertIn('s3UriPasteFeedback.tone !== "error"', controller)
        self.assertIn("const pendingTarget = pendingPastedS3Target", controller)
        self.assertIn("data-csv-s3-uri-paste-status", template)
        self.assertIn("handleCsvIngestionPaste(event)", app)
        self.assertIn("syncRestoredCsvObjectNames", navigation_state)
        self.assertIn('root.querySelectorAll("[data-csv-import-base-name]")', navigation_state)
        self.assertIn("revealSelectedSourceObject(sourceObjectRoot)", source_inspector)
        self.assertIn("[data-source-s3-folder]", source_inspector)
        self.assertIn(".ingestion-csv-s3-uri-paste-status.is-success", styles)
        self.assertIn('[data-csv-s3-bucket][aria-invalid="true"]', styles)

    def test_publish_wizard_requires_explicit_duplicate_slug_overwrite(self) -> None:
        controller = (STATIC_ROOT / "js/data-products-controller.js").read_text(
            encoding="utf-8"
        )
        ui = (STATIC_ROOT / "js/data-products-ui.js").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")

        self.assertIn("data-data-product-overwrite-panel", ui)
        self.assertIn("data-data-product-overwrite-confirm", controller)
        self.assertIn('role="alert" aria-live="polite"', controller)
        self.assertIn("This slug is already published", controller)
        self.assertIn("Existing title", controller)
        self.assertIn("Current source", controller)
        self.assertIn("Last updated", controller)
        self.assertIn("overwriteExisting", controller)
        self.assertIn("expectedProductId", controller)
        self.assertIn("expectedUpdatedAt", controller)
        self.assertIn("Replace existing data product", controller)
        self.assertIn("publicationState.overwriteConfirmed = false", controller)
        self.assertIn("invalidatePublicationPreview()", controller)
        self.assertIn("publishToDacaInput.disabled = dacaManagedExisting", controller)
        self.assertIn("Data product replaced", controller)
        slug_handler = controller.split(
            'if (event.target.matches("[data-data-product-slug-input]")) {', 1
        )[1].split(
            'if (event.target.matches("[data-data-product-description-input]")) {', 1
        )[0]
        source_handler = controller.split(
            'if (event.target.matches("[data-data-product-source-select]")) {', 1
        )[1].split(
            'if (event.target.matches("[data-data-product-access-level-input]")) {', 1
        )[0]
        self.assertIn("invalidatePublicationPreview()", slug_handler)
        self.assertIn("invalidatePublicationPreview()", source_handler)
        self.assertIn(".data-product-overwrite-conflict", styles)
        self.assertIn(".data-product-overwrite-confirmation", styles)
        self.assertIn(".data-product-daca-publication-option.is-locked", styles)

    def test_versioned_static_assets_receive_immutable_cache_header(self) -> None:
        static_app = FastAPI()
        static_app.mount(
            "/static",
            VersionedStaticFiles(directory=STATIC_ROOT),
            name="static",
        )
        with TestClient(static_app) as client:
            versioned = client.get("/static/js/demo-identity.js?v=test-build")
            unversioned = client.get("/static/js/demo-identity.js")

        self.assertEqual(versioned.status_code, 200)
        self.assertEqual(
            versioned.headers["cache-control"],
            "public, max-age=31536000, immutable",
        )
        self.assertEqual(
            unversioned.headers["cache-control"],
            "public, max-age=3600",
        )


if __name__ == "__main__":
    unittest.main()
