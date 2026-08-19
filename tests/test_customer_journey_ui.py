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

from bit_data_workbench.api.workbench_metadata import (
    feature_release_notes,
    notebook_search_index,
)
from bit_data_workbench.backend.notebook_search import notebook_search_items
from bit_data_workbench.backend.runbooks import build_runbook_tree
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry
from bit_data_workbench.backend.static_assets import VersionedStaticFiles
from bit_data_workbench.models import NotebookDefinition
from bit_data_workbench.release_notes import release_notes
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
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")
        identity = (STATIC_ROOT / "js/demo-identity.js").read_text(encoding="utf-8")
        self.assertIn("federal-authority-strip", header)
        self.assertIn("data-demo-user-select", header)
        self.assertIn(
            ".federal-workbench-nav .topbar-actions-primary {\n"
            "  display: flex;\n"
            "  align-items: stretch;\n"
            "  justify-content: flex-start;\n"
            "  gap: 0;\n"
            "  min-height: 64px;\n"
            "  overflow: visible;\n"
            "}",
            styles,
        )
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

    def test_version_overlay_opens_the_current_daca_aligned_feature_list(self) -> None:
        template = (
            BDW_ROOT / "bit_data_workbench/templates/index.html"
        ).read_text(encoding="utf-8")
        settings = (
            BDW_ROOT / "bit_data_workbench/templates/partials/federal_header.html"
        ).read_text(encoding="utf-8")
        dialogs = (STATIC_ROOT / "js/dialogs.js").read_text(encoding="utf-8")
        controller = (STATIC_ROOT / "js/feature-list-controller.js").read_text(
            encoding="utf-8"
        )
        styles = (STATIC_ROOT / "css/app.css").read_text(encoding="utf-8")

        self.assertIn('class="app-version-feature-trigger"', template)
        self.assertIn('aria-controls="daaif-feature-list-dialog"', template)
        self.assertIn('aria-haspopup="dialog"', template)
        self.assertIn("Featureliste anzeigen", template)
        self.assertIn('aria-controls="daaif-feature-list-dialog"', settings)
        self.assertIn('id="daaif-feature-list-dialog"', dialogs)
        self.assertIn('aria-labelledby="daaif-feature-list-title"', dialogs)
        self.assertIn('aria-describedby="daaif-feature-list-introduction"', dialogs)
        self.assertIn("data-feature-list-close", dialogs)
        self.assertIn("feature-list-dialog-note", dialogs)
        self.assertIn("syncTriggerExpanded(true)", controller)
        self.assertIn("renderLoading(dialog)", controller)
        self.assertLess(
            controller.index("renderLoading(dialog)"),
            controller.index("ensureReleaseNotes()"),
        )
        self.assertIn("data-feature-list-loading", controller)
        self.assertIn("Die Featureliste konnte nicht geladen werden", controller)
        self.assertIn("data-feature-list-version", controller)
        self.assertIn("feature-list-current-badge", controller)
        self.assertIn('dialog.addEventListener("cancel", onCancel)', controller)
        self.assertIn('event.target === dialog', controller)
        self.assertIn("focusableReturnTarget(returnTarget)?.focus()", controller)
        self.assertIn("pointer-events: auto", styles)
        self.assertIn("width: min(680px, calc(100vw - 32px))", styles)
        self.assertIn("max-height: calc(100dvh - 16px)", styles)

        notes = release_notes()
        current = notes[0]
        feature_list = current["featureList"]
        self.assertEqual(feature_list["title"], "Was kann DAAIF Factory?")
        self.assertGreaterEqual(len(feature_list["features"]), 5)
        self.assertTrue(
            all(
                item["title"] and item["description"]
                for item in feature_list["features"]
            )
        )
        history = feature_list["releases"]
        self.assertGreaterEqual(len(history), 50)
        versions = [release["version"] for release in history]
        self.assertEqual(versions[0], current["version"])
        self.assertEqual(len(versions), len(set(versions)))
        semantic_versions = [
            tuple(int(part) for part in version.split(".")) for version in versions
        ]
        self.assertEqual(semantic_versions, sorted(semantic_versions, reverse=True))
        self.assertTrue(all(release["releasedAt"] for release in history))
        self.assertGreaterEqual(
            sum(len(release["features"]) for release in history),
            80,
        )
        self.assertTrue(
            all(
                feature["title"] and feature["description"]
                for release in history
                for feature in release["features"]
            )
        )
        self.assertTrue(
            {
                current["version"],
                "0.10.42",
                "0.10.40",
                "0.9.0",
                "0.7.0",
                "0.6.0",
                "0.5.2",
                "0.3.24",
            }.issubset(versions)
        )
        visible_copy = " ".join(
            feature["title"] + " " + feature["description"]
            for release in history
            for feature in release["features"]
        )
        for internal_fragment in (
            "Regression coverage",
            "RHOS deployment",
            "worker PID",
            "WAL lock",
            "ASGI",
        ):
            self.assertNotIn(internal_fragment, visible_copy)

        response = feature_release_notes(if_none_match=None)
        payload = loads(response.body)
        self.assertEqual(payload[0]["version"], current["version"])
        self.assertEqual(payload[0]["featureList"], feature_list)
        replay = feature_release_notes(if_none_match=response.headers["etag"])
        self.assertEqual(replay.status_code, 304)

        module_uri = (STATIC_ROOT / "js/feature-list-controller.js").resolve().as_uri()
        script = f"""
          import {{ featureReleaseHistory }} from {module_uri!r};
          const history = featureReleaseHistory([
            {{version:'2.0.0', releasedAt:'2026-08-18', features:[{{title:'Neu', description:'Sichtbar'}}]}},
            {{version:'1.0.0', releasedAt:'2026-01-01', features:['Regression coverage now verifies internals.', 'Wichtig']}}
          ]);
          if (history.length !== 2) process.exit(2);
          if (history[0].features[0].title !== 'Neu') process.exit(3);
          if (history[1].features.length !== 1 || history[1].features[0].description !== 'Wichtig') process.exit(4);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

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
          const documentedProduct = normalizedDataProduct({{
            productId:'p2',
            slug:'kantonale-steuern',
            title:'Kantonale Steuern',
            documentationPath:'/dataproducts/custom-kantonale-steuern',
          }});
          if (product.targetUrl !== '/dataproducts/steuerzahlen') process.exit(7);
          if (documentedProduct.targetUrl !== '/dataproducts/custom-kantonale-steuern') process.exit(8);
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
        result_styles = styles.split(".home-notebook-search-results {", 1)[1].split("}", 1)[0]
        expanded_feedback_styles = styles.split(
            ".home-notebook-search.is-expanded .home-notebook-search-feedback {", 1
        )[1].split("}", 1)[0]

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
        self.assertIn("data-home-notebook-search-all", template)
        self.assertIn("data-home-notebook-search-expert", template)
        self.assertIn('href="/search"', template)
        self.assertIn('form.addEventListener("focusin"', search)
        self.assertIn('form.addEventListener("focusout"', search)
        self.assertIn('form.classList.toggle("is-expanded", expanded)', search)
        self.assertIn("syncActiveResultState", search)
        self.assertIn("syncAllResultsLink", search)
        self.assertIn("syncExpertSearchLink", search)
        self.assertIn('event.target.closest("[data-home-notebook-search-expert]")', search)
        self.assertIn("Alle Inhalte · Erweiterte Filter", template)
        self.assertIn("Expertensuche öffnen", template)
        self.assertNotIn('form.querySelector("[data-home-notebook-search-all]")?.remove()', search)
        self.assertNotIn("activeIndex = index;\n      render();", search)
        self.assertIn(".home-notebook-search.is-expanded", styles)
        self.assertIn(".home-notebook-search-all[hidden]", styles)
        self.assertIn(".home-notebook-search:hover .home-notebook-search-expert", styles)
        self.assertIn(".home-notebook-search:focus-within .home-notebook-search-expert", styles)
        self.assertIn(
            "radial-gradient(circle at 86% 16%, rgba(11, 68, 121, 0.07), transparent 32%)",
            styles,
        )
        self.assertNotIn(
            "radial-gradient(circle at top right, rgba(213, 43, 30, 0.08), transparent 32%)",
            styles,
        )
        self.assertIn("height: 552px", styles)
        self.assertIn("height: 318px", expanded_feedback_styles)
        self.assertIn("padding-right: 0", expanded_feedback_styles)
        self.assertNotIn("max-height", result_styles)
        self.assertNotIn("overflow-y", result_styles)
        self.assertIn("WORKBENCH_LIVE_RESULT_LIMIT = 3", search)
        self.assertIn('"swiss-aarau-old-town-summer"', search)
        self.assertIn('"swiss-neuchatel-castle-lake"', search)
        self.assertIn("/search?q=", search)
        self.assertIn("/api/workbench/catalog-search-index", search)
        self.assertIn("/api/data-products", search)

    def test_expert_search_covers_notebooks_sources_and_daaif_products(self) -> None:
        router = (BDW_ROOT / "bit_data_workbench/web/router.py").read_text(encoding="utf-8")
        template = (
            BDW_ROOT / "bit_data_workbench/templates/partials/expert_search.html"
        ).read_text(encoding="utf-8")
        search = (STATIC_ROOT / "js/expert-search.js").read_text(encoding="utf-8")

        self.assertIn('@router.get("/search"', router)
        self.assertIn("Notebooks, Data Sources, Datenobjekte", template)
        self.assertIn('value="notebook"', template)
        self.assertIn('<option value="source">Data Sources</option>', template)
        self.assertIn('<option value="object">Datenobjekte</option>', template)
        self.assertIn('value="product"', template)
        self.assertIn("Ohne Suchbegriff werden alle Inhalte", template)
        self.assertIn("Die Filterung beginnt mit dem ersten Zeichen", template)
        self.assertIn("loadWorkbenchSearchIndex", search)
        self.assertIn("searchExpertWorkbenchIndex", search)
        self.assertIn("data-workbench-expert-search-result-kind", search)

        module_uri = (STATIC_ROOT / "js/expert-search-filter.js").resolve().as_uri()
        script = f"""
          import {{ expertSearchKindFromParams, searchExpertWorkbenchIndex }} from {module_uri!r};
          const items = [
            {{id:'n1', kind:'notebook', title:'Zürcher Analyse', summary:'', tags:[], path:'PoC'}},
            {{id:'s1', kind:'source', title:'PostgreSQL OLAP', summary:'Zürich', tags:[], path:'pg'}},
            {{id:'s2', kind:'source', title:'S3 Object Storage', summary:'', tags:[], path:'s3'}},
            {{id:'o1', kind:'object', title:'orders.parquet', summary:'', tags:['S3'], path:'s3://data/orders.parquet'}},
            {{id:'p1', kind:'product', title:'Steuerzahlen', summary:'', tags:[], path:'DAAIF'}},
          ];
          const sources = searchExpertWorkbenchIndex(items, '', 'source');
          if (sources.length !== 2 || sources.some((item) => item.kind !== 'source')) process.exit(2);
          const products = searchExpertWorkbenchIndex(items, '', 'product');
          if (products.length !== 1 || products[0].id !== 'p1') process.exit(3);
          const objects = searchExpertWorkbenchIndex(items, '', 'object');
          if (objects.length !== 1 || objects[0].id !== 'o1') process.exit(9);
          const notebooks = searchExpertWorkbenchIndex(items, '', 'notebook');
          if (notebooks.length !== 1 || notebooks[0].id !== 'n1') process.exit(4);
          const firstCharacter = searchExpertWorkbenchIndex(items, 'b', 'all');
          if (firstCharacter.length !== 1 || firstCharacter[0].id !== 's2') process.exit(5);
          const cleared = searchExpertWorkbenchIndex(items, '   ', 'all');
          if (cleared.length !== items.length) process.exit(6);
          if (expertSearchKindFromParams('?kind=source') !== 'source') process.exit(7);
          if (expertSearchKindFromParams('?kind=unknown') !== 'all') process.exit(8);
          if (expertSearchKindFromParams('?kind=object') !== 'object') process.exit(10);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

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
        journey_generator = (
            BDW_ROOT
            / "bit_data_workbench/data_generator/data_analysts_journey.py"
        ).read_text(encoding="utf-8")
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
        self.assertIn("data-loader-required-storage-format", loader_ui)
        self.assertIn("Erforderliches Speicherformat:", loader_ui)
        self.assertIn("Nicht in Parquet", journey_generator)
        self.assertIn('"storageFormat": "csv"', journey_generator)
        self.assertIn("storageFormatInstruction", app)
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
        source_helper = (STATIC_ROOT / "js/data-products-source.js").read_text(
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
        self.assertIn("function publicationSourceDescriptor()", controller)
        self.assertIn("dataProductSourceForPublication", controller)
        self.assertIn('source.sourceKind === "object"', source_helper)
        self.assertIn('sourceKind: "relation"', source_helper)
        self.assertIn("typed relation", controller)
        self.assertIn("Data product replaced", controller)
        self.assertIn("Open data product in DaCa as Joel Ruod", controller)
        module_uri = (STATIC_ROOT / "js/data-products-source.js").resolve().as_uri()
        script = f"""
          import {{ dataProductSourceForPublication }} from {module_uri!r};
          const source = {{
            sourceKind: 'object', sourceId: 's3', bucket: 'bfs',
            key: 'pipeline-demo.parquet'
          }};
          const managed = dataProductSourceForPublication(source, {{ publishToDaca: true }});
          if (managed.sourceKind !== 'relation' || managed.relation !== '') process.exit(2);
          if (managed.bucket !== 'bfs' || managed.key !== 'pipeline-demo.parquet') process.exit(3);
          if (source.sourceKind !== 'object' || 'relation' in source) process.exit(4);
          if (dataProductSourceForPublication(source).sourceKind !== 'object') process.exit(5);
          const local = {{ sourceKind: 'local-object', sourceId: 'workspace.local' }};
          if (dataProductSourceForPublication(local, {{ publishToDaca: true }}) !== local) process.exit(6);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )
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

    def test_static_asset_cache_header_can_be_overridden_for_development(self) -> None:
        static_app = FastAPI()
        static_app.mount(
            "/static",
            VersionedStaticFiles(
                directory=STATIC_ROOT,
                cache_control_override="no-store",
            ),
            name="static",
        )
        with TestClient(static_app) as client:
            versioned = client.get("/static/js/demo-identity.js?v=test-build")
            unversioned = client.get("/static/js/demo-identity.js")

        self.assertEqual(versioned.status_code, 200)
        self.assertEqual(versioned.headers["cache-control"], "no-store")
        self.assertEqual(unversioned.headers["cache-control"], "no-store")


if __name__ == "__main__":
    unittest.main()
