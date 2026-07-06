from __future__ import annotations

from pathlib import Path
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.version_info import current_repo_version  # noqa: E402
from fastapi import HTTPException  # noqa: E402

from bit_data_workbench.web.data_platform import (  # noqa: E402
    data_platform_page,
    data_platform_topic_page,
)
from bit_data_workbench.web.data_platform_content import (  # noqa: E402
    CONTENT_PATH,
    DATA_PLATFORM_TOPIC_USE_CASES,
    DATA_PLATFORM_TOPICS,
)
from bit_data_workbench.web.router import index  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def build_request(path: str, *, partial: bool = False) -> Request:
    headers = [(b"host", b"testserver")]
    if partial:
        headers.append((b"hx-request", b"true"))
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "method": "GET",
            "path": path,
            "headers": headers,
            "server": ("testserver", 80),
        }
    )


class FakeWorkbenchService:
    def __init__(self) -> None:
        self.settings = type(
            "Settings",
            (),
            {
                "pg_host": None,
                "pg_port": None,
                "pg_user": None,
                "pg_password": None,
                "pg_oltp_database": None,
                "pg_olap_database": None,
                "s3_endpoint": None,
                "s3_bucket": None,
                "s3_url_style": "path",
                "s3_use_ssl": False,
                "s3_verify_ssl": False,
                "current_s3_access_key_id": lambda self: None,
                "current_s3_secret_access_key": lambda self: None,
                "effective_s3_ca_cert_file": lambda self: None,
            },
        )()

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": CURRENT_VERSION,
            "hostname": "test-host",
            "pod_name": "bdw-pod",
            "pod_namespace": "bdw-namespace",
            "pod_ip": "127.0.0.1",
            "node_name": "bdw-node",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-06-30T00:00:00+00:00",
        }

    def catalogs(self):
        return []

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def source_options(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []

    def completion_schema(self):
        return {}


class DataPlatformRouteTests(unittest.TestCase):
    def test_data_platform_page_renders_standalone_admin_header(self) -> None:
        response = data_platform_page(
            request=build_request("/the-data-platform"),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("<title>DAAIF Factory - Data Platform BIT</title>", body)
        self.assertIn('class="data-platform-document"', body)
        self.assertIn("The federal authorities of the Swiss Confederation", body)
        self.assertIn("/static/img/swiss-confederation-logo.png", body)
        self.assertIn("Bundesamt für Informatik", body)
        self.assertNotIn("Der Bundesrat", body)
        self.assertIn('role="search"', body)
        self.assertIn(">Medien</a>", body)
        self.assertIn(">Services</a>", body)
        self.assertIn(">Kontakt</a>", body)
        self.assertNotIn('class="shell', body)

    def test_data_platform_page_renders_core_platform_map(self) -> None:
        response = data_platform_page(
            request=build_request("/the-data-platform"),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Data Platform BIT oversight", body)
        self.assertNotIn("BIT Data Platform oversight", body)
        self.assertIn("Secure, governed Data Products", body)
        self.assertNotIn("data-platform-product-actions", body)
        self.assertNotIn("Data product workflow", body)
        self.assertNotIn(">Find</a>", body)
        self.assertNotIn(">Request</a>", body)
        self.assertNotIn(">Analyse</a>", body)
        self.assertNotIn(">Publish</a>", body)
        self.assertIn("The federal Data Plattform", body)
        self.assertNotIn("Catalogue-centric data oversight", body)
        self.assertIn("Data Governance", body)
        self.assertIn("Data Security", body)
        self.assertIn("Data Management", body)
        self.assertIn("Data Processing", body)
        self.assertIn("Data Storage", body)
        self.assertIn("Data Catalogue SaaS", body)
        self.assertIn("Data Owner", body)
        self.assertIn("PBAC", body)
        self.assertIn("PAP", body)
        self.assertIn("S3 / Object", body)
        self.assertIn("RDBMS", body)
        self.assertIn("DRILL DOWN ->", body)
        self.assertIn('href="/the-data-platform/data-management"', body)
        self.assertIn('href="/the-data-platform/data-security"', body)
        self.assertIn('href="/the-data-platform/data-products"', body)
        self.assertIn("storage policy", body)
        self.assertIn("stored products", body)
        self.assertIn('class="data-platform-flow-map"', body)
        self.assertIn("protect data products", body)
        self.assertIn("audit data manipulation", body)
        self.assertIn("request access decision", body)
        self.assertIn("provide policy requirements", body)
        self.assertIn("authorize product access", body)
        self.assertIn("serve product states", body)
        self.assertIn('class="data-platform-usecase-flow data-platform-flow-security-management"', body)
        self.assertIn('class="data-platform-usecase-flow data-platform-flow-management-governance"', body)
        self.assertIn('data-source-topic="data-security"', body)
        self.assertIn('data-target-topic="data-management"', body)
        self.assertIn('data-flow-duration="4.2s"', body)
        self.assertIn("data-flow-label", body)
        self.assertIn('preserveAspectRatio="none"', body)
        self.assertNotIn("<animateMotion", body)
        self.assertNotIn('rotate="0"', body)
        self.assertNotIn('dy="-16"', body)
        self.assertNotIn("<mpath", body)
        self.assertNotIn("<textPath", body)
        self.assertNotIn('attributeName="startOffset"', body)
        self.assertNotIn("data-platform-flow-arrow", body)
        self.assertNotIn("<marker", body)
        script = (REPO_ROOT / "bdw/bit_data_workbench/static/js/data-platform-topic.js").read_text()
        self.assertIn("FLOW_LABEL_GAP_FROM_LINE = 16", script)
        self.assertIn("FLOW_LABEL_STAGGER = 0.14", script)
        self.assertIn("normalizeLabelAngle", script)
        self.assertIn("requestAnimationFrame(animateTopicUseCases)", script)
        self.assertIn("rotate(\" +", script)
        css = (REPO_ROOT / "bdw/bit_data_workbench/static/css/app.css").read_text()
        self.assertIn("--data-platform-flow-stroke: rgba(224, 0, 26, 0.56)", css)
        self.assertIn("stroke-linecap: butt", css)
        self.assertIn("stroke: none", css)
        self.assertIn("opacity: 0", css)
        self.assertIn("font-size: 14px", css)
        self.assertIn("font-weight: 400", css)
        self.assertIn(".data-platform-bubble-link", css)
        self.assertIn("width: min(100%, 1540px)", css)
        self.assertIn("height: clamp(360px, min(60vw, calc(100vh - 320px)), 924px)", css)
        self.assertIn("--data-platform-bubble-size: clamp(120px, min(11vw, 17vh), 250px)", css)
        self.assertIn("--data-platform-product-bubble-size: clamp(150px, min(13vw, 20vh), 300px)", css)
        self.assertIn("--data-platform-bubble-size: var(--data-platform-product-bubble-size, 300px)", css)
        self.assertIn("aspect-ratio: auto", css)
        self.assertIn("(min-width: 1081px) and (max-height: 860px)", css)
        self.assertNotIn("min-height: 930px", css)
        self.assertIn("--data-platform-bubble-left: 12%", css)
        self.assertIn("--data-platform-bubble-left: 88%", css)
        self.assertIn("--data-platform-bubble-top: 44%", css)
        self.assertIn("--data-platform-bubble-top: 84%", css)
        self.assertNotIn("paint-order: stroke fill", css)
        self.assertIn("data-platform-flow-dash", css)
        self.assertIn(".data-platform-usecase-flow.is-visible", css)
        self.assertNotIn(".data-platform-product-actions", css)
        self.assertNotIn(".data-platform-flow-security-storage", css)
        self.assertNotIn("radial-gradient(circle at 72% 76%, rgba(224, 0, 26", css)
        self.assertNotIn("inset 0 -22px 44px rgba(224, 0, 26", css)
        self.assertNotIn("0 30px 60px rgba(224, 0, 26", css)
        self.assertNotIn("data-platform-card-processing:is(:hover, :focus, :focus-within)) .data-platform-flow-management-processing", css)
        self.assertNotIn("marker-end", css)
        self.assertNotIn("Back to DAAIF Factory", body)
        self.assertNotIn("data-platform-flow-line-horizontal", body)

    def test_data_platform_content_is_loaded_from_editable_json(self) -> None:
        self.assertEqual(
            CONTENT_PATH,
            REPO_ROOT / "bdw/bit_data_workbench/data/data_platform_content.json",
        )
        self.assertTrue(CONTENT_PATH.exists())
        self.assertGreaterEqual(len(DATA_PLATFORM_TOPICS), 6)
        self.assertEqual(
            len(DATA_PLATFORM_TOPIC_USE_CASES),
            len(DATA_PLATFORM_TOPICS) * (len(DATA_PLATFORM_TOPICS) - 1),
        )

        data_management = next(
            topic for topic in DATA_PLATFORM_TOPICS if topic.slug == "data-management"
        )
        self.assertIn(
            ("data-inventory", "catalog-service"),
            {(link.capability, link.service) for link in data_management.links},
        )
        self.assertIn(
            ("data-security", "data-management", "protect data products"),
            {
                (use_case.source, use_case.target, use_case.label)
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
            },
        )
        outgoing_counts = {
            topic.slug: sum(
                1
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
                if use_case.source == topic.slug
            )
            for topic in DATA_PLATFORM_TOPICS
        }
        self.assertEqual(
            outgoing_counts,
            {
                topic.slug: len(DATA_PLATFORM_TOPICS) - 1
                for topic in DATA_PLATFORM_TOPICS
            },
        )
        topic_slugs = {topic.slug for topic in DATA_PLATFORM_TOPICS}
        targets_by_source = {
            topic.slug: {
                use_case.target
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
                if use_case.source == topic.slug
            }
            for topic in DATA_PLATFORM_TOPICS
        }
        self.assertEqual(
            targets_by_source,
            {
                topic.slug: topic_slugs - {topic.slug}
                for topic in DATA_PLATFORM_TOPICS
            },
        )
        self.assertTrue(
            all(
                use_case.source != use_case.target
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
            )
        )
        self.assertIn(
            ("data-governance", "data-storage", "define retention obligations"),
            {
                (use_case.source, use_case.target, use_case.label)
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
            },
        )
        self.assertIn(
            (
                "data-processing",
                "data-products",
                "M 600 546 C 500 470 450 410 506 360 C 542 342 584 380 548 430",
            ),
            {
                (use_case.source, use_case.target, use_case.path)
                for use_case in DATA_PLATFORM_TOPIC_USE_CASES
            },
        )

    def test_data_platform_topic_page_renders_capabilities_services_and_links(self) -> None:
        response = data_platform_topic_page(
            request=build_request("/the-data-platform/data-management"),
            topic_slug="data-management",
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("<title>DAAIF Factory - Data Management - Data Platform BIT</title>", body)
        self.assertIn("Data Platform BIT", body)
        self.assertIn("Data Management", body)
        self.assertIn("Capabilities", body)
        self.assertIn("Technical Services", body)
        self.assertIn("Daten Inventorisierung", body)
        self.assertIn("Datenkatalog Service", body)
        self.assertIn("Data Quality Management", body)
        self.assertIn("Data Quality Service", body)
        self.assertIn('data-capability="data-inventory"', body)
        self.assertIn('data-service="catalog-service"', body)
        self.assertIn("/static/js/data-platform-topic.js", body)

        css = (REPO_ROOT / "bdw/bit_data_workbench/static/css/app.css").read_text()
        self.assertIn(".data-platform-topic-board", css)
        self.assertIn(".data-platform-topic-connection", css)
        self.assertIn("stroke-dasharray: 10 10", css)
        self.assertIn("animation: none", css)
        self.assertNotIn("marker-end", css)

    def test_data_platform_topic_page_returns_404_for_unknown_topic(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            data_platform_topic_page(
                request=build_request("/the-data-platform/not-a-topic"),
                topic_slug="not-a-topic",
            )

        self.assertEqual(exc.exception.status_code, 404)

    def test_home_page_links_to_data_platform(self) -> None:
        response = index(
            request=build_request("/", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('href="/the-data-platform"', body)
        self.assertIn("Data Platform BIT", body)
        self.assertIn("federal Data Plattform oversight map", body)

    def test_full_shell_renders_data_platform_topbar_link(self) -> None:
        response = index(
            request=build_request("/", partial=False),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('href="/the-data-platform"', body)
        self.assertIn("Open the Data Platform BIT oversight page.", body)
        self.assertIn("Data Platform BIT", body)


if __name__ == "__main__":
    unittest.main()
