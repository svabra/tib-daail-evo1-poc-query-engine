from __future__ import annotations

import json
from pathlib import Path
import subprocess
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_URI = (
    REPO_ROOT
    / "bdw/bit_data_workbench/static/js/data-exporters/result-storage-export-target.js"
).resolve().as_uri()


class ResultStorageExportTargetTests(TestCase):
    def test_completed_result_storage_prefills_the_canonical_s3_target(self) -> None:
        job = {
            "resultStorage": {
                "enabled": True,
                "status": "completed",
                "format": "parquet",
                "path": "s3://data-analysts-journey/products/kantonale-gewerbesteuer-soll-ist-2022-2026.parquet",
                "bucket": "data-analysts-journey",
                "key": "products/kantonale-gewerbesteuer-soll-ist-2022-2026.parquet",
            }
        }
        script = f"""
          import assert from 'node:assert/strict';
          const {{ resultStorageExportTarget }} = await import({MODULE_URI!r});
          const job = {json.dumps(job)};
          assert.deepEqual(resultStorageExportTarget(job), {{
            bucket: 'data-analysts-journey',
            key: 'products/kantonale-gewerbesteuer-soll-ist-2022-2026.parquet',
            prefix: 'products/',
            fileName: 'kantonale-gewerbesteuer-soll-ist-2022-2026.parquet',
            exportFormat: 'parquet',
            path: 's3://data-analysts-journey/products/kantonale-gewerbesteuer-soll-ist-2022-2026.parquet',
          }});
          assert.equal(resultStorageExportTarget({{ resultStorage: {{ enabled: false }} }}), null);
          assert.equal(resultStorageExportTarget({{ resultStorage: {{ path: 's3://bucket/folder/' }} }}), null);
        """

        completed = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_app_uses_configured_result_storage_before_generic_defaults(self) -> None:
        app_source = (
            REPO_ROOT / "bdw/bit_data_workbench/static/js/app.js"
        ).read_text(encoding="utf-8")

        self.assertIn("const configuredTarget = resultStorageExportTarget(job);", app_source)
        self.assertIn("configuredTarget?.fileName", app_source)
        self.assertIn("resultExportDialogState.selectedBucket = configuredTarget.bucket", app_source)
        self.assertIn("resultExportDialogState.selectedPrefix = configuredTarget.prefix", app_source)
        self.assertIn("already materialized at", app_source)

    def test_journey_publication_uses_the_completed_storage_target(self) -> None:
        app_source = (
            REPO_ROOT / "bdw/bit_data_workbench/static/js/app.js"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "const job = queryJobForResultActionTarget(publishJourneyDataProductTrigger);",
            app_source,
        )
        self.assertIn("bucket: configuredTarget.bucket", app_source)
        self.assertIn("key: configuredTarget.key", app_source)
        self.assertNotIn(
            "data_analysts_journey_6f15a669.kantonale_gewerbesteuer_soll_ist_2022_2026",
            app_source,
        )
