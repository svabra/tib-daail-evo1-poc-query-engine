from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_target_helpers():
    from bit_data_workbench.data_generator.base import generation_target, update_generation_target_status
    from bit_data_workbench.models import DataGenerationJobDefinition, normalize_data_generation_targets

    return generation_target, update_generation_target_status, DataGenerationJobDefinition, normalize_data_generation_targets


class DataGenerationTargetTests(unittest.TestCase):
    def test_generation_target_status_updates_by_location(self) -> None:
        generation_target, update_generation_target_status, _, _ = import_target_helpers()
        targets = [
            generation_target(
                target_kind="postgres_table",
                label="OLTP table",
                location="pg_oltp.public.tax_assessment",
            ),
            generation_target(
                target_kind="s3_prefix",
                label="S3 prefix",
                location="s3://loader-bucket/generated/tax_assessment",
            ),
        ]

        updated = update_generation_target_status(
            targets,
            "s3://loader-bucket/generated/tax_assessment",
            status="written",
        )

        self.assertEqual(updated[0].status, "pending")
        self.assertEqual(updated[1].status, "written")

    def test_normalize_data_generation_targets_deduplicates_and_filters_invalid_entries(self) -> None:
        _, _, _, normalize_data_generation_targets = import_target_helpers()

        normalized = normalize_data_generation_targets(
            [
                {
                    "targetKind": "postgres_table",
                    "label": "OLTP table",
                    "location": "pg_oltp.public.tax_assessment",
                    "status": "written",
                },
                {
                    "targetKind": "postgres_table",
                    "label": "Duplicate should be ignored",
                    "location": "pg_oltp.public.tax_assessment",
                    "status": "pending",
                },
                {
                    "targetKind": "s3_prefix",
                    "location": "",
                },
            ]
        )

        self.assertEqual(
            [item.payload for item in normalized],
            [
                {
                    "targetKind": "postgres_table",
                    "label": "OLTP table",
                    "location": "pg_oltp.public.tax_assessment",
                    "status": "written",
                }
            ],
        )

    def test_job_payload_includes_written_targets(self) -> None:
        generation_target, _, job_type, _ = import_target_helpers()

        job = job_type(
            job_id="ingest-1",
            generator_id="loader-1",
            title="Loader",
            description="Writes demo data",
            target_kind="postgres",
            requested_size_gb=1.0,
            status="completed",
            started_at="2026-04-14T10:00:00+00:00",
            updated_at="2026-04-14T10:01:00+00:00",
            written_targets=[
                generation_target(
                    target_kind="postgres_table",
                    label="OLTP table",
                    location="pg_oltp.public.tax_assessment",
                    status="written",
                )
            ],
        )

        self.assertEqual(
            job.payload["writtenTargets"],
            [
                {
                    "targetKind": "postgres_table",
                    "label": "OLTP table",
                    "location": "pg_oltp.public.tax_assessment",
                    "status": "written",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
