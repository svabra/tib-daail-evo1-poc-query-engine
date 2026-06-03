from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import completion_schema  # noqa: E402


class FakeWorkbenchService:
    def completion_schema(self) -> dict[str, object]:
        return {
            "s3": {
                "vat_smoke_test": {
                    "sample_tax": {
                        "csv": [],
                    },
                },
            },
        }


class CompletionSchemaApiTests(unittest.TestCase):
    def test_completion_schema_endpoint_returns_service_schema(self) -> None:
        response = completion_schema(service=FakeWorkbenchService())  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json.loads(response.body.decode("utf-8")),
            {
                "s3": {
                    "vat_smoke_test": {
                        "sample_tax": {
                            "csv": [],
                        },
                    },
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
