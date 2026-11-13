from ..data_generator.mwst_journey import BUCKET, COMPANY_KEY, GENERATOR_ID, NOTEBOOK_ID, RESULT_KEY
from ..models import NotebookCellDefinition, NotebookDefinition
from .source_references import s3_source_reference


def build_mwst_journey_notebook() -> NotebookDefinition:
    source = s3_source_reference(bucket=BUCKET, key=COMPANY_KEY)
    return NotebookDefinition(
        notebook_id=NOTEBOOK_ID,
        title="MWST-Journey – SG-Produkt selbst erstellen",
        summary=(
            "Als Kassandra Valdata: Firmenprodukt auf 2026 und Kanton SG filtern, 60 Firmenzeilen "
            "prüfen, Ergebnis als Parquet speichern und als eigenes Datenprodukt veröffentlichen."
        ),
        cells=[
            NotebookCellDefinition(
                cell_id="mwst-sg-filter",
                data_sources=[source],
                sql=(
                    "-- Data Owner: Kassandra Valdata (ESTV). Rein synthetische Beispieldaten.\n"
                    "-- Kanton ist die Spalte canton_code. Keine Summierung: Firmenzeilen bleiben erhalten.\n"
                    f"SELECT *\nFROM {source}\n"
                    "WHERE tax_year = 2026\n  AND canton_code = 'SG'\nORDER BY company_id;"
                ),
                processing_hints="Zuerst den MWST-Loader ausführen. Filter selbst prüfen und die Abfrage ausführen.",
                result_expectations="60 Firmen aus SG, Jahr 2026, vorläufig per 12.11.2026. Anschliessend «Datenprodukt veröffentlichen» wählen.",
                query_options={
                    "duckdb": {
                        "resultStorage": {"mode": "on", "path": f"s3://{BUCKET}/{RESULT_KEY}"}
                    }
                },
            )
        ],
        tags=["mwst", "customer-journey", "synthetic", "st-gallen"],
        tree_path=("Customer Journeys",),
        linked_generator_id=GENERATOR_ID,
        can_edit=True,
        can_delete=False,
        shared=True,
        created_at="2026-11-12T08:00:00+00:00",
    )
