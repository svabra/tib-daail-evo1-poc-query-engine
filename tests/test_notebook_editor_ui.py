from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "static"


class NotebookEditorUiRegressionTests(unittest.TestCase):
    def test_client_rendered_notebooks_include_editor_expand_control(self) -> None:
        source = (STATIC_ROOT / "js" / "notebook-workspace-markup.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("data-copy-editor-sql", source)
        self.assertIn("data-expand-editor", source)
        self.assertIn('aria-label="Expand SQL editor"', source)
        self.assertIn('aria-pressed="false"', source)

    def test_editor_expand_control_has_runtime_wiring_and_styles(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        autosize_source = (
            STATIC_ROOT / "js" / "editor-autosize-manager.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("function toggleEditorExpanded", app_source)
        self.assertIn("[data-expand-editor]", app_source)
        self.assertIn('classList.contains("is-editor-expanded")', autosize_source)
        self.assertIn(".editor-expand-button", css_source)


if __name__ == "__main__":
    unittest.main()
