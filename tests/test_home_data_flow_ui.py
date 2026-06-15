from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "static"
TEMPLATE_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "templates"


class HomeDataFlowUiTests(unittest.TestCase):
    def test_template_contains_home_lineage_payload_hook(self) -> None:
        source = (TEMPLATE_ROOT / "partials" / "home.html").read_text(encoding="utf-8")

        self.assertIn('class="home-flow-showcase"', source)
        self.assertIn('class="home-flow-panel"', source)
        self.assertIn('data-home-data-flow', source)
        self.assertIn('data-home-data-flow-json', source)
        self.assertIn("home_data_flows", source)
        self.assertIn("Data Flow Visualization", source)

    def test_home_ui_wires_carousel_controls_and_modern_status_treatment(self) -> None:
        source = (STATIC_ROOT / "js" / "home-ui.js").read_text(encoding="utf-8")

        self.assertIn("data-home-flow-prev", source)
        self.assertIn("data-home-flow-next", source)
        self.assertIn("data-home-flow-pause", source)
        self.assertIn("home-flow-particle", source)
        self.assertIn("dataFlowNodeMarkup", source)
        self.assertIn("dataFlowIconMarkup", source)
        self.assertNotIn("round-check", source)

    def test_css_includes_reduced_motion_and_particle_lineage_styles(self) -> None:
        source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn(".home-flow-panel", source)
        self.assertIn(".home-flow-showcase", source)
        self.assertIn(".home-flow-particle", source)
        self.assertIn(".home-flow-stage-meter", source)
        self.assertIn("@keyframes home-flow-particle", source)
        self.assertIn("@media (prefers-reduced-motion: reduce)", source)
        self.assertIn(".home-flow-paths path", source)

    def test_lineage_animation_uses_hidden_routes_and_card_to_card_particles(self) -> None:
        script = (STATIC_ROOT / "js" / "home-ui.js").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn('d="M74 33 L285 33', script)
        self.assertIn('d="M74 117 C155 117', script)
        self.assertIn('d="M74 117 L285 117', script)
        self.assertNotIn('d="M0 33', script)
        self.assertNotIn('d="M0 117', script)
        self.assertIn('data-flow-duration="3625"', script)
        self.assertIn('data-flow-duration="3938"', script)
        self.assertIn('data-flow-duration="4250"', script)
        self.assertIn("stroke: transparent;", styles)
        self.assertIn("stroke-width: 0;", styles)


if __name__ == "__main__":
    unittest.main()
