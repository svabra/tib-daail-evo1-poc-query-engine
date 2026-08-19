# DAAIF / DaCa customer-journey design QA

Date: 2026-08-13
Viewport: 1920 × 1080
Browsers: Microsoft Edge, headed
States: DAAIF as Joel Ruod; DaCa as Kassandra Valdata

## Visual comparison

- Reference: `output/playwright/daca-reference.png`
- Implementation: `output/playwright/daaif-home.png`
- Side-by-side comparison: `output/playwright/design-comparison.png`
- Real Jupyter chart: `output/playwright/journey-chart.png`
- Shared federal shell verified: full-width `#2f4356` authority strip, white
  Swiss Confederation brand row, right-aligned demo identity, horizontal primary
  navigation, and red active-state marker.
- Shared landing-page language verified: responsive photographic hero,
  high-contrast content overlay, and a bordered search panel integrated into the
  hero. DAAIF retains its own product title, notebook-oriented copy, and existing
  action icons by design.
- The same viewport was used for both captures. No clipped text, stretched hero
  image, accidental horizontal overflow, broken spacing, or missing asset was
  visible in the combined comparison.
- All nine DaCa themes are present in AVIF and WebP at both responsive widths;
  Kassandra and Noémie use the shared portrait assets while Beat, Joel and Thomas
  use legible initial fallbacks.

## Interaction and accessibility checks

- The DAAIF identity selector exposes the same five PoC identities as DaCa,
  visibly labels the context as a demo, persists selection, and returns to Joel.
- Landing search found and directly opened
  “A Data Analyst's Journey – Kantonale Gewerbesteuer”. Search supports keyboard
  submission, result limits, empty/no-result messaging, and an `aria-live`
  result region.
- The Journey loader exposes the Aargau CSV download and its exact manual S3
  upload destination.
- Navigation, browser history, deferred sidebars, notebook/SSE refreshes, and
  Ingestion state were exercised with deliberately reordered responses.
- DAAIF and DaCa browser consoles were checked at the reference states with no
  application warnings or errors.
- The real kernel chart was inspected at original resolution. Both panels,
  CHF units, cutoff date, 2026 projection/YTD labels, zero/mean/median reference
  lines, hatching and synthetic-data notice are legible without clipping.

## Performance checks

- Direct Ingestion: 586,116 B live, 92.4% smaller than the 7.72 MB baseline.
- Deferred Loader sidebar: 5,221 B live, below the 25 KB limit and 99.917%
  smaller than the 6.29 MB baseline.
- Hidden source trees stay deferred, UI clocks run at most once per second and
  pause in hidden tabs, and versioned static assets return immutable cache
  headers.

final result: passed

---

# Design QA — Full-width quick-search results beside the expert CTA

## Comparison target

- Source visual truth: the user-supplied DAAIF screenshot showing unnecessarily wrapped live-result cards.
- Before/after evidence: `output/playwright/expert-search-result-width/daaif-before.png` and `output/playwright/expert-search-result-width/daaif-after.png`.
- Combined DAAIF/DaCa comparison: `output/playwright/expert-search-result-width/comparison.png`.
- Viewport: 1074 × 924 CSS pixels, device scale factor 1; DAAIF query `me` with 137 matches and three preview cards.

## Findings and verification

- Root cause confirmed: the expanded feedback area was 377 px wide, but a permanent 246 px right padding reduced every result card to 131 px.
- Expanded results now use the complete 377 px feedback width. All three titles render on one line instead of up to four lines.
- The 246 px reservation remains in the compact hover/focus state, where it is still needed to protect feedback copy from the expert-search CTA.
- Browser geometry found zero overlap between the three result cards, the all-results link, and the absolute expert-search CTA. The result list has no inner scrollbar.
- At 430 px, all three results remain visible with no inner or page-level horizontal scrollbar and no browser page errors.
- Focused Pytest: 15 passed. The dedicated expert-search Playwright navigation smoke passed.

final result: passed

---

# Design QA — Quick-search preview without an inner scrollbar

## Comparison target

- Source visual truth: the supplied DAAIF screenshot with three `mw` preview cards and an unwanted vertical scrollbar.
- Rendered implementation: `output/playwright/expert-search-hover/daaif-scrollbar-fixed.png`.
- Viewport: 1600 × 1000 CSS pixels, device scale factor 1; focused capture of the expanded search widget.

## Findings and verification

- The preview remains deliberately limited to three results; the complete result set remains available through the expert-search link.
- The redundant 218 px `max-height` and `overflow-y: auto` were removed. The three rendered cards now occupy 226 px with `clientHeight == scrollHeight` and computed `overflow-y: visible`.
- The expanded widget gained 12 px of height so all preview cards and the all-results link remain inside its bounds without clipping or a nested scroll region.
- Desktop and 430 px responsive measurements confirmed three visible result cards and a visible all-results link. The existing one-click expert-search navigation smoke also passed.

final result: passed

---

# Design QA — Expertensuche CTA refinement and one-click navigation

## Comparison target

- Source visual truth: the user-rejected first implementation in `output/playwright/expert-search-hover/daaif-desktop-hover.png`, together with the established federal/BIT landing-page design visible in the supplied reference.
- Rendered implementation: `output/playwright/expert-search-hover/daaif-desktop-hover-refined-final.png`.
- Combined before/after evidence: `output/playwright/expert-search-hover/refinement-comparison.png`.
- Viewport: 1440 × 1000 CSS pixels, device scale factor 1; source and implementation are both 1440 × 1000 pixels, so no density normalization was required.
- State: blank quick-search widget, loaded search index, pointer hovering over the widget.

## Findings

- The earlier P1 visual finding is resolved: the small generic secondary button has been replaced by a deliberate BIT-blue action surface with a federal-red top rule, clear microcopy, stronger hierarchy, and suitable elevation.
- The earlier P0 interaction finding is resolved: focusing the CTA no longer expands and relocates its parent widget between pointer-down and pointer-up. A single native click now navigates directly to `/search`.
- A post-fix P1 overlap found during live-result testing is resolved: compact feedback reserves the CTA area, while expanded results end above its dedicated bottom action row.
- A P2 copy-clipping issue was resolved by shortening the compact DAAIF hint while retaining its meaning.

## Required fidelity surfaces

- Fonts and typography: existing DAAIF/Noto Sans typography is retained; the CTA adds an uppercase 0.61 rem context line and a 0.9 rem high-emphasis action label.
- Spacing and layout rhythm: the action aligns to the existing 22 px right and 18 px bottom insets; compact feedback reserves 246 px, while expanded result cards use the full content width without changing the hero geometry.
- Colors and visual tokens: BIT blue, federal red, white foreground, and the existing neutral shadow vocabulary are used; no new arbitrary palette was introduced.
- Image quality and asset fidelity: hero assets, crop, responsiveness, and loading behavior are unchanged; the CTA needs no image or icon asset.
- Copy and content: `Alle Inhalte · Erweiterte Filter` explains the added capability and `Expertensuche öffnen` states the navigation outcome.

## Interaction and runtime evidence

- Resting state hidden, hover reveal, focus reveal, query preservation, native pointer-down stability, first-click navigation, live-result clickability, and expert-search rendering were exercised in Chromium.
- Browser console and page-error checks reported zero errors.
- Focused Pytest: 15 passed. The dedicated Playwright navigation smoke passed.

## Comparison history

1. The initial implementation was visually too generic and required a second click because focus expanded the widget mid-click.
2. The CTA became a branded two-level action surface and its focus event was excluded from quick-search expansion.
3. Live-result testing exposed pointer interception; the result column was constrained beside the action.
4. The final combined comparison shows a stronger, consistent CTA in both DAAIF and DaCa with no remaining P0/P1/P2 issue.

final result: passed

---

# Design QA — DaCa-aligned expert-search canvas

## Comparison target

- Source visual truth: the supplied DaCa expert-search screenshot and the live local DaCa `/search` reference.
- Rendered implementation: `output/playwright/expert-search-background/daaif.png`.
- Combined comparison: `output/playwright/expert-search-background/comparison.png`.
- Viewport and density: both pages at 1600 × 1000 CSS pixels, device scale factor 1; no density normalization required.
- State: expert-search landing state; DAAIF shows its loaded default result set, DaCa shows its empty-query guidance.

## Findings

- The P1 color mismatch is resolved. DAAIF no longer uses the red radial page wash.
- DAAIF now uses the same three-layer canvas as DaCa: white light at 12%/26%, a 7% BIT-blue accent at 86%/16%, and the `#fafbfc` to `#f4f6f7` neutral gradient.
- Red remains only in intentional federal accents such as eyebrows, panel rules, and primary actions.
- Fonts, spacing, imagery, icons, and page copy were not changed by this focused correction.
- Browser verification found no horizontal overflow, console errors, or page errors.

## Comparison history

1. The supplied DAAIF screenshot exposed a broad pale-red canvas that did not match DaCa.
2. The global red radial wash was replaced with DaCa's neutral/blue canvas tokens.
3. The equal-size side-by-side comparison shows no remaining P0/P1/P2 background mismatch.

final result: passed
