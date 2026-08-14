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
