# Data Generator Folder Guide

## Responsibility
- Deterministic smoke, union, contest, and multi-table loader modules plus registry wiring.
- These loaders represent Use Cases for our clients that they can load and test themselves. It's not limited to performance tests but explains functionality like UNION among different data source types e.g. postgresql + s3

## Working Rules
- Keep generator metadata stable: ids, titles, linked notebooks, target info, and size controls.
- Prefer explicit, deterministic outputs over hidden side effects.
- When changing generated targets, update tests that cover `writtenTargets` payloads and linked notebook behavior.