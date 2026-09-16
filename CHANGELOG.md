# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.3.0 (2026-09-15)

### Feat

- **loader**: attach identifier aliases per batch and per record
- **loader**: resolve native catalog ids per batch and write gm_item_id
- **telemetry**: join the extractor's trace and span every batch flush

### Fix

- **ci**: accept commitizen's no-eligible-commits bump-preview state
- **deps**: bump python to 3.14.7-slim and dockerfile frontend to 1.27 (#1)
- **upsert**: backfill media in the non-batch path too
- **batch**: backfill media on hash-unchanged releases rows
- **media**: require all formats entries be mappings before routing structured
- **media**: preserve format qty in legacy media fallback

### Refactor

- **delivery**: adopt shared SQL runtime contracts
- **sql**: isolate batch writes from coordination
- **sql**: separate consumer lifecycle from persistence

## v0.2.0 (2026-09-04)

### Feat

- **loader**: write the canonical media column on release upsert
- **telemetry**: adopt common.telemetry and record pipeline metrics

### Fix

- **contracts**: add a local dead-letter-name adapter for the split binding
- **image**: install the otel extra in the runtime image
- **ci**: use public python libraries

## v0.1.1 (2026-08-31)

### Fix

- **ci**: accept release-boundary bump states and use Commitizen's supported files-only option

## v0.1.0 (2026-08-31)

The v0.1.0 workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
