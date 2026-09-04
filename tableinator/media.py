"""Canonical media block derivation for `releases` rows (ADR 0007).

`releases.media` is indexed and must never be NULL for a row this loader writes, so both
upsert paths route every release through :func:`media_for_release` before writing. A
`releases` event that already carries the canonical `media` block (added at the Discogs
producer's normalisation boundary) is written verbatim. An event that predates the field
carries only the raw `formats` list. When that list's entries are still structured objects
(the normalized releases-event shape, or the raw Discogs API shape), it is mapped directly
through the runtime's structured mapper, which preserves each entry's `qty` -- a `2xLP`
release derives an item with `qty: 2`, not `1`. Only when `formats` is not a list of objects
at all (already flattened to bare names, or otherwise malformed) does derivation fall back to
the runtime's name-only legacy mapper, which cannot recover per-entry quantities. `data` (the
whole JSONB payload) is never touched; this module only decides what goes in the separate
`media` column.
"""

from __future__ import annotations

from typing import Any

from common.media import flatten_descriptions, legacy_format_names_to_media, map_discogs_formats


__all__ = ["media_for_release"]


def media_for_release(data: dict[str, Any]) -> dict[str, Any]:
    """Return the canonical media block to write to `releases.media`.

    Args:
        data: The (consumer-normalized) `releases` record.

    Returns:
        `data["media"]` verbatim when present and a mapping. Otherwise, when `formats` is a
        list holding at least one mapping entry, the block `common.media.map_discogs_formats`
        derives directly from that list -- preserving each entry's `qty`. Otherwise (no
        `formats`, or a `formats` list with no mapping entries), a best-effort block derived
        via `common.media.legacy_format_names_to_media`, which cannot recover per-entry `qty`.
    """
    media = data.get("media")
    if isinstance(media, dict):
        return media
    formats = data.get("formats")
    if isinstance(formats, list) and any(isinstance(entry, dict) for entry in formats):
        return map_discogs_formats(formats)
    return legacy_format_names_to_media(_flatten_formats(formats))


def _flatten_formats(formats: object) -> list[str]:
    """Flatten the raw Discogs `formats` list to one ordered list of names.

    Used only for the name-only fallback, when `formats` holds no mapping entries at all
    (there is no structure left to preserve `qty` from).

    Each entry's format `name` precedes its own `descriptions` (flattened via
    `common.media.flatten_descriptions`, which accepts both the normalized releases-event
    shape and the raw Discogs API shape). That ordering is what lets
    `legacy_format_names_to_media` recover which descriptions belong to which format entry.

    Args:
        formats: The raw `formats` value from a release record. Anything that is not a list
            of mappings flattens to an empty list rather than raising.

    Returns:
        A flat, source-ordered list of format names and descriptions.
    """
    names: list[str] = []
    for entry in formats if isinstance(formats, list) else []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if isinstance(name, str):
            names.append(name)
        names.extend(flatten_descriptions(entry.get("descriptions")))
    return names
