"""Canonical media block derivation for `releases` rows (ADR 0007).

`releases.media` is indexed and must never be NULL for a row this loader writes, so both
upsert paths route every release through :func:`media_for_release` before writing. A
`releases` event that already carries the canonical `media` block (added at the Discogs
producer's normalisation boundary) is written verbatim. An event that predates the field
carries only the raw `formats` list, so a best-effort block is derived from it through the
runtime's shared legacy mapper — the same fallback the ADR specifies for any consumer that
receives a pre-decision event. `data` (the whole JSONB payload) is never touched; this module
only decides what goes in the separate `media` column.
"""

from __future__ import annotations

from typing import Any

from common.media import flatten_descriptions, legacy_format_names_to_media


__all__ = ["media_for_release"]


def media_for_release(data: dict[str, Any]) -> dict[str, Any]:
    """Return the canonical media block to write to `releases.media`.

    Args:
        data: The (consumer-normalized) `releases` record.

    Returns:
        `data["media"]` verbatim when present and a mapping, otherwise a best-effort block
        derived from the raw `formats` list via `common.media.legacy_format_names_to_media`.
    """
    media = data.get("media")
    if isinstance(media, dict):
        return media
    return legacy_format_names_to_media(_flatten_formats(data.get("formats")))


def _flatten_formats(formats: object) -> list[str]:
    """Flatten the raw Discogs `formats` list to one ordered list of names.

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
