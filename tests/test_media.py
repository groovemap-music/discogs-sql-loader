"""Tests for tableinator.media (releases.media derivation, ADR 0007)."""

from typing import Any

from tableinator.media import media_for_release


class TestMediaForRelease:
    """`media_for_release` decides what a release upsert writes to `releases.media`."""

    def test_present_media_is_returned_verbatim(self) -> None:
        """When the event already carries a canonical `media` block, use it as-is."""
        media_block: dict[str, Any] = {"taxonomy_version": "1", "items": [], "families": ["vinyl"]}
        data = {"id": "1", "media": media_block, "formats": [{"name": "CD"}]}

        assert media_for_release(data) is media_block

    def test_absent_media_is_derived_from_formats(self) -> None:
        """When `media` is absent, derive a block from the raw `formats` list."""
        data = {
            "id": "1",
            "formats": [{"name": "Vinyl", "qty": "1", "descriptions": {"description": "LP"}}],
        }

        block = media_for_release(data)

        assert block["families"] == ["vinyl"]
        assert block["items"][0]["medium"] == "vinyl_12"
        assert block["items"][0]["source"] == {
            "provider": "discogs",
            "name": "Vinyl",
            "descriptions": ["LP"],
            "text": None,
        }

    def test_absent_media_multi_entry_formats_preserve_order(self) -> None:
        """Each entry's descriptions stay attached to its own format across entries.

        The legacy path flattens to names only, so per-entry `qty` is lost (defaults to
        1) -- the ADR-documented limitation of this fallback versus the structured mapper.
        """
        data = {
            "id": "1",
            "formats": [
                {"name": "Vinyl", "qty": "2", "descriptions": ["LP", "Album"]},
                {"name": "CD", "descriptions": ["Compilation"]},
            ],
        }

        block = media_for_release(data)

        assert block["families"] == ["optical", "vinyl"]
        vinyl_item = next(item for item in block["items"] if item["family"] == "vinyl")
        cd_item = next(item for item in block["items"] if item["family"] == "optical")
        assert "Compilation" not in vinyl_item["source"]["descriptions"]
        assert cd_item["source"]["descriptions"] == ["Compilation"]

    def test_unmapped_only_formats_still_derive_a_block(self) -> None:
        """A format name the vocabulary does not know yields an empty-but-present block."""
        data = {"id": "1", "formats": [{"name": "Zorbatron"}]}

        block = media_for_release(data)

        assert block["items"] == []
        assert block["families"] == []
        assert "Zorbatron" in block["unmapped"]["formats"] or "Zorbatron" in block["unmapped"]["descriptions"]

    def test_missing_formats_derive_an_empty_block(self) -> None:
        """No `formats` at all still returns a well-formed (empty) block, never None."""
        block = media_for_release({"id": "1"})

        assert block["items"] == []
        assert block["families"] == []
        assert block["unmapped"] == {"formats": [], "descriptions": []}

    def test_non_dict_formats_entries_are_skipped(self) -> None:
        """A malformed `formats` list (non-mapping entries) degrades to an empty block."""
        block = media_for_release({"id": "1", "formats": ["Vinyl", None, 42]})

        assert block["items"] == []
        assert block["unmapped"]["formats"] == []

    def test_derivation_is_idempotent(self) -> None:
        """Deriving twice from the same input yields byte-identical blocks."""
        data = {
            "id": "1",
            "formats": [{"name": "Vinyl", "qty": "1", "descriptions": {"description": "LP"}}],
        }

        assert media_for_release(data) == media_for_release(data)

    def test_non_dict_media_falls_back_to_derivation(self) -> None:
        """A malformed `media` value (not a mapping) is treated as absent."""
        data = {"id": "1", "media": "not-a-block", "formats": [{"name": "Vinyl"}]}

        block = media_for_release(data)

        assert block["families"] == ["vinyl"]
