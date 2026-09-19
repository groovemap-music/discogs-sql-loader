"""Every derivation rule in `tableinator.graph_derivation`, against the enricher's rules.

Each test names the `discogs-graph-enricher` function it holds this loader to. The media
payloads are the `input` blocks of that repository's `tests/fixtures/media/*.json`
conformance fixtures, copied in by name so the two derivations can be compared on the
same bytes without this repository depending on a sibling checkout.
"""

from typing import Any, ClassVar

import pytest

from tableinator.graph_derivation import (
    EDGE_COLUMNS,
    VERTEX_COLUMNS,
    company_identity,
    derive_document,
)


def _rows(data_type: str, data_id: str, data: dict[str, Any], relation: str) -> list[tuple[Any, ...]]:
    """Return the rows one document asserts for RELATION, edges or vertices."""
    document = derive_document(data_type, data_id, data)
    return document.edges.get(relation, document.vertices.get(relation, []))


class TestReleaseEntityEdges:
    """`entity_projection.process_release`: BY, ON, DERIVED_FROM, IS(Genre), IS(Style)."""

    def test_three_artists_and_two_genres_do_not_fan_out(self) -> None:
        """A release with three artists and two genres is 3 by_artist and 2 in_genre rows."""
        data = {
            "artists": [{"id": "1"}, {"id": "2"}, {"id": "3"}],
            "genres": ["Electronic", "Rock"],
        }
        document = derive_document("releases", "r1", data)

        assert document.edges["by_artist"] == [("r1", "1"), ("r1", "2"), ("r1", "3")]
        assert document.edges["in_genre"] == [("r1", "Electronic"), ("r1", "Rock")]

    def test_a_label_named_once_per_catalogue_number_is_one_row(self) -> None:
        """`DISTINCT` in the phase 0 body: a repeated reference is one edge."""
        data = {"labels": [{"id": "77", "catno": "A-1"}, {"id": "77", "catno": "A-2"}]}

        assert _rows("releases", "r1", data, "on_label") == [("r1", "77")]

    def test_styles_become_in_style_rows_and_style_vertices(self) -> None:
        """IS(Style) writes both the edge and the name-keyed vertex it points at."""
        document = derive_document("releases", "r1", {"styles": ["House", "Techno"]})

        assert document.edges["in_style"] == [("r1", "House"), ("r1", "Techno")]
        assert document.vertices["style"] == [("House",), ("Techno",)]

    def test_master_id_becomes_one_derived_from_row(self) -> None:
        """A release naming a master derives exactly one DERIVED_FROM edge."""
        assert _rows("releases", "r1", {"master_id": "900"}, "derived_from") == [("r1", "900")]

    @pytest.mark.parametrize("master_id", [None, "", "  ", "0", 0, False])
    def test_an_unusable_master_id_derives_nothing(self, master_id: Any) -> None:
        """`_usable_id`: blank and the Discogs "no entity" sentinel both drop the edge."""
        assert _rows("releases", "r1", {"master_id": master_id}, "derived_from") == []

    def test_a_release_replaces_every_relation_it_owns(self) -> None:
        """The relations the enricher prunes are the ones written as a replace."""
        document = derive_document("releases", "r1", {"artists": [{"id": "1"}]})

        assert document.replaced == frozenset({"by_artist", "on_label", "derived_from", "in_genre", "in_style", "credited_on", "issued_on"})


class TestUsableIds:
    """`_usable_id` in `groovemap_schema.postgres`, mirroring the enricher's truthiness."""

    def test_the_no_entity_sentinel_and_blanks_are_dropped(self) -> None:
        """`0`, `"0"`, blank, and a missing id all drop the element."""
        data = {"artists": [{"id": "0"}, {"id": 0}, {"id": ""}, {"id": "   "}, {"name": "no id"}, {"id": "5"}]}

        assert _rows("releases", "r1", data, "by_artist") == [("r1", "5")]

    def test_a_numeric_id_is_stringified_because_every_key_column_is_text(self) -> None:
        """The dump states an id as element text and the API as a number; both key as text."""
        assert _rows("releases", "r1", {"artists": [{"id": 42}]}, "by_artist") == [("r1", "42")]

    def test_an_id_is_trimmed_before_it_is_used(self) -> None:
        """`btrim` in the phase 0 body."""
        assert _rows("releases", "r1", {"artists": [{"id": " 42 "}]}, "by_artist") == [("r1", "42")]

    @pytest.mark.parametrize("artists", [None, "not-an-array", 7, {"id": "1"}])
    def test_a_malformed_array_skips_the_document_rather_than_raising(self, artists: Any) -> None:
        """`_jsonb_array` guards the SQL side; an unschema'd document must not raise here."""
        assert _rows("releases", "r1", {"artists": artists}, "by_artist") == []

    def test_a_non_mapping_element_is_skipped(self) -> None:
        """A malformed entry drops one element, not the whole document."""
        data = {"artists": ["1", None, {"id": "2"}]}

        assert _rows("releases", "r1", data, "by_artist") == [("r1", "2")]


class TestTagNames:
    """The `MERGE (:Genre)` / `MERGE (:Style)` statements of both entity projections."""

    def test_an_empty_name_is_dropped_and_a_repeat_is_one_row(self) -> None:
        """`if genre:` in `batch_projection`, and `_non_empty` in the phase 0 body."""
        data = {"genres": ["Rock", "", "Rock", "Jazz"]}
        document = derive_document("releases", "r1", data)

        assert document.edges["in_genre"] == [("r1", "Rock"), ("r1", "Jazz")]
        assert document.vertices["genre"] == [("Rock",), ("Jazz",)]

    def test_a_name_is_used_exactly_as_the_document_spells_it(self) -> None:
        """Neither the enricher nor the phase 0 body trims a tag name."""
        assert _rows("releases", "r1", {"genres": [" Hip Hop "]}, "in_genre") == [("r1", " Hip Hop ")]


class TestMasterEdges:
    """`entity_projection.process_master`: BY, IS(Genre), IS(Style) on a master."""

    def test_a_master_derives_its_three_relations_and_replaces_all_of_them(self) -> None:
        data = {"artists": [{"id": "1"}, {"id": "2"}], "genres": ["Rock"], "styles": ["Indie Rock"]}
        document = derive_document("masters", "m1", data)

        assert document.edges["master_by_artist"] == [("m1", "1"), ("m1", "2")]
        assert document.edges["master_in_genre"] == [("m1", "Rock")]
        assert document.edges["master_in_style"] == [("m1", "Indie Rock")]
        assert document.replaced == frozenset({"master_by_artist", "master_in_genre", "master_in_style"})

    def test_a_master_asserts_no_release_relation(self) -> None:
        """A master document must never write a release-keyed edge."""
        document = derive_document("masters", "m1", {"artists": [{"id": "1"}], "labels": [{"id": "9"}]})

        assert "by_artist" not in document.edges
        assert "on_label" not in document.edges


class TestArtistEdges:
    """`entity_projection.process_artist`: MEMBER_OF from both ends, and ALIAS_OF."""

    def test_members_and_groups_become_one_directed_relation(self) -> None:
        """The band lists `members`, the member lists `groups`; both are one edge direction."""
        data = {"members": [{"id": "10"}, {"id": "11"}], "groups": [{"id": "20"}]}

        assert _rows("artists", "a1", data, "member_of") == [("10", "a1"), ("11", "a1"), ("a1", "20")]

    def test_a_reciprocal_pair_collapses_to_one_row(self) -> None:
        """`UNION`, not `UNION ALL`: a self-referencing pair must not assert an edge twice."""
        data = {"members": [{"id": "a1"}], "groups": [{"id": "a1"}]}

        assert _rows("artists", "a1", data, "member_of") == [("a1", "a1")]

    def test_member_of_is_never_replaced_but_alias_of_always_is(self) -> None:
        """Either end's document can assert a member_of row; only one can assert an alias."""
        document = derive_document("artists", "a1", {"members": [{"id": "10"}], "aliases": [{"id": "30"}]})

        assert document.replaced == frozenset({"alias_of"})

    def test_aliases_point_at_the_document_that_named_them(self) -> None:
        assert _rows("artists", "a1", {"aliases": [{"id": "30"}, {"id": "31"}]}, "alias_of") == [
            ("30", "a1"),
            ("31", "a1"),
        ]


class TestLabelDerivesNothing:
    """`sublabel_of` is a VIEW at the pinned schema revision, so no label row is written."""

    def test_a_label_document_asserts_no_table_row_at_all(self) -> None:
        data = {"parentLabel": {"id": "1"}, "sublabels": [{"id": "2"}, {"id": "3"}]}
        document = derive_document("labels", "l1", data)

        assert document.edges == {}
        assert document.vertices == {}
        assert document.replaced == frozenset()


class TestCredits:
    """The `extraartists` block of `entity_projection.process_release`."""

    _CREDITS: ClassVar[dict[str, Any]] = {
        "extraartists": [
            {"id": "5", "name": "Geoff Emerick", "role": "Engineer"},
            {"id": "5", "name": "Geoff Emerick", "role": "Mixed By"},
            {"name": "Uncredited Assistant", "role": "Assistant"},
            {"name": "No Role"},
            {"role": "No Name"},
        ]
    }

    def test_one_person_under_two_roles_is_two_credited_on_rows(self) -> None:
        """CREDITED_ON is keyed on (person, release, role)."""
        assert _rows("releases", "r1", self._CREDITS, "credited_on") == [
            ("Geoff Emerick", "r1", "Engineer"),
            ("Geoff Emerick", "r1", "Mixed By"),
            ("Uncredited Assistant", "r1", "Assistant"),
        ]

    def test_credited_on_never_names_the_generated_role_category(self) -> None:
        """`graph.credited_on.role_category` is GENERATED; naming it in an INSERT is an error."""
        assert EDGE_COLUMNS["credited_on"] == ("person_name", "release_id", "role")

    def test_a_person_vertex_is_keyed_on_the_verbatim_credit_name(self) -> None:
        """`Person.name` is the Neo4j key, so folding it here would key the vertex apart."""
        document = derive_document("releases", "r1", {"extraartists": [{"name": " dr. Dre ", "role": "Producer"}]})

        assert document.vertices["person"] == [(" dr. Dre ",)]
        assert document.edges["credited_on"] == [(" dr. Dre ", "r1", "Producer")]

    def test_a_credit_needs_both_a_name_and_a_role(self) -> None:
        """The enricher MERGEs neither the `:Person` nor the edge without both."""
        document = derive_document("releases", "r1", self._CREDITS)

        assert document.vertices["person"] == [("Geoff Emerick",), ("Uncredited Assistant",)]

    def test_same_as_is_one_row_per_credit_that_names_an_artist(self) -> None:
        """SAME_AS is keyed on (person, artist); a credit with no id asserts nothing."""
        assert _rows("releases", "r1", self._CREDITS, "same_as") == [("Geoff Emerick", "5")]

    def test_same_as_is_never_replaced(self) -> None:
        """It carries no release column, so no delete scoped to one release can reach it."""
        document = derive_document("releases", "r1", self._CREDITS)

        assert "same_as" not in document.replaced
        assert "credited_on" in document.replaced


class TestCompanyIdentity:
    """`company_projection.company_identity` and `credited_to_rows` (ADR 0011)."""

    def test_a_whole_discogs_id_is_the_identity_and_is_stringified(self) -> None:
        """The dump states it as element text and the API as a number; both key as text."""
        assert company_identity({"discogs_id": 1234, "name": "Abbey Road"}) == "1234"
        assert company_identity({"discogs_id": "1234", "name": "Abbey Road"}) == "1234"

    @pytest.mark.parametrize("discogs_id", [0, "0", -1, "", "  ", None, True, "12a"])
    def test_an_unusable_discogs_id_falls_back_to_the_name(self, discogs_id: Any) -> None:
        """`0` is Discogs' "no label", which is not an id."""
        assert company_identity({"discogs_id": discogs_id, "name": "Abbey Road"}) == "name:abbey road"

    def test_a_derived_id_collapses_inner_whitespace_and_leaves_punctuation_alone(self) -> None:
        """Two spellings differing by a comma stay two companies a reconciliation can merge."""
        assert company_identity({"name": "  Abbey   Road\tStudios "}) == "name:abbey road studios"
        assert company_identity({"name": "Abbey Road, Studios"}) == "name:abbey road, studios"

    def test_the_fold_is_python_casefold_not_sql_lower(self) -> None:
        """`graph.bootstrap_fill()` folds with SQL `lower`; this loader's answer is correct."""
        assert company_identity({"name": "Straße"}) == "name:strasse"

    def test_an_entry_with_no_identity_at_all_yields_nothing(self) -> None:
        assert company_identity({"role": "Pressed By"}) is None

    def test_the_company_vertex_publishes_a_label_id_only_when_its_identity_is_one(self) -> None:
        """The phase 0 `graph.company` body publishes `company_id` when it is all digits."""
        block = {"items": [{"discogs_id": "77", "name": "Sony", "role": "Distributed By"}, {"name": "Local Plant", "role": "Pressed By"}]}
        document = derive_document("releases", "r1", {"companies": block})

        assert document.vertices["company"] == [
            ("77", "Sony", "77"),
            ("name:local plant", "Local Plant", None),
        ]


class TestCreditedTo:
    """`company_projection.credited_to_rows` and `resolve_companies_block`."""

    def test_one_row_per_distinct_company_and_role_in_source_order(self) -> None:
        block = {
            "items": [
                {"discogs_id": "1", "name": "Plant", "role": "Pressed By", "role_category": "manufacture"},
                {"discogs_id": "1", "name": "Plant", "role": "Pressed By", "role_category": "manufacture"},
                {"discogs_id": "1", "name": "Plant", "role": "Lacquer Cut By"},
            ]
        }

        assert _rows("releases", "r1", {"companies": block}, "credited_to") == [
            ("r1", "1", "Pressed By", "manufacture", "discogs"),
            ("r1", "1", "Lacquer Cut By", "other", "discogs"),
        ]

    def test_an_entry_with_no_role_yields_no_edge(self) -> None:
        """A node keyed on nothing is worse than a missing edge."""
        block = {"items": [{"discogs_id": "1", "name": "Plant"}, {"role": "Pressed By"}]}

        assert _rows("releases", "r1", {"companies": block}, "credited_to") == []

    def test_a_release_with_no_canonical_block_leaves_credited_to_untouched(self) -> None:
        """A pre-cutover record is silent about company credits, not asserting it has none."""
        document = derive_document("releases", "r1", {"companies": [{"name": "Raw Discogs List"}]})

        assert "credited_to" not in document.edges
        assert "credited_to" not in document.replaced

    def test_an_empty_canonical_block_is_an_assertion_and_replaces(self) -> None:
        """The opposite case: every company credit was removed, so the prune acts on it."""
        document = derive_document("releases", "r1", {"companies": {"items": []}})

        assert "credited_to" not in document.edges
        assert "credited_to" in document.replaced

    def test_role_category_is_a_plain_column_this_loader_writes(self) -> None:
        """Unlike `credited_on`'s, it is not generated — an unwritten one would be null."""
        assert EDGE_COLUMNS["credited_to"] == ("release_id", "company_id", "role", "role_category", "source")


class TestIssuedOn:
    """`media_projection.issued_on_rows`, on the enricher's own conformance fixtures."""

    def test_a_2xlp_derives_one_edge_carrying_its_quantity(self) -> None:
        """`tests/fixtures/media/discogs-2xlp-gatefold-reissue.json`."""
        data = {
            "formats": [
                {
                    "name": "Vinyl",
                    "qty": "2",
                    "text": "Blue Vinyl",
                    "descriptions": {"description": ["LP", "Album", "Reissue", "Remastered", "Gatefold", "Numbered"]},
                }
            ]
        }

        assert _rows("releases", "r1", data, "issued_on") == [("r1", "vinyl_12", "discogs", 2)]

    def test_a_box_set_derives_its_sibling_media_and_not_the_container(self) -> None:
        """`tests/fixtures/media/discogs-box-set-cd-and-vinyl.json`: Box Set is not a medium."""
        data = {
            "formats": [
                {"name": "Box Set", "qty": "1", "descriptions": {"description": ["Limited Edition", "Numbered"]}},
                {"name": "CD", "qty": "2", "descriptions": {"description": ["Album", "Remastered"]}},
                {"name": "Vinyl", "qty": "1", "descriptions": {"description": ["LP", "Picture Disc"]}},
            ]
        }
        document = derive_document("releases", "r1", data)

        assert document.edges["issued_on"] == [
            ("r1", "optical_cd", "discogs", 2),
            ("r1", "vinyl_12", "discogs", 1),
        ]
        assert document.vertices["media_family"] == [("optical",), ("vinyl",)]
        assert [row[0] for row in document.vertices["medium"]] == ["optical_cd", "vinyl_12"]

    def test_an_unmapped_format_derives_no_edge_and_no_vertex(self) -> None:
        """`tests/fixtures/media/discogs-unknown-format.json`."""
        data = {"formats": [{"name": "Holographic Cube", "qty": "1", "descriptions": {"description": ["Album"]}}]}
        document = derive_document("releases", "r1", data)

        assert "issued_on" not in document.edges
        assert "medium" not in document.vertices

    def test_two_entries_resolving_to_one_medium_are_one_edge_whose_qty_is_their_sum(self) -> None:
        """A 2xLP split across two Discogs format entries is one ISSUED_ON edge."""
        block = {"items": [{"medium": "vinyl_12", "family": "vinyl", "qty": 1}, {"medium": "vinyl_12", "family": "vinyl", "qty": 2}]}

        assert _rows("releases", "r1", {"media": block}, "issued_on") == [("r1", "vinyl_12", "discogs", 3)]

    @pytest.mark.parametrize("qty", [None, 0, -3, True, "2", 1.5])
    def test_an_unusable_quantity_defaults_to_one(self, qty: Any) -> None:
        """`media_projection._quantity`."""
        block = {"items": [{"medium": "vinyl_12", "family": "vinyl", "qty": qty}]}

        assert _rows("releases", "r1", {"media": block}, "issued_on") == [("r1", "vinyl_12", "discogs", 1)]

    def test_a_medium_the_vocabulary_does_not_know_is_labelled_with_its_own_id(self) -> None:
        """`media_projection._label`: a newer taxonomy must not fail the whole record."""
        block = {"items": [{"medium": "holo_cube", "family": "holographic"}]}
        document = derive_document("releases", "r1", {"media": block})

        assert document.vertices["medium"] == [("holo_cube", "holographic", "holo_cube")]

    def test_an_item_missing_a_medium_or_a_family_yields_nothing(self) -> None:
        block = {"items": [{"family": "vinyl"}, {"medium": "vinyl_12"}, {"medium": "", "family": "vinyl"}]}

        assert _rows("releases", "r1", {"media": block}, "issued_on") == []

    def test_the_edge_this_loader_writes_is_always_its_own_source(self) -> None:
        """`musicbrainz-sql-loader` owns `source = 'musicbrainz'` over the same vertices."""
        block = {"items": [{"medium": "vinyl_12", "family": "vinyl"}]}
        rows = _rows("releases", "r1", {"media": block}, "issued_on")

        assert [row[EDGE_COLUMNS["issued_on"].index("source")] for row in rows] == ["discogs"]


class TestColumnContracts:
    """The column blocks this module writes, against what the schema declares."""

    def test_every_relation_this_loader_owns_has_a_column_block(self) -> None:
        """Fourteen edge tables; `part_of`, `in_family`, and `sublabel_of` stay views."""
        assert set(EDGE_COLUMNS) == {
            "by_artist",
            "on_label",
            "derived_from",
            "in_genre",
            "in_style",
            "master_by_artist",
            "master_in_genre",
            "master_in_style",
            "member_of",
            "alias_of",
            "credited_on",
            "same_as",
            "credited_to",
            "issued_on",
        }
        assert set(VERTEX_COLUMNS) == {"genre", "style", "person", "media_family", "medium", "company"}

    def test_an_unknown_data_type_derives_nothing_rather_than_raising(self) -> None:
        document = derive_document("videos", "v1", {"artists": [{"id": "1"}]})

        assert document.edges == {}
        assert document.vertices == {}
