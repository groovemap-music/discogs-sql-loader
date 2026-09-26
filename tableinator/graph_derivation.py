"""Derive the `graph` vertex and edge rows one Discogs document asserts.

This module is the loader's half of the edge model: it turns one normalized Discogs
document into the rows `database-schema` declares for the `graph` schema, and it does it
with no database anywhere in sight, so every rule below is unit-testable against the same
payload shapes `discogs-graph-enricher` projects into Neo4j.

Every rule mirrors a named function of the enricher, which stays the reference
implementation. The mapping is one-to-one:

| relation | enricher function |
| --- | --- |
| `by_artist`, `on_label`, `derived_from`, `in_genre`, `in_style` | `entity_projection.process_release` |
| `master_by_artist`, `master_in_genre`, `master_in_style` | `entity_projection.process_master` |
| `member_of`, `alias_of` | `entity_projection.process_artist` |
| `credited_on`, `same_as`, `person` | `entity_projection.process_release` (the `extraartists` block) |
| `credited_to`, `company` | `company_projection.credited_to_rows` / `company_identity` |
| `issued_on`, `medium`, `media_family` | `media_projection.issued_on_rows` / `resolve_media_block` |
| `genre`, `style` | the `MERGE (:Genre)` / `MERGE (:Style)` statements of both entity projections |
| `track_credited_on`, `track_by_artist` | no enricher counterpart; mirrors `groovemap_schema.postgres`'s `_TRACK_CREDIT_SOURCE` / `_TRACK_PERFORMER_SOURCE` |

`database-schema`'s `_phase0_relation_bodies()` states the same rules in SQL and is what
`graph.bootstrap_fill()` runs, so where a Python truthiness test and a SQL predicate could
disagree the SQL is followed: an id is unusable when it is blank *or* the Discogs "no
entity" sentinel `0`, which is `_usable_id` in that module.

Three relations the enricher writes are deliberately absent. `part_of`, `in_family`, and
`sublabel_of` are VIEWS at the pinned schema revision, so a label document derives no rows
at all and the genre/style/medium vertex rows this module writes are what makes the first
two resolve.

`track_credited_on` and `track_by_artist` are a fourth kind of absence: they were declared
by `database-schema` (gm-database-schema-ug3v) for the FastRP embedding pipeline chw.2's
spike calls for, after `Neo4j`/`discogs-graph-enricher` last moved, so there is no enricher
function to mirror and no cross-store parity claim to make for them — `tests/integration/
test_store_parity.py` excludes both by name rather than by omission. The schema's own
`_TRACK_CREDIT_SOURCE` / `_TRACK_PERFORMER_SOURCE` bodies are the reference implementation
instead, and this module's `_track_credits` / `_track_performers` mirror them field for
field, including the raw xmltodict wrapper `tracklist`, `sub_tracks`, and a track's own
`extraartists`/`artists` are still in — `discogs-ingestion`'s `normalize_release` unwraps
every release-level array this schema reads but never recurses into a track.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final, NamedTuple

from common.media import medium_label

from tableinator.media import media_for_release


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


__all__ = [
    "DISCOGS_SOURCE",
    "EDGE_COLUMNS",
    "VERTEX_COLUMNS",
    "DocumentGraph",
    "company_identity",
    "derive_document",
]

# Provenance stamped on every `issued_on` and `credited_to` row this loader writes, and the
# only value its source-scoped deletes ever touch. `musicbrainz-sql-loader` owns
# `source = 'musicbrainz'` rows over the same shared `medium` vocabulary.
DISCOGS_SOURCE: Final = "discogs"

# The role category the vendored vocabulary reserves for a role it does not recognise.
# `company_projection.UNMAPPED_ROLE_CATEGORY`.
UNMAPPED_ROLE_CATEGORY: Final = "other"

# Prefix for the derived id of a company the source gave no Discogs id.
# `company_projection.DERIVED_ID_PREFIX`.
DERIVED_ID_PREFIX: Final = "name:"

# The Discogs "no entity" sentinel. `_usable_id` in `groovemap_schema.postgres` drops it,
# and the enricher drops the numeric `0` the same way through plain truthiness.
NO_ENTITY_ID: Final = "0"

_DIGITS: Final = re.compile(r"[0-9]+")

# Column order per relation, copied from `_BOOTSTRAP_COLUMNS` in `groovemap_schema.postgres`.
# `credited_on.role_category` is absent on purpose: it is a GENERATED column bound to
# `graph.credit_role_category`, and naming it in an INSERT is an error rather than an
# overwrite.
VERTEX_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "genre": ("name",),
    "style": ("name",),
    "person": ("name",),
    "media_family": ("name",),
    "medium": ("medium_id", "family", "label"),
    "company": ("company_id", "name", "discogs_label_id"),
}

EDGE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "by_artist": ("release_id", "artist_id"),
    "on_label": ("release_id", "label_id"),
    "derived_from": ("release_id", "master_id"),
    "in_genre": ("release_id", "genre_name"),
    "in_style": ("release_id", "style_name"),
    "master_by_artist": ("master_id", "artist_id"),
    "master_in_genre": ("master_id", "genre_name"),
    "master_in_style": ("master_id", "style_name"),
    "member_of": ("member_artist_id", "group_artist_id"),
    "alias_of": ("alias_artist_id", "artist_id"),
    "credited_on": ("person_name", "release_id", "role"),
    "same_as": ("person_name", "artist_id"),
    "credited_to": ("release_id", "company_id", "role", "role_category", "source"),
    "issued_on": ("release_id", "medium_id", "source", "qty"),
    "track_credited_on": ("person_name", "release_id", "track_ordinal", "sub_track_ordinal", "track_position", "role"),
    "track_by_artist": ("release_id", "track_ordinal", "sub_track_ordinal", "track_position", "artist_id"),
}


class DocumentGraph(NamedTuple):
    """The vertex rows, edge rows, and document-scoped replacements of one document.

    `replaced` names the edge relations whose rows for THIS document are deleted before
    `edges` is inserted. A relation absent from it is written additively, which is the
    behaviour the enricher has for the relations it never prunes; see `derive_artist` and
    the `same_as` note in `derive_release` for the two cases and why.
    """

    vertices: dict[str, list[tuple[Any, ...]]]
    edges: dict[str, list[tuple[Any, ...]]]
    replaced: frozenset[str]


def derive_document(data_type: str, data_id: str, data: dict[str, Any]) -> DocumentGraph:
    """Return the graph rows one Discogs document asserts.

    Args:
        data_type: One of the four contract entity tables.
        data_id: The document's Discogs id, as the loader keys its entity row.
        data: The normalized document payload.

    Returns:
        The vertex rows, edge rows, and replaced relations for this one document. An
        unknown `data_type` and a `labels` document both derive nothing.
    """
    if data_type == "artists":
        return derive_artist(data_id, data)
    if data_type == "masters":
        return derive_master(data_id, data)
    if data_type == "releases":
        return derive_release(data_id, data)
    # A label asserts only SUBLABEL_OF, which is a view at the pinned schema revision.
    return DocumentGraph({}, {}, frozenset())


def derive_artist(data_id: str, data: dict[str, Any]) -> DocumentGraph:
    """Return the `member_of` and `alias_of` rows one artist document asserts.

    Mirrors `entity_projection.process_artist`. Discogs states band membership from both
    ends — the band lists `members`, the member lists `groups` — so both unnests become one
    directed `member_of` edge and the reciprocal pair collapses, exactly as the `UNION` in
    the phase 0 view does.

    `member_of` is NOT in `replaced`, and that is the one place this module knowingly
    departs from a blanket document-scoped delete. A `member_of` row names two artists and
    EITHER of their documents can assert it, so a delete scoped to this document would drop
    a row the other end still asserts, and whether it survives would depend on which of the
    two documents the dump happened to deliver last. The enricher never prunes MEMBER_OF
    either, so writing it additively is what keeps the two stores agreeing. `alias_of` has
    no such ambiguity: a row's `artist_id` is the asserting document, so it is replaced.

    The cost is a known, bounded drift from the schema's own phase 0 body, which is a
    `UNION` over both unnests and therefore drops a row the moment NEITHER end asserts it.
    Written additively, a membership Discogs withdraws from both documents survives here
    and in Neo4j until something sweeps it, so this relation and `same_as` are the two
    `graph.bootstrap_fill()` can disagree with after a removal. Reconciling them belongs to
    the `extraction_complete` pass in gm-discogs-sql-loader-2eg.3, which already re-reads
    every edge table to recompute the counters; it is deliberately NOT done per document,
    because per document is exactly the scope that cannot tell the two ends apart.
    """
    members = [(member_id, data_id) for member_id in _element_ids(data.get("members"))]
    groups = [(data_id, group_id) for group_id in _element_ids(data.get("groups"))]
    aliases = [(alias_id, data_id) for alias_id in _element_ids(data.get("aliases"))]

    edges: dict[str, list[tuple[Any, ...]]] = {}
    _put(edges, "member_of", _unique([*members, *groups]))
    _put(edges, "alias_of", _unique(aliases))
    return DocumentGraph({}, edges, frozenset({"alias_of"}))


def derive_master(data_id: str, data: dict[str, Any]) -> DocumentGraph:
    """Return the vertex and edge rows one master document asserts.

    Mirrors `entity_projection.process_master`, whose `_prune_stale_edges` calls make BY,
    IS(Genre), and IS(Style) a replace rather than an append.
    """
    genres = _tag_names(data.get("genres"))
    styles = _tag_names(data.get("styles"))

    vertices: dict[str, list[tuple[Any, ...]]] = {}
    _put(vertices, "genre", [(name,) for name in genres])
    _put(vertices, "style", [(name,) for name in styles])

    edges: dict[str, list[tuple[Any, ...]]] = {}
    _put(edges, "master_by_artist", [(data_id, artist_id) for artist_id in _element_ids(data.get("artists"))])
    _put(edges, "master_in_genre", [(data_id, name) for name in genres])
    _put(edges, "master_in_style", [(data_id, name) for name in styles])

    return DocumentGraph(vertices, edges, frozenset({"master_by_artist", "master_in_genre", "master_in_style"}))


def derive_release(data_id: str, data: dict[str, Any]) -> DocumentGraph:
    """Return the vertex and edge rows one release document asserts.

    Mirrors `entity_projection.process_release` statement for statement: BY, ON,
    DERIVED_FROM, IS(Genre), and IS(Style) are pruned there and so are replaced here;
    ISSUED_ON is pruned through `PRUNE_ISSUED_ON_CYPHER` and CREDITED_TO through
    `PRUNE_CREDITED_TO_CYPHER`. `track_credited_on` and `track_by_artist` (gm-database-
    schema-ug3v) have no enricher counterpart to mirror; `_track_credits` and
    `_track_performers` mirror the schema's own `_TRACK_CREDIT_SOURCE` /
    `_TRACK_PERFORMER_SOURCE` instead, and are release-scoped the same way `credited_on`
    and `by_artist` are, so they are replaced here for the same reason.

    Two relations are written additively. `credited_on` is release-scoped and could be
    replaced, and is — the enricher leaves a superseded credit behind, and a
    document-scoped delete is unambiguous here because `release_id` names the only document
    that can assert the row. `same_as` cannot be: its key is `(person_name, artist_id)` with
    no release column at all, so every release that credits the same person by id asserts
    the same row and no delete scoped to one release can tell them apart. The enricher never
    prunes SAME_AS either. Like `member_of`, that leaves it able to outlive the last
    release that asserted it, which is the one place it can disagree with the phase 0
    body; see `derive_artist` for why the sweep belongs to the `extraction_complete` pass
    rather than to a document. A person named only on a track, never on the release itself,
    resolves to an artist id and mints a `person` vertex the same way: `same_as` and
    `person` fold the track-level credits in alongside the release-level ones rather than
    keying them apart, because neither relation carries a column that would let it.

    A release carrying no canonical `companies` block leaves `credited_to` untouched —
    neither deleted nor written. That is `company_projection.resolve_companies_block`: such
    a record is silent about company credits rather than asserting it has none, and an
    empty-keep prune would delete the credits a post-cutover event already wrote.
    """
    genres = _tag_names(data.get("genres"))
    styles = _tag_names(data.get("styles"))
    credits = _credits(data.get("extraartists"))
    track_credits = _track_credits(data.get("tracklist"))
    track_performers = _track_performers(data.get("tracklist"))
    media_items = _media_rows(media_for_release(data))
    companies_block = data.get("companies")
    company_rows = _company_rows(companies_block) if isinstance(companies_block, dict) else []

    vertices: dict[str, list[tuple[Any, ...]]] = {}
    _put(vertices, "genre", [(name,) for name in genres])
    _put(vertices, "style", [(name,) for name in styles])
    _put(
        vertices,
        "person",
        _unique(
            [(name,) for name, _role, _artist_id in credits]
            + [(name,) for _track_ordinal, _sub_track_ordinal, _position, name, _role, _artist_id in track_credits]
        ),
    )
    _put(vertices, "media_family", _unique([(family,) for _medium, family, _label, _qty in media_items]))
    _put(vertices, "medium", [(medium, family, label) for medium, family, label, _qty in media_items])
    _put(vertices, "company", [(company_id, name, _discogs_label_id(company_id)) for company_id, name, _role, _category in company_rows])

    edges: dict[str, list[tuple[Any, ...]]] = {}
    _put(edges, "by_artist", [(data_id, artist_id) for artist_id in _element_ids(data.get("artists"))])
    _put(edges, "on_label", [(data_id, label_id) for label_id in _element_ids(data.get("labels"))])
    master_id = _entity_id(data.get("master_id"))
    _put(edges, "derived_from", [(data_id, master_id)] if master_id is not None else [])
    _put(edges, "in_genre", [(data_id, name) for name in genres])
    _put(edges, "in_style", [(data_id, name) for name in styles])
    _put(edges, "credited_on", _unique([(name, data_id, role) for name, role, _artist_id in credits]))
    _put(
        edges,
        "same_as",
        _unique(
            [(name, artist_id) for name, _role, artist_id in credits if artist_id is not None]
            + [(name, artist_id) for _track_ordinal, _sub_track_ordinal, _position, name, _role, artist_id in track_credits if artist_id is not None]
        ),
    )
    _put(edges, "issued_on", [(data_id, medium, DISCOGS_SOURCE, qty) for medium, _family, _label, qty in media_items])
    _put(
        edges,
        "credited_to",
        [(data_id, company_id, role, category, DISCOGS_SOURCE) for company_id, _name, role, category in company_rows],
    )
    _put(
        edges,
        "track_credited_on",
        _unique(
            [
                (name, data_id, track_ordinal, sub_track_ordinal, position, role)
                for track_ordinal, sub_track_ordinal, position, name, role, _artist_id in track_credits
            ]
        ),
    )
    _put(
        edges,
        "track_by_artist",
        _unique(
            [
                (data_id, track_ordinal, sub_track_ordinal, position, artist_id)
                for track_ordinal, sub_track_ordinal, position, artist_id in track_performers
            ]
        ),
    )

    replaced = {
        "by_artist",
        "on_label",
        "derived_from",
        "in_genre",
        "in_style",
        "credited_on",
        "issued_on",
        "track_credited_on",
        "track_by_artist",
    }
    if isinstance(companies_block, dict):
        replaced.add("credited_to")
    return DocumentGraph(vertices, edges, frozenset(replaced))


def company_identity(item: dict[str, Any]) -> str | None:
    """Return the `graph.company.company_id` for one companies entry, or None.

    Verbatim `company_projection.company_identity`: a whole Discogs id of at least one when
    the source supplies one, stringified because the dump states it as element text while
    the API states it as a number; otherwise `name:` followed by the name case-folded with
    inner whitespace collapsed and punctuation left alone. The fold is Python's
    `str.casefold`, which is the rule of record — `graph.bootstrap_fill()` approximates it
    with SQL `lower` and the schema says this loader's answer is the correct one.
    """
    discogs_id = _discogs_id(item.get("discogs_id"))
    if discogs_id is not None:
        return discogs_id
    name = _text(item.get("name"))
    return None if name is None else DERIVED_ID_PREFIX + " ".join(name.split()).casefold()


def _put(target: dict[str, list[tuple[Any, ...]]], relation: str, rows: list[tuple[Any, ...]]) -> None:
    """Record ROWS under RELATION, leaving a relation this document says nothing about out."""
    if rows:
        target[relation] = rows


def _unique(rows: Iterable[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Return ROWS deduplicated in source order — the set semantics every relation has."""
    return list(dict.fromkeys(rows))


def _elements(value: Any) -> Iterator[dict[str, Any]]:
    """Yield the mapping elements of a JSON array, skipping anything else.

    Discogs documents are not schema-checked, so an array can be absent, null, or a scalar.
    `_jsonb_array` in `groovemap_schema.postgres` guards the SQL side the same way.
    """
    if not isinstance(value, list):
        return
    for element in value:
        if isinstance(element, dict):
            yield element


def _element_ids(value: Any) -> list[str]:
    """Return the usable ids of a JSON array of entity references, deduplicated in order."""
    ids = [_entity_id(element.get("id")) for element in _elements(value)]
    return list(dict.fromkeys(entity_id for entity_id in ids if entity_id is not None))


def _entity_id(value: Any) -> str | None:
    """Return a usable entity id as text, or None when the reference must be dropped.

    `_usable_id` in `groovemap_schema.postgres`: blank drops, and so does the Discogs "no
    entity" sentinel `0`. The enricher drops a numeric `0` through plain truthiness; it
    would keep the STRING `"0"`, which the SQL rule drops, and the SQL rule is followed here
    because it is what `graph.bootstrap_fill()` and the phase 0 views assert.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    return None if not text or text == NO_ENTITY_ID else text


def _xmltodict_array(container: Any, key: str) -> list[Any]:
    """Return the array CONTAINER's xmltodict wrapper unwraps to under KEY.

    `discogs-ingestion`'s `normalize_release` (`unwrap_container` in `src/discogs/
    normalize.rs`) flattens every release-level array this schema reads — `artists`,
    `labels`, `extraartists`, `companies`, `identifiers`, `formats`, `genres`, `styles` —
    but never recurses into `tracklist` or anything nested inside a track: a track's or
    sub-track's own `extraartists`/`artists`, and `sub_tracks` itself, are still exactly
    the raw Discogs XML converts to. Mirrors `_xmltodict_array` in `groovemap_schema.
    postgres`, field for field:

    - CONTAINER already a list: returned as-is (a future producer that normalizes
      `tracklist` the way it normalizes everything else costs this function nothing).
    - CONTAINER a dict carrying KEY: that value, itself unwrapped the same way
      `unwrap_container` unwraps it — a list kept, `None` emptied, anything else (the
      single-child case) wrapped as one element.
    - CONTAINER a dict without KEY, or `None`: empty.
    - Anything else (a bare scalar where a dict was expected): wrapped as a single-element
      list, `unwrap_container`'s own defensive fallback — no known Discogs shape reaches it.
    """
    if isinstance(container, list):
        return container
    if isinstance(container, dict):
        if key not in container:
            return []
        inner = container[key]
        if isinstance(inner, list):
            return inner
        return [] if inner is None else [inner]
    return [] if container is None else [container]


def _track_position(track: dict[str, Any]) -> str | None:
    """Return a track or sub-track's raw `position` label, or None.

    `track.value ->> 'position'` in the schema: the dump's own string, kept verbatim and
    untrimmed for display, never part of a key. A non-string value reads the same as an
    absent one.
    """
    position = track.get("position")
    return position if isinstance(position, str) else None


def _track_credits(tracklist: Any) -> list[tuple[int, int, str | None, str, str, str | None]]:
    """Return `(track_ordinal, sub_track_ordinal, track_position, person_name, role,
    artist_id)` for every usable `extraartists` credit on a track or one of its sub-tracks.

    Mirrors `_TRACK_CREDIT_SOURCE` in `groovemap_schema.postgres`. `track_ordinal` and
    `sub_track_ordinal` are 1-based positions `enumerate` assigns over what
    `_xmltodict_array` unwraps — the same thing `WITH ORDINALITY` counts in the SQL body —
    rather than the dump's own `position` string, which can be empty or repeated.
    `sub_track_ordinal` is `0` for a credit on the track itself. `artist_id` is carried
    through unused by `track_credited_on`, which never stores it, for `same_as` to resolve
    exactly as a release-level credit's id does.
    """
    rows: list[tuple[int, int, str | None, str, str, str | None]] = []
    for track_ordinal, track in enumerate(_xmltodict_array(tracklist, "track"), start=1):
        if not isinstance(track, dict):
            continue
        position = _track_position(track)
        rows.extend(
            (track_ordinal, 0, position, name, role, artist_id)
            for name, role, artist_id in _credits(_xmltodict_array(track.get("extraartists"), "artist"))
        )
        for sub_track_ordinal, sub_track in enumerate(_xmltodict_array(track.get("sub_tracks"), "track"), start=1):
            if not isinstance(sub_track, dict):
                continue
            sub_position = _track_position(sub_track)
            rows.extend(
                (track_ordinal, sub_track_ordinal, sub_position, name, role, artist_id)
                for name, role, artist_id in _credits(_xmltodict_array(sub_track.get("extraartists"), "artist"))
            )
    return rows


def _track_performers(tracklist: Any) -> list[tuple[int, int, str | None, str]]:
    """Return `(track_ordinal, sub_track_ordinal, track_position, artist_id)` for every
    usable formal `<artists>` performer named on a track or one of its sub-tracks.

    Mirrors `_TRACK_PERFORMER_SOURCE`: the same id-bearing shape `by_artist` reads from
    `releases.data->'artists'` at release level, most often naming a different artist than
    the release's own credit on a various-artists compilation. `_element_ids` drops the
    same falsy id and Discogs "no entity" `0` sentinel `by_artist` drops, for the same
    reason; the ordinal keying and xmltodict unwrap are `_track_credits`'s own.
    """
    rows: list[tuple[int, int, str | None, str]] = []
    for track_ordinal, track in enumerate(_xmltodict_array(tracklist, "track"), start=1):
        if not isinstance(track, dict):
            continue
        position = _track_position(track)
        rows.extend((track_ordinal, 0, position, artist_id) for artist_id in _element_ids(_xmltodict_array(track.get("artists"), "artist")))
        for sub_track_ordinal, sub_track in enumerate(_xmltodict_array(track.get("sub_tracks"), "track"), start=1):
            if not isinstance(sub_track, dict):
                continue
            sub_position = _track_position(sub_track)
            rows.extend(
                (track_ordinal, sub_track_ordinal, sub_position, artist_id)
                for artist_id in _element_ids(_xmltodict_array(sub_track.get("artists"), "artist"))
            )
    return rows


def _tag_names(value: Any) -> list[str]:
    """Return the non-empty genre or style names of a tag array, deduplicated in order.

    The enricher keeps every truthy element (`if genre:` in `batch_projection`) and the
    phase 0 view keeps every non-empty one, neither of them trimming, so a name is used
    exactly as the document spells it.
    """
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(name for name in value if isinstance(name, str) and name))


def _credits(value: Any) -> list[tuple[str, str, str | None]]:
    """Return `(person_name, role, artist_id)` for every usable `extraartists` entry.

    `entity_projection.process_release` requires both a name and a role before it MERGEs a
    `:Person` or a `[:CREDITED_ON]`, and reads the name verbatim: `Person.name` is the Neo4j
    key, so folding or trimming it here would key the vertex differently from the node.
    """
    credits: list[tuple[str, str, str | None]] = []
    for element in _elements(value):
        name = element.get("name")
        role = element.get("role", "")
        if isinstance(name, str) and name and isinstance(role, str) and role:
            credits.append((name, role, _entity_id(element.get("id"))))
    return credits


def _media_rows(block: dict[str, Any]) -> list[tuple[str, str, str, int]]:
    """Return `(medium_id, family, label, qty)` per distinct medium of a canonical block.

    `media_projection.issued_on_rows`: `ISSUED_ON` is keyed on `(release, medium, source)`,
    so two entries resolving to the same canonical medium — a 2xLP split across two Discogs
    format entries — are one row whose `qty` is their sum. Source order is preserved, and a
    release whose media are all unmapped yields nothing. The block itself comes from
    `tableinator.media.media_for_release`, which is the same derivation
    `media_projection.resolve_media_block` runs, so the `releases.media` column the loader
    writes and the edges it derives cannot disagree.
    """
    order: list[str] = []
    resolved: dict[str, tuple[str, str]] = {}
    quantities: dict[str, int] = {}
    for item in _elements(block.get("items")):
        medium = item.get("medium")
        family = item.get("family")
        if not isinstance(medium, str) or not medium or not isinstance(family, str) or not family:
            continue
        if medium not in resolved:
            order.append(medium)
            resolved[medium] = (family, _medium_label(medium))
            quantities[medium] = 0
        quantities[medium] += _quantity(item)
    return [(medium, resolved[medium][0], resolved[medium][1], quantities[medium]) for medium in order]


def _company_rows(block: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    """Return `(company_id, name, role, role_category)` per distinct (company, role).

    `company_projection.credited_to_rows`: the same entry repeated in the source is one
    row, the first occurrence wins so the row reads the way the release does, and an entry
    with no usable identity or no role yields nothing rather than a vertex keyed on nothing.
    """
    rows: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    for item in _elements(block.get("items")):
        role = _text(item.get("role"))
        company_id = company_identity(item)
        if role is None or company_id is None:
            continue
        key = (company_id, role)
        if key in rows:
            continue
        rows[key] = (
            company_id,
            _text(item.get("name")) or company_id,
            role,
            _text(item.get("role_category")) or UNMAPPED_ROLE_CATEGORY,
        )
    return list(rows.values())


def _discogs_label_id(company_id: str) -> str | None:
    """Return the company's Discogs label id when its identity IS one, else None.

    The phase 0 `graph.company` body publishes `company_id` as `discogs_label_id` exactly
    when it is all digits; a `name:`-derived identity has no label page to point at.
    """
    return company_id if _DIGITS.fullmatch(company_id) else None


def _medium_label(medium: str) -> str:
    """Return the taxonomy's label for a medium id, falling back to the id itself.

    `media_projection._label`. A producer on a newer taxonomy can name a medium this
    runtime's vendored vocabulary does not carry yet; labelling that row with its own id
    keeps the release's media in the graph rather than failing the whole record over a
    cosmetic property.
    """
    try:
        return medium_label(medium)
    except KeyError:
        return medium


def _quantity(item: dict[str, Any]) -> int:
    """Return an item's unit count, defaulting to one. `media_projection._quantity`."""
    qty = item.get("qty")
    if isinstance(qty, bool) or not isinstance(qty, int) or qty < 1:
        return 1
    return qty


def _discogs_id(value: Any) -> str | None:
    """Return a positive whole Discogs id as text, or None. `company_projection._discogs_id`."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if value >= 1 else None
    if isinstance(value, str):
        text = value.strip()
        return text if _DIGITS.fullmatch(text) and int(text) >= 1 else None
    return None


def _text(value: Any) -> str | None:
    """Return a trimmed non-empty string, or None. `company_projection._text`."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None
