"""The set-based `first_year` bodies return the rows the per-tag correlated bodies did.

`COUNTER_BODIES["genre_stats"]` and `["style_stats"]` compute `first_year` in one pass over
`public.releases` (database-schema gm-database-schema-0exy). The correlated form they replace
is kept below as the reference. Both run against the real promoted schema on malformed
years -- `0`, `0000`, empty, padded, text, a decimal, missing -- plus a tag with two dated
releases, a tag with none, and a tag with no release at all, and every row must match.
"""

import os
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from tableinator.graph_counters import COUNTER_BODIES


if TYPE_CHECKING:
    from collections.abc import Iterator


pytestmark = pytest.mark.integration

OLD_BODIES = {
    "genre_stats": """
SELECT genre.name AS name,
       (SELECT count(*) FROM graph.in_genre AS edge WHERE edge.genre_name = genre.name) AS release_count,
       (SELECT count(DISTINCT by_artist.artist_id)
          FROM graph.in_genre AS edge
          JOIN graph.by_artist AS by_artist ON by_artist.release_id = edge.release_id
         WHERE edge.genre_name = genre.name) AS artist_count,
       (SELECT count(DISTINCT on_label.label_id)
          FROM graph.in_genre AS edge
          JOIN graph.on_label AS on_label ON on_label.release_id = edge.release_id
         WHERE edge.genre_name = genre.name) AS label_count,
       (SELECT count(DISTINCT in_style.style_name)
          FROM graph.in_genre AS in_genre
          JOIN graph.in_style AS in_style ON in_style.release_id = in_genre.release_id
         WHERE in_genre.genre_name = genre.name) AS style_count,
       (SELECT min(btrim(release.year)::integer)
          FROM graph.in_genre AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.genre_name = genre.name
           AND btrim(release.year) ~ '^[0-9]+$'
           AND btrim(release.year)::numeric > 0) AS first_year
FROM graph.genre AS genre
""",
    "style_stats": """
SELECT style.name AS name,
       (SELECT count(*) FROM graph.in_style AS edge WHERE edge.style_name = style.name) AS release_count,
       (SELECT count(DISTINCT by_artist.artist_id)
          FROM graph.in_style AS edge
          JOIN graph.by_artist AS by_artist ON by_artist.release_id = edge.release_id
         WHERE edge.style_name = style.name) AS artist_count,
       (SELECT count(DISTINCT on_label.label_id)
          FROM graph.in_style AS edge
          JOIN graph.on_label AS on_label ON on_label.release_id = edge.release_id
         WHERE edge.style_name = style.name) AS label_count,
       (SELECT count(DISTINCT in_genre.genre_name)
          FROM graph.in_style AS in_style
          JOIN graph.in_genre AS in_genre ON in_genre.release_id = in_style.release_id
         WHERE in_style.style_name = style.name) AS genre_count,
       (SELECT min(btrim(release.year)::integer)
          FROM graph.in_style AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.style_name = style.name
           AND btrim(release.year) ~ '^[0-9]+$'
           AND btrim(release.year)::numeric > 0) AS first_year
FROM graph.style AS style
""",
}

# (release id, document) -- one tag each, named for the year it carries.
YEARS: list[tuple[str, dict[str, Any]]] = [
    ("fy-1", {"year": "1970"}),
    ("fy-2", {"year": "0"}),
    ("fy-3", {"year": "0000"}),
    ("fy-4", {"year": ""}),
    ("fy-5", {"year": "  1985 "}),
    ("fy-6", {"year": "abc"}),
    ("fy-7", {"year": "1999.5"}),
    ("fy-8", {}),
    ("fy-9", {"year": "987"}),
    ("fy-10", {"year": "0042"}),
    ("fy-11", {"year": "-5"}),
    ("fy-12", {"year": None}),
]


@pytest.fixture
def connection() -> Iterator[psycopg.Connection[Any]]:
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    with psycopg.connect(dsn) as conn:
        yield conn
        conn.rollback()


@pytest.mark.parametrize(
    ("relation", "vertex", "edge", "column"),
    [("genre_stats", "genre", "in_genre", "genre_name"), ("style_stats", "style", "in_style", "style_name")],
)
def test_the_one_pass_body_matches_the_per_tag_body_on_malformed_years(
    connection: psycopg.Connection[Any], relation: str, vertex: str, edge: str, column: str
) -> None:
    with connection.cursor() as cursor:
        for release_id, document in YEARS:
            payload = {"id": release_id, **document}
            cursor.execute(
                "INSERT INTO public.releases (hash, data_id, data) VALUES (%s, %s, %s::jsonb)",
                ("h", release_id, psycopg.types.json.Jsonb(payload)),
            )
            tag = f"tag-{release_id}"
            cursor.execute(f"INSERT INTO graph.{vertex} (name) VALUES (%s)", (tag,))  # noqa: S608
            cursor.execute(f"INSERT INTO graph.{edge} (release_id, {column}) VALUES (%s, %s)", (release_id, tag))  # noqa: S608
        # A tag two releases carry takes the earliest positive year; a tag with no release at all
        # and a tag whose only release is undated both stay NULL.
        cursor.execute(f"INSERT INTO graph.{vertex} (name) VALUES ('tag-shared'), ('tag-empty')")  # noqa: S608
        for release_id in ("fy-1", "fy-9", "fy-2"):
            cursor.execute(f"INSERT INTO graph.{edge} (release_id, {column}) VALUES (%s, 'tag-shared')", (release_id,))  # noqa: S608

        columns = "name, first_year"
        cursor.execute(f"SELECT {columns} FROM ({COUNTER_BODIES[relation]}) AS new ORDER BY name")  # noqa: S608
        new = cursor.fetchall()
        cursor.execute(f"SELECT {columns} FROM ({OLD_BODIES[relation]}) AS old ORDER BY name")  # noqa: S608
        old = cursor.fetchall()

    assert new == old
    by_name = dict(new)
    assert by_name["tag-fy-1"] == 1970
    assert by_name["tag-fy-5"] == 1985
    assert by_name["tag-fy-9"] == 987
    assert by_name["tag-fy-10"] == 42
    assert by_name["tag-shared"] == 987
    for undated in ("fy-2", "fy-3", "fy-4", "fy-6", "fy-7", "fy-8", "fy-11", "fy-12"):
        assert by_name[f"tag-{undated}"] is None
    assert by_name["tag-empty"] is None
