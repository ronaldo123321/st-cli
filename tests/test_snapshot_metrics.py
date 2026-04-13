"""Tests for snapshot metric extraction helpers."""

from st_cli.st_api import extract_wau_absolute_from_facets_v2_rows


def test_extract_wau_absolute_from_facets_v2_rows():
    rows = [
        {
            "appId": None,
            "activeUsersWAUAbsolute": 4321,
        }
    ]

    assert extract_wau_absolute_from_facets_v2_rows(rows) == 4321.0
