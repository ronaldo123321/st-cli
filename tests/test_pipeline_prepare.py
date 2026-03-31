"""Tests for search term preparation (no browser)."""

from st_cli.pipeline import prepare_search_term


def test_prepare_plain_name():
    term, w = prepare_search_term("  Duolingo  ")
    assert term == "Duolingo"
    assert w == []


def test_prepare_ios_store_url_uses_slug_for_autocomplete():
    term, w = prepare_search_term("https://apps.apple.com/us/app/duolingo/id570060128")
    assert term == "duolingo"
    assert "using_ios_slug_from_url" in w


def test_prepare_play_store_url():
    term, w = prepare_search_term(
        "https://play.google.com/store/apps/details?id=com.duolingo"
    )
    assert term == "com.duolingo"
    assert "using_android_package_from_url" in w
