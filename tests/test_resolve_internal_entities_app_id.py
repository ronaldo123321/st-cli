"""resolve_internal_entities_app_id picks numeric unified id, not Mongo ObjectId."""

from st_cli.st_api import resolve_internal_entities_app_id


def test_prefers_unified_app_id_over_objectid_app_id():
    chosen = {
        "app_id": "55c50c6302ac64f9c000232d",
        "unified_app_id": 12345,
        "name": "X",
    }
    assert resolve_internal_entities_app_id(chosen) == 12345


def test_nested_entity():
    chosen = {
        "app_id": "55c50c6302ac64f9c000232d",
        "entity": {"unified_app_id": 999},
    }
    assert resolve_internal_entities_app_id(chosen) == 999


def test_objectid_only_returns_none():
    chosen = {"app_id": "55c50c6302ac64f9c000232d", "name": "X"}
    assert resolve_internal_entities_app_id(chosen) is None


def test_ios_apps_first_entry():
    chosen = {
        "app_id": "abc",
        "ios_apps": [{"unified_app_id": 42, "app_id": "zzz"}],
    }
    assert resolve_internal_entities_app_id(chosen) == 42
