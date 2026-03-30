"""internal_entities app_id coercion."""

from st_cli.st_api import _coerce_internal_entity_app_ids


def test_coerce_string_digits_to_int():
    assert _coerce_internal_entity_app_ids(["570060128"]) == [570060128]


def test_coerce_preserves_int_and_mixed():
    assert _coerce_internal_entity_app_ids([123, "456"]) == [123, 456]
