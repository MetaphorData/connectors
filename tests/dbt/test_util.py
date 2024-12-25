from datetime import datetime

from metaphor.common.entity_id import to_person_entity_id
from metaphor.dbt.config import MetaOwnership, MetaTag
from metaphor.dbt.util import (
    get_data_platform_from_manifest,
    get_dbt_tags_from_meta,
    get_metaphor_tags_from_meta,
    get_ownerships_from_meta,
    parse_date_time_from_result,
)
from metaphor.models.metadata_change_event import DataPlatform, Ownership


def test_get_ownerships_from_meta(test_root_dir):
    meta = {
        "owners": ["foo", "bar"],
        "owner": "baz@metaphor.io",
        "invalid_email": "not_an_email",
    }

    meta_ownerships = [
        MetaOwnership(
            meta_key="owners",
            email_domain="metaphor.io",
            ownership_type="Data Owner",
        ),
        MetaOwnership(
            meta_key="owner",
            ownership_type="Tech Owner",
        ),
        MetaOwnership(
            meta_key="invalid_email",
            ownership_type="Tech Owner",
        ),
        MetaOwnership(
            meta_key="non-existing-key",
            ownership_type="Data Boss",
        ),
    ]

    expected_ownerships = [
        Ownership(
            contact_designation_name="Data Owner",
            person=str(to_person_entity_id("foo@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="Data Owner",
            person=str(to_person_entity_id("bar@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="Tech Owner",
            person=str(to_person_entity_id("baz@metaphor.io")),
        ),
    ]

    assert (
        get_ownerships_from_meta(meta, meta_ownerships).materialized_table
        == expected_ownerships
    )
    assert (
        get_ownerships_from_meta(meta, meta_ownerships).dbt_model == expected_ownerships
    )


def test_get_ownerships_with_assignment_targets(test_root_dir):
    meta = {
        "owners_dbt_model": ["foo", "bar"],
        "owners_materialized_table": ["bar", "qux"],
        "owners_both": ["baz"],
    }
    meta_ownerships = [
        MetaOwnership(
            meta_key="owners_dbt_model",
            ownership_type="dbt model owner",
            email_domain="metaphor.io",
            assignment_target="dbt_model",
        ),
        MetaOwnership(
            meta_key="owners_both",
            ownership_type="owner of both dbt model and materialized table",
            email_domain="metaphor.io",
            assignment_target="both",
        ),
        MetaOwnership(
            meta_key="owners_materialized_table",
            ownership_type="materialized table owner",
            email_domain="metaphor.io",
            assignment_target="materialized_table",
        ),
    ]
    ownerships = get_ownerships_from_meta(meta, meta_ownerships)
    expected_dbt_model_ownerships = [
        Ownership(
            contact_designation_name="dbt model owner",
            person=str(to_person_entity_id("foo@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="dbt model owner",
            person=str(to_person_entity_id("bar@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="owner of both dbt model and materialized table",
            person=str(to_person_entity_id("baz@metaphor.io")),
        ),
    ]
    expected_materialized_table_ownerships = [
        Ownership(
            contact_designation_name="owner of both dbt model and materialized table",
            person=str(to_person_entity_id("baz@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="materialized table owner",
            person=str(to_person_entity_id("bar@metaphor.io")),
        ),
        Ownership(
            contact_designation_name="materialized table owner",
            person=str(to_person_entity_id("qux@metaphor.io")),
        ),
    ]
    assert ownerships.dbt_model == expected_dbt_model_ownerships
    assert ownerships.materialized_table == expected_materialized_table_ownerships


def test_get_metaphor_tags_from_meta(test_root_dir):
    meta = {
        "pii": True,
        "prod": False,
        "team": "sales",
    }

    meta_tags = [
        MetaTag(
            meta_key="pii",
            tag_type="pii",
        ),
        MetaTag(
            meta_key="prod",
            tag_type="prod",
        ),
        MetaTag(
            meta_key="team",
            meta_value_matcher="sales",
            tag_type="sales",
        ),
        MetaTag(
            meta_key="team",
            meta_value_matcher="eng",
            tag_type="eng",
        ),
    ]

    expected_tags = ["pii", "sales"]

    assert get_metaphor_tags_from_meta(meta, meta_tags) == expected_tags


def test_get_dbt_tags_from_meta(test_root_dir):

    assert get_dbt_tags_from_meta(None, None) == []

    assert get_dbt_tags_from_meta({"other_key": "val"}, "dbt_tag") == []

    assert get_dbt_tags_from_meta({"dbt_tag": "foo"}, "dbt_tag") == ["foo"]

    assert get_dbt_tags_from_meta({"dbt_tags": ["foo", "bar"]}, "dbt_tags") == [
        "foo",
        "bar",
    ]


def test_get_data_platform_from_manifest(test_root_dir: str) -> None:
    manifest_path = f"{test_root_dir}/dbt/data/databricks/manifest.json"
    platform = get_data_platform_from_manifest(manifest_path)
    assert platform is DataPlatform.UNITY_CATALOG


def test_parse_date_time_from_result() -> None:
    assert parse_date_time_from_result(None) is None
    assert parse_date_time_from_result("") is None
    assert parse_date_time_from_result("not a date") is None

    # Test ISO format
    assert parse_date_time_from_result("2023-01-01T12:34:56") == datetime(
        2023, 1, 1, 12, 34, 56
    )

    # Test date only
    assert parse_date_time_from_result("2023-01-01") == datetime(2023, 1, 1)
