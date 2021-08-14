from metaphor.slack_directory.extractor import SlackRunConfig


def test_json_config(test_root_dir):
    config = SlackRunConfig.from_json_file(
        f"{test_root_dir}/slack_directory/config.json"
    )

    assert config == SlackRunConfig(
        oauth_token="oauth_token",
        page_size=1,
        include_deleted=True,
        include_restricted=True,
        excluded_ids=frozenset(["id1", "id2"]),
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = SlackRunConfig.from_yaml_file(
        f"{test_root_dir}/slack_directory/config.yml"
    )

    assert config == SlackRunConfig(
        oauth_token="oauth_token",
        page_size=1,
        include_deleted=True,
        include_restricted=True,
        excluded_ids=frozenset(["id1", "id2"]),
        output=None,
    )
