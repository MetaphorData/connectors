import json
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from metaphor.common import extractor, handler


@dataclass_json
@dataclass
class DummyRunConfig(extractor.RunConfig):
    foo: str


class DummyExtractor(extractor.BaseExtractor):
    """Dummy extractor"""

    def run(self, config: extractor.RunConfig):
        pass

    def extract(self, config: extractor.RunConfig):
        pass


def test_missing_config_file():
    event = {"no_config_file": "foo"}

    ret = handler.handle_api(event, {}, DummyRunConfig, DummyExtractor)
    assert ret["statusCode"] == 422
    assert ret["body"] == "Missing 'config_file' parameter"


def test_invalid_config_file(test_root_dir):
    config_file = f"{test_root_dir}/common/invalid_config.json"
    event = {"config_file": config_file}

    ret = handler.handle_api(event, {}, DummyRunConfig, DummyExtractor)
    assert ret["statusCode"] == 422
    assert ret["body"] == f"Missing 'foo' key in {config_file}"


def test_read_from_params(test_root_dir):
    config_file = f"{test_root_dir}/common/valid_config.json"
    event = {"config_file": config_file}

    ret = handler.handle_api(event, {}, DummyRunConfig, DummyExtractor)
    assert ret["statusCode"] == 200
    assert json.loads(ret["body"])["config_file"] == config_file


def test_override_kinesis_config(test_root_dir):
    config_file = f"{test_root_dir}/common/kinesis_config.json"
    config = DummyRunConfig.from_json_file(config_file)

    assert config.kinesis.region_name == "foo"
    assert config.kinesis.assume_role_arn == "baz"


def test_api_config(test_root_dir):
    config_file = f"{test_root_dir}/common/api_config.json"
    config = DummyRunConfig.from_json_file(config_file)

    assert config.api.url == "foo_url"
    assert config.api.api_key == "key"
    assert config.api.timeout == 5
