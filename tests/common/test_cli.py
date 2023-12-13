from metaphor.common.cli import cli_main
from metaphor.models.crawler_run_metadata import RunStatus
from tests.common.test_extractor import DummyExtractor


def test_cli(test_root_dir):
    events, run_metadata = cli_main(
        DummyExtractor, f"{test_root_dir}/common/configs/config.yml"
    )
    assert events == []
    assert run_metadata.description == "dummy crawler"
    assert run_metadata.crawler_name == "tests.common.test_extractor.DummyExtractor"
    assert run_metadata.status == RunStatus.SUCCESS
    assert run_metadata.platform is None
