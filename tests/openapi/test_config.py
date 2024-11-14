from metaphor.common.base_config import OutputConfig
from metaphor.openapi.config import OpenAPIJsonConfig, OpenAPIRunConfig


def test_yaml_config(test_root_dir):
    config = OpenAPIRunConfig.from_yaml_file(f"{test_root_dir}/openapi/config.yml")

    assert config == OpenAPIRunConfig(
        base_url="https://petstore3.swagger.io",
        openapi_json_url="https://petstore3.swagger.io/api/v3/openapi.json",
        specs=[
            OpenAPIJsonConfig(
                base_url="https://base2",
                openapi_json_url="https://petstore3.swagger.io/api/v3/openapi.json",
            ),
            OpenAPIJsonConfig(
                base_url="https://base3",
                openapi_json_url="https://petstore3.swagger.io/api/v3/openapi.json",
            ),
        ],
        output=OutputConfig(),
    )
