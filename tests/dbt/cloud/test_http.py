import json

import httpx
from testcontainers.general import DockerContainer

from metaphor.common.logger import debug_files
from metaphor.dbt.cloud.http import LogTransport


def test_http_client():
    http_client = httpx.Client(
        transport=LogTransport(httpx.HTTPTransport()), timeout=30
    )

    with DockerContainer("hashicorp/http-echo").with_exposed_ports(5678).with_env(
        "ECHO_TEXT", json.dumps({})
    ) as container:
        port = container.get_exposed_port(5678)
        host = container.get_container_host_ip()

        debug_files.clear()
        url = f"http://{host}:{port}"
        http_client.post(url, content=json.dumps({"foo": "bar"}))

        # Should log two json file
        assert len(debug_files) == 2
