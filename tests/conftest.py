import os

import pytest


@pytest.fixture()
def test_root_dir():
    return os.path.join(os.path.dirname(__file__))
