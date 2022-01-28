import pytest
from pydantic import ValidationError

from metaphor.common.sampling import SamplingConfig


def test_sampling_config():
    assert SamplingConfig(percentage=50)

    with pytest.raises(ValidationError):
        SamplingConfig(percentage=-1)

    with pytest.raises(ValidationError):
        SamplingConfig(percentage=101)
