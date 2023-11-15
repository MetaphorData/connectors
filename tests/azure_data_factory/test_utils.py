from metaphor.azure_data_factory.utils import safe_get_from_json


def test_safe_get_from_json():
    assert safe_get_from_json(None) is None
