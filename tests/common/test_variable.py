import os

from metaphor.common.variable import variable_substitution


def test_variable_substitution():
    os.environ["FOO"] = "FOO"
    os.environ["BAR"] = "BAR"
    os.environ["BAZ"] = "BAZ"

    target = {
        "str": "something ${FOO} something",
        "int": 1,
        "dict": {
            "key1": "${BAZ}",
            "key2": "${NON_EXISTING}",
            "list": ["${BAR}", "something"],
        },
    }

    assert variable_substitution(target) == {
        "str": "something FOO something",
        "int": 1,
        "dict": {
            "key1": "BAZ",
            "key2": "${NON_EXISTING}",
            "list": ["BAR", "something"],
        },
    }
