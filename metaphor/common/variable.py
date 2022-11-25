import os
import re
from typing import Any, Dict, List, TypeVar

# Allowed variable pattern: ${ENV_VAR_NAME}
var_pattern = r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}"


T = TypeVar("T")


def variable_substitution(target: T) -> T:
    """Recursively substitute strings with variable expression in the dictionary

    A variable expression is in the form of ${VAR_NAME}. Its value will
    be substitute with the value of a matching environment variable name.
    If no matching environment variable is found, the expression will be
    left as-is.
    """

    def sub_str(target_str: str) -> str:
        def replacement(match: re.Match) -> str:
            if match is not None:
                env_val = os.environ.get(match.group(1))
                if env_val is not None:
                    return env_val
            return target_str

        return re.sub(var_pattern, replacement, target_str)

    def sub_list(target_list: List) -> List:
        return [sub_val(val) for val in target_list]

    def sub_dict(target_dict: Dict) -> Dict:
        return {key: sub_val(val) for key, val in target_dict.items()}

    def sub_val(target_val: Any) -> Any:
        if isinstance(target_val, dict):
            return sub_dict(target_val)
        elif isinstance(target_val, list):
            return sub_list(target_val)
        elif isinstance(target_val, str):
            return sub_str(target_val)

        return target_val

    return sub_val(target)
