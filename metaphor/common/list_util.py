from typing import List


def unique_list(items: List) -> List:
    # dict.fromkeys preserves order in Python 3.7+
    return list(dict.fromkeys(items))
