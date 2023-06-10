import json


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def compare_list_ignore_order(a: list, b: list):
    t = list(b)  # make a mutable copy
    try:
        for elem in a:
            t.remove(elem)
    except ValueError:
        return False
    return not t
