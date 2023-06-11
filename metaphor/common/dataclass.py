from pydantic import Extra


class DataclassConfig:
    """
    Config for pydantic dataclass and BaseModel
    """

    validate_all = True
    extra = Extra.forbid
