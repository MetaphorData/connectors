def get_field_path(cur_path: str, field_name: str) -> str:
    return ".".join([cur_path, field_name]) if cur_path else field_name
