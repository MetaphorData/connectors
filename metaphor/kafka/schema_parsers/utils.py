def get_field_path(cur_path: str, field_name: str) -> str:
    return f"{cur_path}.{field_name}".lower() if cur_path else field_name.lower()
