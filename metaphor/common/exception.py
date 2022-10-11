class AuthenticationError(Exception):
    def __init__(self, body) -> None:
        super().__init__(
            f"Authentication error: {body}.\n"
            f"Please\n"
            f"  1. Enable Power BI admin read-only API for the app\n"
            f"  2. Enable service principal to use Power BI APIs for the app\n"
        )


class EntityNotFoundError(Exception):
    def __init__(self, body) -> None:
        super().__init__(f"EntityNotFound error: {body}")
