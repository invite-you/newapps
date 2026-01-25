import os


class DatabaseUnavailableError(RuntimeError):
    """Raised when the database remains unavailable after retries."""


DB_UNAVAILABLE_EXIT_CODE = int(os.getenv("DB_UNAVAILABLE_EXIT_CODE", "75"))
