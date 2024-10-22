# db_operations/AbstractDatabaseOperations.py

from typing import Protocol


class AbstractCopy(Protocol):
    def execute_copy_command(self, *args, **kwargs) -> None:
        """Execute the copy command for database operations."""
        raise NotImplementedError("Subclasses must implement this method.")
